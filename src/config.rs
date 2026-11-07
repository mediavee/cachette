//! Configuration: defaults < TOML file < `CACHETTE_*` environment < command line.
//!
//! [`Cli`] is parsed by clap, which already folds the environment into the flags. [`Config::load`]
//! layers it over the file and the defaults, then validates. Every error names the key.

use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use bytesize::ByteSize;
use clap::{Args, Parser, ValueEnum};
use serde::Deserialize;
use serde::de::{self, Deserializer};
use thiserror::Error;

const DEFAULT_LISTEN: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 5071);
const DEFAULT_MAX_OBJECT_SIZE: u64 = 512 * 1024 * 1024;
const DEFAULT_MIN_FREE_DISK: u64 = 1024 * 1024 * 1024;
const DEFAULT_TOUCH_INTERVAL: Duration = Duration::from_secs(300);
const DEFAULT_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_BODY_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_HEADER_SIZE: u64 = 16 * 1024;
/// hyper refuses a header buffer smaller than this.
const MIN_HEADER_SIZE: u64 = 8 * 1024;

/// A configuration problem. Startup stops on it.
#[derive(Debug, Error)]
pub(crate) enum ConfigError {
    /// The configuration file cannot be read.
    #[error("cannot read config file {}: {source}", path.display())]
    Read {
        /// The file.
        path: PathBuf,
        /// The underlying error.
        source: std::io::Error,
    },
    /// The configuration file is not valid TOML, or has an unknown or mistyped key. The message
    /// carries no excerpt of the file, which may hold secrets.
    #[error("invalid config file {}: {message}", path.display())]
    Parse {
        /// The file.
        path: PathBuf,
        /// The parser's message, which names the key, and the line it was found on.
        message: String,
    },
    /// A mandatory key is not set anywhere.
    #[error("`{key}` is required: set it in the config file, as {env} or as {flag}")]
    Missing {
        /// The TOML key.
        key: &'static str,
        /// The environment variable.
        env: &'static str,
        /// The command-line flag.
        flag: &'static str,
    },
    /// A key has a value that cannot work.
    #[error("`{key}` {reason}")]
    Invalid {
        /// The TOML key.
        key: &'static str,
        /// What is wrong with it.
        reason: String,
    },
}

/// A secret kept only as its blake3 digest, so it cannot be printed or leaked by `Debug`, and is
/// compared in constant time whatever its length.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Secret(blake3::Hash);

impl Secret {
    /// Whether `candidate` is this secret. Constant time: [`blake3::Hash`] equality is.
    pub(crate) fn matches(&self, candidate: &str) -> bool {
        self.0 == blake3::hash(candidate.as_bytes())
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl FromStr for Secret {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.is_empty() {
            return Err("must not be empty".to_owned());
        }
        Ok(Self(blake3::hash(text.as_bytes())))
    }
}

/// What a token may do. Write includes read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Access {
    /// `GET`, `HEAD`, `PROPFIND`.
    Read,
    /// Everything `read` allows, plus `PUT` and `MKCOL`.
    Write,
}

impl FromStr for Access {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "read" => Ok(Self::Read),
            "write" => Ok(Self::Write),
            _ => Err("access must be `read` or `write`".to_owned()),
        }
    }
}

/// A named credential. Its name is what the access log shows.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Token {
    /// Name shown in logs; unique.
    pub(crate) name: String,
    /// What the token may do.
    pub(crate) access: Access,
    /// The secret, accepted as Basic password, base64url Basic or Bearer.
    pub(crate) secret: Secret,
}

impl FromStr for Token {
    type Err = String;

    /// Parses `NAME:ACCESS:SECRET`; the secret may contain `:`. An error never repeats any part of
    /// `text`, since it may be a secret.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let mut parts = text.splitn(3, ':');
        let (Some(name), Some(access), Some(secret)) = (parts.next(), parts.next(), parts.next())
        else {
            return Err("expected NAME:ACCESS:SECRET".to_owned());
        };
        Ok(Self {
            name: name.to_owned(),
            access: access.parse()?,
            secret: secret.parse()?,
        })
    }
}

/// Whether requests without credentials may read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Anonymous {
    /// Every request needs a token.
    #[default]
    None,
    /// Reads are open; writes still need a `write` token.
    Read,
}

/// Format of the logs on stderr.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub(crate) enum LogFormat {
    /// Human-readable lines.
    #[default]
    Text,
    /// One JSON object per line.
    Json,
}

/// How much the store spends to survive a crash or a power cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum Durability {
    /// No `fsync` per object. After any unclean shutdown the store purges itself at start-up
    /// instead of serving a torn file. Needs `syncfs`, so Linux only.
    PurgeOnCrash,
    /// `sync_all` each object before it becomes visible. A crash loses at worst recent entries.
    Fsync,
}

/// Whether the platform has `syncfs`, which `purge-on-crash` relies on to shut down cleanly.
const HAS_SYNCFS: bool = cfg!(target_os = "linux");

impl Default for Durability {
    /// `purge-on-crash` where it is possible, `fsync` elsewhere.
    fn default() -> Self {
        if HAS_SYNCFS {
            Self::PurgeOnCrash
        } else {
            Self::Fsync
        }
    }
}

/// Which requests the access log records.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub(crate) enum AccessLog {
    /// Errors, authentication refusals and dropped PUTs only.
    #[default]
    Errors,
    /// Every request.
    All,
    /// No access log; errors still reach the main log.
    Off,
}

/// Certificate chain and private key, both PEM files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Tls {
    /// Certificate chain.
    pub(crate) cert: PathBuf,
    /// Private key.
    pub(crate) key: PathBuf,
}

/// A size such as `30GiB`, or a plain number of bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Size(u64);

impl FromStr for Size {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.trim()
            .parse::<ByteSize>()
            .map(|size| Self(size.as_u64()))
    }
}

impl<'de> Deserialize<'de> for Size {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor<'_> for Visitor {
            type Value = Size;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a size such as \"30GiB\", or a number of bytes")
            }

            fn visit_u64<E: de::Error>(self, bytes: u64) -> Result<Size, E> {
                Ok(Size(bytes))
            }

            fn visit_i64<E: de::Error>(self, bytes: i64) -> Result<Size, E> {
                u64::try_from(bytes)
                    .map(Size)
                    .map_err(|_| E::custom("a size cannot be negative"))
            }

            fn visit_str<E: de::Error>(self, text: &str) -> Result<Size, E> {
                text.parse().map_err(E::custom)
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

/// A duration such as `10s` or `5m`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Dur(Duration);

impl FromStr for Dur {
    type Err = humantime::DurationError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        humantime::parse_duration(text).map(Self)
    }
}

/// Deserialises a string-parsed scalar.
fn from_string<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: fmt::Display,
{
    String::deserialize(deserializer)?
        .parse()
        .map_err(de::Error::custom)
}

impl<'de> Deserialize<'de> for Dur {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        from_string(deserializer)
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        from_string(deserializer)
    }
}

/// The scalar settings, as given by one source. `None` means "not set here".
///
/// Doubles as the TOML file's shape and as the command-line flags. Doc comments are the `--help`.
#[derive(Debug, Clone, Default, Args, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Settings {
    /// Address to listen on [default: 127.0.0.1:5071]. Port 0 picks a free port.
    #[arg(long, env = "CACHETTE_LISTEN", value_name = "ADDR")]
    listen: Option<SocketAddr>,
    /// Directory holding the cache [default: data].
    #[arg(long, env = "CACHETTE_DATA_DIR", value_name = "DIR")]
    data_dir: Option<PathBuf>,
    /// Ceiling for stored objects and in-flight uploads, such as `20GiB`. Required.
    #[arg(long, env = "CACHETTE_MAX_SIZE", value_name = "SIZE")]
    max_size: Option<Size>,
    /// Largest accepted object [default: 512MiB, or `max_size` if smaller].
    #[arg(long, env = "CACHETTE_MAX_OBJECT_SIZE", value_name = "SIZE")]
    max_object_size: Option<Size>,
    /// Free disk space to keep [default: 1GiB].
    #[arg(long, env = "CACHETTE_MIN_FREE_DISK", value_name = "SIZE")]
    min_free_disk: Option<Size>,
    /// Crash behaviour: purge after an unclean shutdown instead of syncing each object (Linux
    /// only), or sync each object [default: purge-on-crash on Linux, fsync elsewhere].
    #[arg(long, env = "CACHETTE_DURABILITY", value_enum, value_name = "MODE")]
    durability: Option<Durability>,
    /// Minimum delay between two persisted recency updates of an entry [default: 5m].
    #[arg(long, env = "CACHETTE_TOUCH_INTERVAL", value_name = "DURATION")]
    touch_interval: Option<Dur>,
    /// Time a client has to send its request headers [default: 10s].
    #[arg(long, env = "CACHETTE_HEADER_READ_TIMEOUT", value_name = "DURATION")]
    header_read_timeout: Option<Dur>,
    /// Time a request body may stay silent [default: 60s].
    #[arg(long, env = "CACHETTE_BODY_IDLE_TIMEOUT", value_name = "DURATION")]
    body_idle_timeout: Option<Dur>,
    /// Largest request header section, at least 8KiB [default: 16KiB].
    #[arg(long, env = "CACHETTE_MAX_HEADER_SIZE", value_name = "SIZE")]
    max_header_size: Option<Size>,
    /// Time requests in flight get to finish on SIGTERM [default: 30s].
    #[arg(long, env = "CACHETTE_SHUTDOWN_TIMEOUT", value_name = "DURATION")]
    shutdown_timeout: Option<Dur>,
    /// Whether anonymous requests may read [default: none].
    #[arg(long, env = "CACHETTE_ANONYMOUS", value_enum, value_name = "MODE")]
    anonymous: Option<Anonymous>,
    /// Bearer token protecting `/metrics`; open when unset.
    #[arg(
        long,
        env = "CACHETTE_METRICS_TOKEN",
        value_name = "SECRET",
        hide_env_values = true
    )]
    #[serde(deserialize_with = "from_string_option", default)]
    metrics_token: Option<Secret>,
    /// PEM certificate chain; with `tls_key`, serves HTTPS.
    #[arg(long, env = "CACHETTE_TLS_CERT", value_name = "FILE")]
    tls_cert: Option<PathBuf>,
    /// PEM private key; with `tls_cert`, serves HTTPS.
    #[arg(long, env = "CACHETTE_TLS_KEY", value_name = "FILE")]
    tls_key: Option<PathBuf>,
    /// Log format on stderr [default: text].
    #[arg(long, env = "CACHETTE_LOG_FORMAT", value_enum, value_name = "FORMAT")]
    log_format: Option<LogFormat>,
    /// Which requests the access log records [default: errors].
    #[arg(long, env = "CACHETTE_ACCESS_LOG", value_enum, value_name = "MODE")]
    access_log: Option<AccessLog>,
    /// Runtime worker threads [default: the available parallelism].
    #[arg(long, env = "CACHETTE_WORKER_THREADS", value_name = "N")]
    worker_threads: Option<usize>,
}

fn from_string_option<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Secret>, D::Error> {
    Option::<String>::deserialize(deserializer)?
        .map(|text| text.parse().map_err(de::Error::custom))
        .transpose()
}

impl Settings {
    /// `self` where set, `lower` elsewhere.
    fn over(self, lower: Self) -> Self {
        Self {
            listen: self.listen.or(lower.listen),
            data_dir: self.data_dir.or(lower.data_dir),
            max_size: self.max_size.or(lower.max_size),
            max_object_size: self.max_object_size.or(lower.max_object_size),
            min_free_disk: self.min_free_disk.or(lower.min_free_disk),
            durability: self.durability.or(lower.durability),
            touch_interval: self.touch_interval.or(lower.touch_interval),
            header_read_timeout: self.header_read_timeout.or(lower.header_read_timeout),
            body_idle_timeout: self.body_idle_timeout.or(lower.body_idle_timeout),
            max_header_size: self.max_header_size.or(lower.max_header_size),
            shutdown_timeout: self.shutdown_timeout.or(lower.shutdown_timeout),
            anonymous: self.anonymous.or(lower.anonymous),
            metrics_token: self.metrics_token.or(lower.metrics_token),
            tls_cert: self.tls_cert.or(lower.tls_cert),
            tls_key: self.tls_key.or(lower.tls_key),
            log_format: self.log_format.or(lower.log_format),
            access_log: self.access_log.or(lower.access_log),
            worker_threads: self.worker_threads.or(lower.worker_threads),
        }
    }
}

/// The command line. Every flag also reads its `CACHETTE_*` variable.
#[derive(Debug, Parser)]
#[command(version, about = "Remote build cache server for Gradle and sccache")]
pub(crate) struct Cli {
    /// TOML configuration file. Flags and variables override it.
    #[arg(long, short, env = "CACHETTE_CONFIG", value_name = "FILE")]
    config: Option<PathBuf>,
    /// Token as NAME:ACCESS:SECRET, `ACCESS` being `read` or `write`. Comma-separated in
    /// `CACHETTE_TOKENS`. Replaces the file's `[[tokens]]` when given; prefer the variable, since
    /// flags show in the process list. Kept as text until validation so that no error repeats it.
    #[arg(
        long = "token",
        env = "CACHETTE_TOKENS",
        value_name = "NAME:ACCESS:SECRET",
        value_delimiter = ',',
        hide_env_values = true
    )]
    tokens: Vec<String>,
    #[command(flatten)]
    settings: Settings,
}

/// The validated configuration.
#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) listen: SocketAddr,
    pub(crate) data_dir: PathBuf,
    pub(crate) max_size: u64,
    pub(crate) max_object_size: u64,
    pub(crate) min_free_disk: u64,
    pub(crate) durability: Durability,
    pub(crate) touch_interval: Duration,
    pub(crate) header_read_timeout: Duration,
    pub(crate) body_idle_timeout: Duration,
    pub(crate) max_header_size: usize,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) tokens: Vec<Token>,
    pub(crate) anonymous: Anonymous,
    pub(crate) metrics_token: Option<Secret>,
    pub(crate) tls: Option<Tls>,
    pub(crate) log_format: LogFormat,
    pub(crate) access_log: AccessLog,
    pub(crate) worker_threads: usize,
}

impl Config {
    /// Layers the file named by `cli`, if any, under its flags and variables, and validates.
    ///
    /// # Errors
    ///
    /// A [`ConfigError`] naming the offending key.
    pub(crate) fn load(cli: Cli) -> Result<Self, ConfigError> {
        let (file, file_tokens) = match &cli.config {
            Some(path) => read_file(path)?,
            None => (Settings::default(), Vec::new()),
        };
        let tokens = if cli.tokens.is_empty() {
            file_tokens
        } else {
            parse_tokens(&cli.tokens)?
        };
        Self::build(cli.settings.over(file), tokens)
    }

    fn build(settings: Settings, tokens: Vec<Token>) -> Result<Self, ConfigError> {
        let max_size = settings.max_size.ok_or(ConfigError::Missing {
            key: "max_size",
            env: "CACHETTE_MAX_SIZE",
            flag: "--max-size",
        })?;
        positive("max_size", max_size.0)?;
        let max_object_size = settings
            .max_object_size
            .map_or(DEFAULT_MAX_OBJECT_SIZE.min(max_size.0), |size| size.0);
        positive("max_object_size", max_object_size)?;
        if max_object_size > max_size.0 {
            return Err(invalid(
                "max_object_size",
                format!(
                    "({max_object_size} bytes) must not exceed `max_size` ({} bytes)",
                    max_size.0
                ),
            ));
        }
        let max_header_size = settings
            .max_header_size
            .map_or(DEFAULT_MAX_HEADER_SIZE, |size| size.0);
        if max_header_size < MIN_HEADER_SIZE {
            return Err(invalid(
                "max_header_size",
                format!("must be at least {MIN_HEADER_SIZE} bytes"),
            ));
        }
        let max_header_size = usize::try_from(max_header_size)
            .map_err(|_| invalid("max_header_size", "does not fit in memory".to_owned()))?;
        let header_read_timeout = settings
            .header_read_timeout
            .map_or(DEFAULT_HEADER_READ_TIMEOUT, |d| d.0);
        let body_idle_timeout = settings
            .body_idle_timeout
            .map_or(DEFAULT_BODY_IDLE_TIMEOUT, |d| d.0);
        for (key, value) in [
            ("header_read_timeout", header_read_timeout),
            ("body_idle_timeout", body_idle_timeout),
        ] {
            if value.is_zero() {
                return Err(invalid(key, "must be greater than zero".to_owned()));
            }
        }
        let worker_threads = worker_threads(settings.worker_threads)?;
        let durability = durability(settings.durability)?;
        let anonymous = settings.anonymous.unwrap_or_default();
        check_tokens(&tokens, anonymous)?;
        let tls = tls(settings.tls_cert, settings.tls_key)?;
        Ok(Self {
            listen: settings.listen.unwrap_or(DEFAULT_LISTEN),
            data_dir: settings.data_dir.unwrap_or_else(|| PathBuf::from("data")),
            max_size: max_size.0,
            max_object_size,
            min_free_disk: settings
                .min_free_disk
                .map_or(DEFAULT_MIN_FREE_DISK, |size| size.0),
            durability,
            touch_interval: settings
                .touch_interval
                .map_or(DEFAULT_TOUCH_INTERVAL, |d| d.0),
            header_read_timeout,
            body_idle_timeout,
            max_header_size,
            shutdown_timeout: settings
                .shutdown_timeout
                .map_or(DEFAULT_SHUTDOWN_TIMEOUT, |d| d.0),
            tokens,
            anonymous,
            metrics_token: settings.metrics_token,
            tls,
            log_format: settings.log_format.unwrap_or_default(),
            access_log: settings.access_log.unwrap_or_default(),
            worker_threads,
        })
    }
}

fn worker_threads(requested: Option<usize>) -> Result<usize, ConfigError> {
    match requested {
        Some(0) => Err(invalid("worker_threads", "must be at least 1".to_owned())),
        Some(threads) => Ok(threads),
        None => Ok(std::thread::available_parallelism().map_or(1, usize::from)),
    }
}

fn durability(requested: Option<Durability>) -> Result<Durability, ConfigError> {
    let durability = requested.unwrap_or_default();
    if durability == Durability::PurgeOnCrash && !HAS_SYNCFS {
        return Err(invalid(
            "durability",
            "= \"purge-on-crash\" needs `syncfs`, which this platform lacks: use \"fsync\""
                .to_owned(),
        ));
    }
    Ok(durability)
}

fn tls(cert: Option<PathBuf>, key: Option<PathBuf>) -> Result<Option<Tls>, ConfigError> {
    match (cert, key) {
        (Some(cert), Some(key)) => Ok(Some(Tls { cert, key })),
        (None, None) => Ok(None),
        (Some(_), None) => Err(invalid(
            "tls_key",
            "is required when `tls_cert` is set".to_owned(),
        )),
        (None, Some(_)) => Err(invalid(
            "tls_cert",
            "is required when `tls_key` is set".to_owned(),
        )),
    }
}

/// Parses `NAME:ACCESS:SECRET` entries. A failure names the entry's position only: the text may
/// hold a secret.
fn parse_tokens(entries: &[String]) -> Result<Vec<Token>, ConfigError> {
    entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            entry.parse().map_err(|reason| {
                invalid(
                    "tokens",
                    format!("entry {} is malformed: {reason}", index + 1),
                )
            })
        })
        .collect()
}

fn invalid(key: &'static str, reason: String) -> ConfigError {
    ConfigError::Invalid { key, reason }
}

fn positive(key: &'static str, bytes: u64) -> Result<(), ConfigError> {
    if bytes == 0 {
        return Err(invalid(key, "must be greater than zero".to_owned()));
    }
    Ok(())
}

fn check_tokens(tokens: &[Token], anonymous: Anonymous) -> Result<(), ConfigError> {
    for (index, token) in tokens.iter().enumerate() {
        if token.name.is_empty() {
            return Err(invalid("tokens", "need a non-empty `name`".to_owned()));
        }
        if tokens[..index]
            .iter()
            .any(|earlier| earlier.name == token.name)
        {
            return Err(invalid(
                "tokens",
                format!("have a duplicate name `{}`", token.name),
            ));
        }
    }
    if tokens.is_empty() && anonymous != Anonymous::Read {
        return Err(invalid(
            "tokens",
            "are empty and `anonymous` is `none`: nobody could use the cache; add a token or set `anonymous = \"read\"`"
                .to_owned(),
        ));
    }
    Ok(())
}

/// Describes a TOML error without any excerpt of `text`, which may hold secrets: the message, the
/// line and the key (the text before `=`) when the parser knows where, else the key it names.
fn parse_message(text: &str, source: &toml::de::Error) -> String {
    let Some(span) = source.span() else {
        let display = source.to_string();
        // A mistyped secret (a number, say) is echoed by serde: name the key only.
        return match ["secret", "metrics_token"]
            .into_iter()
            .find(|key| display.contains(&format!("in `{key}`")))
        {
            Some(key) => format!("`{key}` must be a non-empty string"),
            None => display,
        };
    };
    let before = &text[..span.start.min(text.len())];
    let number = before.matches('\n').count() + 1;
    let line = text[before.rfind('\n').map_or(0, |at| at + 1)..]
        .lines()
        .next()
        .unwrap_or_default();
    let key = line.split_once('=').map_or("", |(key, _)| key.trim());
    format!("{} (`{key}`, line {number})", source.message())
}

fn read_file(path: &Path) -> Result<(Settings, Vec<Token>), ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let parse = |source: toml::de::Error| ConfigError::Parse {
        path: path.to_path_buf(),
        message: parse_message(&text, &source),
    };
    let mut table: toml::Table = text.parse().map_err(parse)?;
    let tokens = match table.remove("tokens") {
        Some(value) => value.try_into().map_err(parse)?,
        None => Vec::new(),
    };
    let settings = table.try_into().map_err(parse)?;
    Ok((settings, tokens))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli(args: &[&str]) -> Cli {
        Cli::try_parse_from(std::iter::once("cachette").chain(args.iter().copied()))
            .expect("valid flags")
    }

    fn config(file: Option<&str>, args: &[&str]) -> Result<Config, ConfigError> {
        let dir = tempfile::tempdir().unwrap();
        let mut full: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
        if let Some(text) = file {
            let path = dir.path().join("cachette.toml");
            std::fs::write(&path, text).unwrap();
            full.extend(["--config".to_owned(), path.display().to_string()]);
        }
        let refs: Vec<&str> = full.iter().map(String::as_str).collect();
        Config::load(cli(&refs))
    }

    const MINIMAL: &str = "max_size = \"10GiB\"\nanonymous = \"read\"\n";

    #[test]
    fn defaults_apply() {
        let c = config(Some(MINIMAL), &[]).unwrap();
        assert_eq!(c.listen, "127.0.0.1:5071".parse().unwrap());
        assert_eq!(c.max_size, 10 * 1024 * 1024 * 1024);
        assert_eq!(c.max_object_size, 512 * 1024 * 1024);
        assert_eq!(c.min_free_disk, 1024 * 1024 * 1024);
        assert_eq!(c.header_read_timeout, Duration::from_secs(10));
        assert_eq!(c.body_idle_timeout, Duration::from_secs(60));
        assert_eq!(c.max_header_size, 16 * 1024);
        assert_eq!(c.touch_interval, Duration::from_secs(300));
        assert_eq!(
            c.durability,
            if cfg!(target_os = "linux") {
                Durability::PurgeOnCrash
            } else {
                Durability::Fsync
            }
        );
        assert_eq!(c.access_log, AccessLog::Errors);
        assert_eq!(
            c.worker_threads,
            std::thread::available_parallelism().unwrap().get()
        );
        assert_eq!(c.anonymous, Anonymous::Read);
    }

    #[test]
    fn small_max_size_lowers_the_default_object_limit() {
        let c = config(Some("max_size = \"100MiB\"\nanonymous = \"read\""), &[]).unwrap();
        assert_eq!(c.max_object_size, 100 * 1024 * 1024);
    }

    #[test]
    fn precedence_is_file_then_env_then_cli() {
        let file = "max_size = \"1GiB\"\nanonymous = \"read\"\nlisten = \"127.0.0.1:1\"\ndurability = \"fsync\"\n";
        let c = config(Some(file), &["--listen", "127.0.0.1:2"]).unwrap();
        assert_eq!(c.listen.port(), 2, "flag must beat file");
        assert_eq!(c.durability, Durability::Fsync, "file must beat default");
        assert_eq!(c.max_size, 1024 * 1024 * 1024);
    }

    #[test]
    fn cli_tokens_replace_the_files() {
        let file =
            "max_size = \"1GiB\"\n[[tokens]]\nname = \"file\"\naccess = \"read\"\nsecret = \"s\"\n";
        let c = config(Some(file), &[]).unwrap();
        assert_eq!(c.tokens.len(), 1);
        let c = config(
            Some(file),
            &["--token", "ci:write:a:b", "--token", "dev:read:c"],
        )
        .unwrap();
        let names: Vec<_> = c.tokens.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["ci", "dev"]);
        assert!(c.tokens[0].secret.matches("a:b"), "secret keeps its colons");
        assert!(!c.tokens[0].secret.matches("a"));
    }

    fn message(result: Result<Config, ConfigError>) -> String {
        result
            .expect_err("configuration must be refused")
            .to_string()
    }

    #[test]
    fn max_size_is_mandatory() {
        let text = message(config(Some("anonymous = \"read\""), &[]));
        assert!(text.contains("`max_size` is required"), "{text}");
    }

    #[test]
    fn object_limit_cannot_exceed_the_cache() {
        let text = message(config(
            Some("max_size = \"1MiB\"\nmax_object_size = \"2MiB\"\nanonymous = \"read\""),
            &[],
        ));
        assert!(
            text.contains("`max_object_size`") && text.contains("max_size"),
            "{text}"
        );
    }

    #[test]
    fn needs_a_token_or_anonymous_read() {
        let text = message(config(Some("max_size = \"1GiB\""), &[]));
        assert!(
            text.contains("`tokens`") && text.contains("anonymous"),
            "{text}"
        );
        assert!(config(Some("max_size = \"1GiB\""), &["--token", "ci:write:x"]).is_ok());
    }

    #[test]
    fn rejects_unknown_keys_and_bad_values() {
        let text = message(config(Some("max_size = \"1GiB\"\nmax_sise = 3"), &[]));
        assert!(text.contains("max_sise"), "{text}");
        let text = message(config(
            Some("max_size = \"lots\"\nanonymous = \"read\""),
            &[],
        ));
        assert!(text.contains("max_size"), "{text}");
        let text = message(config(
            Some("max_size = \"1GiB\"\nbody_idle_timeout = \"0s\"\nanonymous = \"read\""),
            &[],
        ));
        assert!(text.contains("`body_idle_timeout`"), "{text}");
        let text = message(config(
            Some("max_size = \"1GiB\"\nanonymous = \"read\"\ntls_cert = \"c.pem\""),
            &[],
        ));
        assert!(text.contains("`tls_key`"), "{text}");
        let text = message(config(
            Some("max_size = \"1GiB\"\nmax_header_size = \"1KiB\"\nanonymous = \"read\""),
            &[],
        ));
        assert!(text.contains("`max_header_size`"), "{text}");
    }

    #[test]
    fn malformed_tokens_never_echo_their_text() {
        for bad in ["ci:invalid:hunter2", "ci:hunter2", "ci:read:", "hunter2"] {
            let text = message(config(None, &["--max-size", "1GiB", "--token", bad]));
            assert!(text.contains("`tokens`"), "{text}");
            assert!(!text.contains("hunter2"), "secret echoed: {text}");
        }
    }

    #[test]
    fn config_file_errors_carry_no_excerpt() {
        let file = "max_size = \"1GiB\"\n[[tokens]]\nname = \"a\"\naccess = \"read\"\nsecret = 1234hunter\n";
        let text = message(config(Some(file), &[]));
        assert!(!text.contains("hunter"), "file excerpt echoed: {text}");
        assert!(text.contains("line 5"), "{text}");
        let numeric =
            "max_size = \"1GiB\"\n[[tokens]]\nname = \"a\"\naccess = \"read\"\nsecret = 987654\n";
        let text = message(config(Some(numeric), &[]));
        assert!(
            text.contains("`secret`") && !text.contains("987654"),
            "{text}"
        );
    }

    #[test]
    fn purge_on_crash_is_refused_where_syncfs_is_missing() {
        let result = config(
            Some("max_size = \"1GiB\"\nanonymous = \"read\"\ndurability = \"purge-on-crash\""),
            &[],
        );
        if cfg!(target_os = "linux") {
            assert_eq!(result.unwrap().durability, Durability::PurgeOnCrash);
        } else {
            let text = message(result);
            assert!(
                text.contains("`durability`") && text.contains("syncfs"),
                "{text}"
            );
        }
        let explicit = config(
            Some("max_size = \"1GiB\"\nanonymous = \"read\"\ndurability = \"fsync\""),
            &[],
        );
        assert_eq!(explicit.unwrap().durability, Durability::Fsync);
    }

    #[test]
    fn runtime_and_log_options_are_validated() {
        let text = message(config(
            Some("max_size = \"1GiB\"\nanonymous = \"read\"\nworker_threads = 0"),
            &[],
        ));
        assert!(text.contains("`worker_threads`"), "{text}");
        let c = config(
            Some("max_size = \"1GiB\"\nanonymous = \"read\"\naccess_log = \"all\"\nworker_threads = 3"),
            &[],
        )
        .unwrap();
        assert_eq!((c.access_log, c.worker_threads), (AccessLog::All, 3));
    }

    #[test]
    fn duplicate_token_names_are_refused() {
        let text = message(config(
            None,
            &[
                "--max-size",
                "1GiB",
                "--token",
                "a:read:x",
                "--token",
                "a:write:y",
            ],
        ));
        assert!(text.contains("duplicate"), "{text}");
    }

    #[test]
    fn secrets_never_print() {
        let c = config(None, &["--max-size", "1GiB", "--token", "ci:write:hunter2"]).unwrap();
        assert!(!format!("{c:?}").contains("hunter2"));
    }
}
