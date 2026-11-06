//! Runs the real `cachette` binary for black-box tests.
//!
//! Every process gets its own data directory, its own `HOME`, and port 0, so tests run in
//! parallel and never touch a fixed port or the developer's files.

use std::fmt::Write as _;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

const BINARY: &str = env!("CARGO_BIN_EXE_cachette");
const STARTUP_TIMEOUT: Duration = Duration::from_secs(15);

/// A token that can write, valid in every default-configured daemon.
pub(crate) const TOKEN: &str = "test:write:secret";

/// A parsed HTTP/1.1 response.
#[derive(Debug)]
pub(crate) struct Response {
    pub(crate) status: u16,
    /// Header section, lower-cased, one `name: value` per line.
    pub(crate) headers: String,
    pub(crate) body: String,
}

/// A running `cachette` process.
#[derive(Debug)]
pub(crate) struct Daemon {
    child: Child,
    pub(crate) addr: SocketAddr,
    pub(crate) scheme: String,
    pub(crate) data_dir: PathBuf,
    dirs: Dirs,
}

#[derive(Debug)]
struct Dirs {
    _data: Option<TempDir>,
    home: TempDir,
}

impl Dirs {
    fn new(data: Option<TempDir>) -> Self {
        Self {
            _data: data,
            home: TempDir::new().expect("create temporary HOME"),
        }
    }
}

/// The defaults every launch gets from the environment, unless the caller names the variable.
/// Flags still beat these, and the config file loses to them, as in production.
fn default_env(data_dir: &Path) -> Vec<(String, String)> {
    vec![
        ("CACHETTE_LISTEN".into(), "127.0.0.1:0".into()),
        ("CACHETTE_DATA_DIR".into(), data_dir.display().to_string()),
        ("CACHETTE_MAX_SIZE".into(), "64MiB".into()),
        ("CACHETTE_TOKENS".into(), TOKEN.into()),
    ]
}

fn command(dirs: &Dirs, data_dir: &Path, args: &[&str], env: &[(&str, &str)]) -> Command {
    let mut command = Command::new(BINARY);
    command
        .args(args)
        .env_clear()
        .env("HOME", dirs.home.path())
        .env("PATH", std::env::var_os("PATH").unwrap_or_default());
    let mut vars = default_env(data_dir);
    for (name, value) in env {
        vars.retain(|(existing, _)| existing != name);
        if !value.is_empty() {
            vars.push(((*name).to_owned(), (*value).to_owned()));
        }
    }
    command.envs(vars);
    command
}

/// Runs `cachette` until it exits by itself, for startup failures. An empty value in `env`
/// removes that variable from the defaults.
pub(crate) fn run_to_exit(args: &[&str], env: &[(&str, &str)]) -> Output {
    let dirs = Dirs::new(None);
    let data = TempDir::new().expect("create temporary data directory");
    let mut child = command(&dirs, data.path(), args, env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cachette");
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    while child.try_wait().expect("poll cachette").is_none() {
        assert!(Instant::now() < deadline, "cachette did not exit");
        thread::sleep(Duration::from_millis(20));
    }
    child.wait_with_output().expect("collect output")
}

impl Daemon {
    /// Starts `cachette` with the default environment and waits until `/healthz` answers 200.
    pub(crate) fn start(args: &[&str]) -> Self {
        Self::start_with(args, &[])
    }

    /// Like [`Daemon::start`], with `env` overriding the defaults (an empty value removes one).
    pub(crate) fn start_with(args: &[&str], env: &[(&str, &str)]) -> Self {
        let data = TempDir::new().expect("create temporary data directory");
        let data_dir = data.path().to_path_buf();
        Self::launch(Dirs::new(Some(data)), data_dir, args, env)
    }

    /// Starts a second process on the data directory of `first`, which must refuse to run, and
    /// returns what it wrote to stderr.
    pub(crate) fn start_refused_on(first: &Self, args: &[&str]) -> String {
        let dirs = Dirs::new(None);
        let env = [(
            "CACHETTE_DATA_DIR",
            first.data_dir.to_str().expect("UTF-8 path"),
        )];
        let mut child = command(&dirs, &first.data_dir, args, &env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn cachette");
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            if let Some(status) = child.try_wait().expect("poll cachette") {
                assert!(
                    !status.success(),
                    "second process on a locked directory started"
                );
                let mut stderr = String::new();
                child
                    .stderr
                    .take()
                    .expect("piped stderr")
                    .read_to_string(&mut stderr)
                    .expect("read stderr");
                return stderr;
            }
            assert!(
                Instant::now() < deadline,
                "second process neither started nor failed"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn launch(dirs: Dirs, data_dir: PathBuf, args: &[&str], env: &[(&str, &str)]) -> Self {
        let log = dirs.home.path().join("stderr.log");
        let mut child = command(&dirs, &data_dir, args, env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(fs::File::create(&log).expect("create log file"))
            .spawn()
            .expect("spawn cachette");
        let stdout = child.stdout.take().expect("piped stdout");
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let mut line = String::new();
            let read = BufReader::new(stdout).read_line(&mut line);
            let _ = sender.send(read.map(|_| line));
        });
        let line = match receiver.recv_timeout(STARTUP_TIMEOUT) {
            Ok(Ok(line)) if !line.is_empty() => line,
            other => {
                let _ = child.kill();
                panic!(
                    "no `listening` line ({other:?}); stderr:\n{}",
                    fs::read_to_string(&log).unwrap_or_default()
                );
            }
        };
        let (scheme, addr) = parse_listening(&line);
        let daemon = Self {
            child,
            addr,
            scheme,
            data_dir,
            dirs,
        };
        daemon.wait_healthy();
        daemon
    }

    fn wait_healthy(&self) {
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        while Instant::now() < deadline {
            if self.scheme == "https" {
                return;
            }
            if let Ok(response) = try_request(self.addr, "GET", "/healthz", &[])
                && response.status == 200
            {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("/healthz never answered 200; stderr:\n{}", self.logs());
    }

    /// Everything the process wrote to stderr so far.
    pub(crate) fn logs(&self) -> String {
        fs::read_to_string(self.dirs.home.path().join("stderr.log")).unwrap_or_default()
    }

    /// `GET path`.
    pub(crate) fn get(&self, path: &str) -> Response {
        self.request("GET", path, &[])
    }

    /// One request on a fresh connection, closed after the response.
    pub(crate) fn request(&self, method: &str, path: &str, headers: &[(&str, &str)]) -> Response {
        try_request(self.addr, method, path, headers)
            .unwrap_or_else(|e| panic!("{method} {path}: {e}"))
    }

    /// Sends SIGTERM and waits for the process to exit.
    pub(crate) fn terminate(&mut self, within: Duration) -> ExitStatus {
        let status = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("run kill");
        assert!(status.success(), "kill -TERM failed");
        self.wait(within)
    }

    /// Waits for the process to exit by itself.
    pub(crate) fn wait(&mut self, within: Duration) -> ExitStatus {
        let deadline = Instant::now() + within;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll cachette") {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "cachette still running after {within:?}; stderr:\n{}",
                self.logs()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn parse_listening(line: &str) -> (String, SocketAddr) {
    let url = line
        .trim()
        .strip_prefix("listening ")
        .unwrap_or_else(|| panic!("unexpected first line {line:?}"));
    let (scheme, addr) = url
        .split_once("://")
        .unwrap_or_else(|| panic!("no scheme in {url:?}"));
    (
        scheme.to_owned(),
        addr.parse()
            .unwrap_or_else(|e| panic!("bad address in {url:?}: {e}")),
    )
}

/// One HTTP/1.1 exchange over a fresh connection.
pub(crate) fn try_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> std::io::Result<Response> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    for (name, value) in headers {
        write!(request, "{name}: {value}\r\n").expect("write to a String");
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes())?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;
    parse_response(&raw)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "no HTTP status line"))
}

/// Parses a complete response read until EOF.
pub(crate) fn parse_response(raw: &[u8]) -> Option<Response> {
    let text = String::from_utf8_lossy(raw);
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())?;
    let headers = lines.map(str::to_lowercase).collect::<Vec<_>>().join("\n");
    Some(Response {
        status,
        headers,
        body: body.to_owned(),
    })
}
