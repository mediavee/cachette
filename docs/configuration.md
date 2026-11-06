# Configuration

Every setting has a TOML key, an environment variable and a command-line flag. The three
spellings are the same name: key `max_size`, variable `CACHETTE_MAX_SIZE`, flag `--max-size`.

[`contrib/cachette.example.toml`](../contrib/cachette.example.toml) is a complete file with the
reason for each value. The key names below are the ones in [`src/config.rs`](../src/config.rs).

## Precedence

From lowest to highest:

1. the built-in default,
2. the TOML file (`--config <file>`, or `CACHETTE_CONFIG`),
3. the `CACHETTE_*` environment variable,
4. the command-line flag.

A key set at a higher level replaces the lower one. There is no merging inside a key.

The file is strict. An unknown key stops the server at start, so a typo cannot go unnoticed.
Errors name the key and never repeat a secret.

Only one setting is required: `max_size`. And at least one token, unless `anonymous = "read"`.

## Sizes and durations

Sizes read `26GiB`, `512MiB`, `16KiB`, `1.5GiB`, or a plain number of bytes. Suffixes are not case
sensitive. `KiB`, `MiB` and `GiB` are powers of 1024. `KB`, `MB`, `GB` (and `K`, `M`, `G`) are
powers of 1000: `1GB` is 1 000 000 000 bytes. Use the binary suffixes: that is what the examples and
the limits below mean.

Durations read `10s`, `5m`, `2h`, `1h30m`. A unit is required: `90` is refused, `90s` is not.

## Settings

| Key | Variable | Flag | Default | What it does |
|---|---|---|---|---|
| `listen` | `CACHETTE_LISTEN` | `--listen` | `127.0.0.1:5071` | Address to listen on. Port `0` picks a free port: the first line on stdout is then `listening <scheme>://<addr>`. |
| `data_dir` | `CACHETTE_DATA_DIR` | `--data-dir` | `data` (relative to the working directory) | The cache directory. See [Data directory](#data-directory). |
| `max_size` | `CACHETTE_MAX_SIZE` | `--max-size` | none, **required** | Ceiling for stored objects, uploads in flight and evicted objects not yet deleted. Each is rounded up to 4 KiB. |
| `max_object_size` | `CACHETTE_MAX_OBJECT_SIZE` | `--max-object-size` | `512MiB`, or `max_size` if smaller | Largest accepted object. A bigger PUT is not stored but still answers `200`. Never above `max_size`. |
| `min_free_disk` | `CACHETTE_MIN_FREE_DISK` | `--min-free-disk` | `1GiB` | Free space the filesystem must keep. A PUT that would eat into it is dropped. |
| `durability` | `CACHETTE_DURABILITY` | `--durability` | `purge-on-crash` on Linux, `fsync` elsewhere | What a crash costs. `purge-on-crash` or `fsync`. See [operations](operations.md#durability-purge-on-crash-or-fsync). |
| `touch_interval` | `CACHETTE_TOUCH_INTERVAL` | `--touch-interval` | `5m` | Minimum delay between two writes of one entry's recency to disk. An entry read more often is written once per interval. |
| `header_read_timeout` | `CACHETTE_HEADER_READ_TIMEOUT` | `--header-read-timeout` | `10s` | Time a client has to send its request headers. Must be above zero. |
| `body_idle_timeout` | `CACHETTE_BODY_IDLE_TIMEOUT` | `--body-idle-timeout` | `60s` | Time a request body may stay silent. It is idle time, not total time: a slow large upload passes. Must be above zero. |
| `max_header_size` | `CACHETTE_MAX_HEADER_SIZE` | `--max-header-size` | `16KiB` | Largest request header section. At least `8KiB`. |
| `shutdown_timeout` | `CACHETTE_SHUTDOWN_TIMEOUT` | `--shutdown-timeout` | `30s` | Time requests in flight get to finish after SIGTERM. |
| `anonymous` | `CACHETTE_ANONYMOUS` | `--anonymous` | `none` | `read` lets requests without credentials read. It never lets them write. |
| `metrics_token` | `CACHETTE_METRICS_TOKEN` | `--metrics-token` | unset (open) | Bearer token protecting `/metrics`. |
| `tls_cert` | `CACHETTE_TLS_CERT` | `--tls-cert` | unset | PEM certificate chain. With `tls_key`, serves HTTPS. |
| `tls_key` | `CACHETTE_TLS_KEY` | `--tls-key` | unset | PEM private key. With `tls_cert`, serves HTTPS. |
| `log_format` | `CACHETTE_LOG_FORMAT` | `--log-format` | `text` | `text` or `json`, on stderr. |
| `access_log` | `CACHETTE_ACCESS_LOG` | `--access-log` | `errors` | `errors`, `all` or `off`. See [Logs](#logs). |
| `worker_threads` | `CACHETTE_WORKER_THREADS` | `--worker-threads` | the available parallelism | Async worker threads, at least 1. The blocking pool keeps tokio's default. |
| `[[tokens]]` | `CACHETTE_TOKENS` | `--token` | none | Named credentials. See [Tokens](#tokens). |
| | `CACHETTE_CONFIG` | `--config`, `-c` | none | The TOML file. |

`cachette --help` lists the flags.

## Tokens

Each token has a unique name, an access level (`read` or `write`) and a secret. `write` includes
`read`. The name is what the access log shows. Secrets are never logged.

In the file:

```toml
[[tokens]]
name = "ci-main"
access = "write"
secret = "REPLACE-ME"

[[tokens]]
name = "developers"
access = "read"
secret = "REPLACE-ME"
```

Put the `[[tokens]]` tables last in the file: TOML assigns every key after a `[[tokens]]` header
to that token.

In the environment, comma-separated `name:access:secret`. The secret may contain `:`.

```sh
CACHETTE_TOKENS="ci-main:write:<secret>,developers:read:<secret>"
```

`CACHETTE_TOKENS` (or `--token`, repeatable) **replaces** every `[[tokens]]` of the file. Prefer the
variable to the flag: flags show in the process list.

Make a secret with `openssl rand -hex 32`.

The server refuses to start when:

- a name is empty or used twice,
- a secret is empty,
- there is no token and `anonymous` is `none` (nobody could use the cache).

A client may send the secret as the Basic password (the user name is ignored), as Basic encoded in
base64url, or as `Bearer <secret>`. See [protocol](protocol.md#authentication).

## Limits

| Limit | Default | Setting |
|---|---|---|
| Largest object | 512 MiB | `max_object_size` |
| Free disk kept | 1 GiB | `min_free_disk` |
| Time to send the request headers | 10 s | `header_read_timeout` |
| Silence allowed in a body | 60 s | `body_idle_timeout` |
| Request header section | 16 KiB (at least 8 KiB) | `max_header_size` |
| Drain time on SIGTERM | 30 s | `shutdown_timeout` |
| Key length | 1024 bytes, 32 segments | fixed |

Over a limit, a PUT is not stored. What the client sees depends on the limit:
[protocol](protocol.md#a-put-the-server-does-not-store).

## Validation

Startup stops, naming the key, when:

- `max_size` is missing or zero,
- `max_object_size` is zero or above `max_size`,
- `max_header_size` is below 8 KiB,
- `header_read_timeout` or `body_idle_timeout` is zero,
- `worker_threads` is zero,
- only one of `tls_cert` and `tls_key` is set,
- `durability = "purge-on-crash"` is asked on a platform without `syncfs` (macOS),
- the tokens are invalid, as above.

A second process on the same `data_dir` also refuses to start.

## Logs

Logs go to stderr through a non-blocking writer. When its queue is full, the new line is dropped,
never the request.

The level comes from `RUST_LOG` (default `info`), as `tracing`'s `EnvFilter` reads it.

`access_log` decides which requests are logged, one line each with the method, key, status, bytes,
duration in microseconds and token name:

| Value | Logged |
|---|---|
| `errors` (default) | Failures, refusals (`401`, `403`), client errors other than `404`, and PUTs answered `200` without being stored. |
| `all` | Every request. A line per hit costs a syscall: for debugging. |
| `off` | Only server errors, refused credentials and dropped PUTs. |

A dropped PUT is logged at `warn`, or at `error` when the cause is an I/O failure.

## Data directory

```
<data_dir>/
  objects/<2 hex>/<64 hex>   one file per object, named by the BLAKE3 hash of its key
  objects/<2 hex>/.tmp-<n>   uploads in progress, deleted at every start
  state                      durability mode of the run and whether it ended cleanly
  lock                       held with flock for the life of the process
```

The key is never a file name, so no key can escape `objects/`.

Keep `data_dir` on a local filesystem (ext4, XFS; APFS on macOS). Do not put anything else in it:
unexpected files are ignored, never deleted, but they take disk the ceiling does not see.
