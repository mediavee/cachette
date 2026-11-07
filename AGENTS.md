# cachette

Remote build cache server (Gradle `HttpBuildCache`, sccache WebDAV). A single binary crate, Rust
edition 2024.

## Gate

CI runs exactly these, on `ubuntu-latest` and `macos-latest`. Run them before pushing:

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo deny check
```

Two more jobs run on Linux: `cargo check --locked --all-targets` on the `rust-version` of
`Cargo.toml` (raise it only when the code needs it, never to follow a dependency bump), and
`cargo test --target x86_64-unknown-linux-musl`, which builds the mimalloc allocator the musl
binaries ship with and runs the smoke tests against it.

## Code rules

- The lint block in `Cargo.toml` and `clippy.toml` are BeautifulRust's, verbatim. Never add
  `#![deny(...)]` or `#![allow(...)]` in source; suppress one lint with
  `#[expect(lint, reason = "...")]` on the narrowest item.
- No `unwrap` outside tests. `expect` only with the invariant that makes it safe.
- Typed errors (`thiserror`) in modules; `anyhow` only in `main.rs`.
- Fail fast inside; fail gracefully at the client boundary. No swallowed errors.
- I/O is async; file I/O goes through tokio's blocking pool.
- **Never `ReaderStream::new` (4 KiB blocks), and never any I/O under the index lock**: not across
  an `.await`, not in `open`, not in a destructor. Decide under the lock, execute outside it.
- On a request path, record metrics through the `Metrics` handles, never the `counter!` or
  `histogram!` macros. Logs go through the non-blocking writer set up in `main.rs`: add no other
  sink.
- Never a fully qualified name inline: import it. Comments say why, not what.
- Every PUT the server does not store answers 413, or 200 to Gradle's `HttpBuildCache`, and reads
  the body to its end unless `Expect: 100-continue` lets it answer first.

## Tests

- Unit tests sit in `#[cfg(test)] mod tests` next to the code.
- Integration tests in `tests/` run the real binary through `tests/common` and talk HTTP.
- **Tests listen on port 0, always**, with a temporary data directory and `HOME`; the binary
  prints `listening <scheme>://<addr>` on its first stdout line. Never a fixed port.

## Modules

| Path | Role |
|---|---|
| `src/main.rs` | Wiring: config, logging, metrics, store, server; prints the bound address. |
| `src/config.rs` | Settings: defaults < TOML file < `CACHETTE_*` < flags; validation. |
| `src/server.rs` | Listener (TCP/TLS), connection limits, `/healthz`, `/metrics`, shutdown. |
| `src/http/` | Key routes (`GET`/`HEAD`/`PUT`, WebDAV), authentication, access log. |
| `src/store/` | Disk store. `mod.rs` is the API `http` calls: change a signature together with its callers. |
| `src/key.rs` | The `Key` type: normalisation, refusals, on-disk path. |
| `src/metrics.rs` | Every metric name, and the functions that record them. |
