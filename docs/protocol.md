# Protocol

What the server answers, to which client, and why. Each section names the test that checks it and the
client code it is written against.

The tests are black box: they start the real binary and talk HTTP to it
([`tests/`](../tests/)). The real clients run in the
[`clients` workflow](../.github/workflows/clients.yml).

## Resources

One resource per path. `GET`, `HEAD` and `PUT` work on `/<key>`, where the key is a path:

- Gradle sends `/<md5 hex>`,
- sccache sends `/<prefix>/a/b/c/<key>`.

Two exact paths are not keys: `GET /healthz` and `GET /metrics`. See
[operations](operations.md#health). Nothing can be written there: a `PUT` or `MKCOL` on them
is refused with `405`, after authentication.

| Request | Needs | Answer |
|---|---|---|
| `GET /<key>` | `read` | `200` with the body and an exact `Content-Length`, or `404`. |
| `HEAD /<key>` | `read` | `200` with the headers of the `GET`, or `404`. |
| `PUT /<key>` | `write` | `201` when stored. `200` when [not stored](#a-put-the-server-does-not-store). `411` without `Content-Length`. |
| `PROPFIND /<path>` | `read` | `207`. See [WebDAV](#webdav-for-sccache). |
| `MKCOL /<path>` | `write` | `201`, and nothing is created. |
| `OPTIONS` | `read` | `200` with `Allow` and `DAV: 1`. |
| anything else (`DELETE`, `MOVE`, `COPY`, `LOCK`, `PROPPATCH`, `POST`, `PATCH`...) | none | `405` with `Allow`, before authentication. |

A `GET` that fails inside the store (a disk error) is answered as a miss and logged: a `5xx` would
make Gradle turn its remote cache off. There is no `Range`, no listing and no `DELETE`.

Checked by `unsupported_methods_are_405` in [`tests/http_gradle.rs`](../tests/http_gradle.rs) and
`reserved_paths_cannot_be_written` in [`tests/http_sccache.rs`](../tests/http_sccache.rs).

## Keys

The key is the request path, percent-decoded once (strict UTF-8), with repeated `/` merged and the
final `/` removed. `%2F` separates segments like `/`. So an endpoint that ends in `/` does not
produce a different key.

A request is refused with `400` when the key:

- has a `.` or `..` segment,
- holds a control character,
- is not valid UTF-8 after decoding,
- is longer than 1024 bytes, or has more than 32 segments,
- is empty (`GET`, `HEAD` or `PUT` on `/`).

The file on disk is named by the BLAKE3 hash of the normalised key, never by the key. A key cannot
reach outside `objects/`.

Checked by `refused_keys_are_400_for_every_method` in
[`tests/http_sccache.rs`](../tests/http_sccache.rs), and by the property tests in
[`src/key.rs`](../src/key.rs).

## Authentication

Checks happen in this order: method, credentials, key, `Content-Length`, admission. A refused
request is refused before the server looks at the rest.

The secret of a token is accepted in three forms:

| Form | Used by |
|---|---|
| Basic, secret as the **password**. The user name is ignored. | Gradle (it can only send Basic) |
| Basic encoded in base64url, with or without `=` padding | Bazel |
| `Authorization: Bearer <secret>` | sccache with `SCCACHE_WEBDAV_TOKEN` |

Secrets are compared in constant time and never logged. The access log shows the token name.

| Situation | Answer |
|---|---|
| No credentials, `anonymous = "none"` | `401` with `WWW-Authenticate: Basic realm="cachette"` |
| Credentials that match no token | `401`. After a Bearer header, also `WWW-Authenticate: Bearer error="invalid_token"`. |
| A `read` token on `PUT` or `MKCOL` | `403` |
| No credentials, `anonymous = "read"`, a read | served |
| No credentials, `anonymous = "read"`, a write | `401` |
| Wrong credentials, `anonymous = "read"` | `401`: a misconfigured client sees the error instead of silently reading |

The `403` is loud on purpose. Gradle warns and turns its remote cache off for the rest of that
build, so a `read` token configured to push is noticed.

Checked by `every_form_of_the_secret_is_accepted`, `unknown_credentials_are_401_with_the_right_challenge`,
`a_read_token_reads_but_cannot_write`, `anonymous_read_opens_reads_only` and
`secrets_never_reach_the_logs` in [`tests/http_gradle.rs`](../tests/http_gradle.rs). The `read-push`
scenario of the [`clients` workflow](../.github/workflows/clients.yml) shows what Gradle prints.

## `PUT`

- `Content-Length` is required. A chunked `PUT` gets `411` and the connection closes. A
  `Content-Length` that is not a number gets `400`.
- The announced size is compared to `max_object_size` and reserved under `max_size` **before** the
  body is read.
- The object is written to a temporary file and renamed. `201` is sent after the rename, never
  before.
- Two `PUT`s on one key both succeed. The last rename wins, and the key counts once. A reader that
  already opened the file keeps the old version.
- A body that ends early (the client hangs up) stores nothing, gives the reservation back and
  never gets a `201`. A body that stays silent for `body_idle_timeout` is cut the same way.

Checked by `a_chunked_put_is_411`, `a_chunked_put_that_also_has_a_length_is_not_stored` and
`a_put_without_any_framing_is_411` in [`tests/http_gradle.rs`](../tests/http_gradle.rs);
`a_stalled_body_is_cut_at_the_idle_timeout_and_stores_nothing` and
`a_client_that_hangs_up_mid_body_stores_nothing` in [`tests/http_limits.rs`](../tests/http_limits.rs);
`replacing_a_key_counts_it_once` in [`src/store/mod.rs`](../src/store/mod.rs).

## A `PUT` the server does not store

A `PUT` can be refused for room, size or readiness. The server still answers **`200`**, with an empty
body. Never `413`, never `5xx`.

| `reason` (metric label and log) | When |
|---|---|
| `too_large` | The announced size is above `max_object_size`. |
| `no_space` | The object does not fit under `max_size` after evicting what can be evicted, or the disk would fall under `min_free_disk`. |
| `not_ready` | The start-up scan is still building the index. |
| `io` | The disk failed after the object was admitted. Logged at `error`. |

Each one counts in `cachette_puts_dropped_total{reason}` and is logged at `warn`, whatever
`access_log` says.

### Why `200`

In Gradle's code, a store answered with any status that is not 2xx makes Gradle disable the remote
cache for the rest of the build, reads included. `413` is no exception:

- [`HttpBuildCacheService`](https://github.com/gradle/gradle/blob/v9.8.2/platforms/core-execution/build-cache-http/src/main/java/org/gradle/caching/http/internal/HttpBuildCacheService.java#L140-L148)
  accepts only 2xx on `PUT` and throws for the rest. `413` is not in the list of fatal codes
  ([lines 51 to 62](https://github.com/gradle/gradle/blob/v9.8.2/platforms/core-execution/build-cache-http/src/main/java/org/gradle/caching/http/internal/HttpBuildCacheService.java#L51-L62)),
  and that list changes nothing: it only decides which exception type is thrown
  ([lines 164 to 170](https://github.com/gradle/gradle/blob/v9.8.2/platforms/core-execution/build-cache-http/src/main/java/org/gradle/caching/http/internal/HttpBuildCacheService.java#L164-L170)).
- [`BaseRemoteBuildCacheServiceHandle`](https://github.com/gradle/gradle/blob/v9.8.2/platforms/core-execution/build-cache/src/main/java/org/gradle/caching/internal/controller/service/BaseRemoteBuildCacheServiceHandle.java#L150-L158)
  catches any exception from a load or a store, sets `disabled = true`, and the build goes on
  without the remote cache.
- The [Gradle documentation](https://github.com/gradle/gradle/blob/v9.8.2/platforms/documentation/docs/src/docs/userguide/optimizing-builds/build-cache/build_cache.adoc#L329)
  says a `413` "will not be treated as an error". The code above does not do that.

So answering `200` is the only way to refuse an object and leave the cache on. The price is that the
client believes the object is stored: the next build rebuilds it, and the metric says why.

### The body is read to its end

A `200` must not break the client's connection.

- **Without `Expect: 100-continue`** (Gradle's default), the client has already sent its body, or is
  sending it. The server reads and discards it to the end, then answers `200` on a connection that
  stays usable. If it answered first, the connection would be reset under the client, which would
  fail or wait out its timeout and turn the cache off.
- **With `Expect: 100-continue`**, the decision is made on the headers. The `200` goes out at once,
  with no `100 Continue`. The client does not send its body, and the connection closes
  (`Connection: close`).

Nothing waits between the headers and the first read of the body: not the index, not an eviction,
not a lock.

The tests use a body of 8 MiB, above any socket buffer; a server that did not read the body would
otherwise pass:
`an_oversized_put_without_expect_is_read_to_its_end_and_answered_200`,
`an_oversized_put_with_expect_is_answered_200_without_a_100_or_a_body` and
`a_put_beyond_the_ceiling_is_dropped_the_same_way` in
[`tests/http_limits.rs`](../tests/http_limits.rs).

The real Gradle shows the same in the `d5` scenario of the
[`clients` workflow](../.github/workflows/clients.yml): an output of 10 MiB over a
`max_object_size` of 1 MiB, with and without `useExpectContinue`. The build passes, Gradle prints no
remote-cache warning, and the other entries are stored and served at the next build.

### A refused `PUT` that has a body

A `401`, `403`, `400` or `411` is a real error and is answered as such. The body is handled the same
way:

- with `Expect: 100-continue`, the answer goes out unread and the client sends no body;
- without it, the body is read and discarded when it is announced no larger than `max_object_size`,
  so the client receives the status instead of a reset. A larger or unannounced body is not read:
  the answer goes out and the connection closes.

Checked by `expect_continue_without_a_token_gets_401_and_never_invites_the_body`,
`expect_continue_with_a_read_token_gets_403_without_a_100`,
`refused_puts_that_send_first_still_get_their_status` and
`a_refused_put_announced_above_the_object_limit_is_answered_at_once` in
[`tests/http_gradle.rs`](../tests/http_gradle.rs).

## WebDAV for sccache

sccache's WebDAV backend (OpenDAL) sends `GET`, `PROPFIND` with `Depth: 0`, `MKCOL` and `PUT`. The
server implements that subset.

**`PROPFIND`**

- On a stored key, `207` with `getcontentlength`, `getlastmodified` and an empty `resourcetype`.
- On any other valid path, `207` as a **collection**: `href`, `getlastmodified` (RFC 2822 date),
  `<D:collection/>` and a `HTTP/1.1 200 OK` status. Directories are virtual: they always exist.
  So sccache's check "does the parent directory exist" succeeds and it never needs to create one.
- `Depth: 0`, `Depth: 1` or no `Depth` header answers about the resource itself. There is no
  listing, so `Depth: 1` does not list children. `Depth: infinity` gets `403`. Any other value gets
  `400`.
- The request body is read and discarded.

**`MKCOL`** answers `201` and creates nothing. OpenDAL accepts only `201` and `405` for it, and a
`200` would fail the write
([`core.rs`](https://github.com/apache/opendal/blob/v0.58.1/core/services/webdav/src/core.rs#L464-L495),
the OpenDAL version sccache 0.19 pins). `MKCOL` needs a `write` token.

**`OPTIONS`** answers `200` with `Allow` and `DAV: 1`.

**`GET`** always carries an exact `Content-Length`, which OpenDAL's reader needs.

Checked by `replays_the_opendal_sequence_on_one_connection`, `the_root_and_missing_paths_are_collections`,
`depth_is_zero_or_one_and_infinity_is_refused`, `options_announces_webdav` and
`a_large_propfind_body_in_segments_keeps_the_connection_usable` in
[`tests/http_sccache.rs`](../tests/http_sccache.rs).

## What each client does

### Gradle (supported, tested with the real client)

Source: [`HttpBuildCacheService.java`](https://github.com/gradle/gradle/blob/v9.8.2/platforms/core-execution/build-cache-http/src/main/java/org/gradle/caching/http/internal/HttpBuildCacheService.java)
at Gradle 9.8.2.

| Gradle does | The server |
|---|---|
| `GET <url>/<key>`. The key is 32 hex characters (MD5). | `200` is a hit. `404` is a miss. Gradle treats any other status as a failure. |
| `PUT <url>/<key>` with a `Content-Length`, never chunked. | `201`, or `200` when not stored. Gradle accepts any 2xx. |
| Never sends `HEAD`. | `HEAD` works, for other clients. |
| Basic authentication, sent preemptively, from `credentials { }`. | The secret is the password. |
| `Expect: 100-continue` only with `useExpectContinue = true`. | Both modes work. |
| A plain `http://` URL needs `allowInsecureProtocol = true`. | Serve HTTPS with `tls_cert` and `tls_key`, or keep the server on a private network. |
| Any failed request turns the remote cache off for the rest of the build. | No `413`, no avoidable `5xx`. `401` and `403` stay, on purpose. |

Checked by `replays_the_gradle_sequence` in [`tests/http_gradle.rs`](../tests/http_gradle.rs) and by
the `gradle` jobs of the [`clients` workflow](../.github/workflows/clients.yml): Gradle 9.8.1 and
the latest 8.x, on Linux and macOS. A build with a `write` token fills the cache. From another
directory, a build with a `read` token serves every cacheable task `FROM-CACHE`.

### sccache (supported, tested with the real client)

Sources: [`docs/Webdav.md`](https://github.com/mozilla/sccache/blob/v0.19.0/docs/Webdav.md) and
[`src/config.rs`](https://github.com/mozilla/sccache/blob/v0.19.0/src/config.rs#L1131-L1139) at
sccache 0.19.0.

| sccache does | The server |
|---|---|
| Starts with `GET <endpoint>/<prefix>/.sccache_check`. | `404`, which sccache accepts. |
| Unless read-only, checks write access: `PROPFIND`, then `PUT .sccache_check` with `Hello, World!`. | `207`, then `201`. |
| `GET <endpoint>/<prefix>/a/b/c/<key>`, no `Range`. | `200` with `Content-Length`, or `404`. |
| Before each `PUT`, a `PROPFIND` with `Depth: 0` on the parent directory. | `207` as a collection, so no `MKCOL` follows. |
| `PUT <endpoint>/<prefix>/a/b/c/<key>` with `Content-Length`. | `201`. |
| With `SCCACHE_WEBDAV_DISABLE_CREATE_DIR=true`: no `PROPFIND`, no `MKCOL`. | Nothing is needed. |
| `SCCACHE_WEBDAV_TOKEN` is sent as `Authorization: Bearer`. | A token secret. |

The `sccache` jobs of the [`clients` workflow](../.github/workflows/clients.yml) run the pinned
sccache 0.19.0 release through [`tests/clients/sccache/run.sh`](../tests/clients/sccache/run.sh).
The first build writes with a `write` token. After `cargo clean`, the second build reads with a
`read` token in `READ_ONLY` mode and must report remote hits. A second variant sets
`SCCACHE_WEBDAV_DISABLE_CREATE_DIR=true` and asserts that the server saw **no** `PROPFIND` and no
`MKCOL`. The default variant asserts it saw at least one `PROPFIND`.

### Maven build cache extension (should work, untested)

No test runs Maven. From the extension's code
([`RemoteCacheRepositoryImpl.java`](https://github.com/apache/maven-build-cache-extension/blob/maven-build-cache-extension-1.3.0/src/main/java/org/apache/maven/buildcache/RemoteCacheRepositoryImpl.java),
release 1.3.0):

- it sends `GET` and `PUT` on multi-segment paths such as
  `v1.2/<groupId>/<artifactId>/<checksum>/<file>`, which keys accept;
- `404` is a miss, and it sends no `HEAD`;
- Maven Resolver's HTTP transport
  ([`HttpTransporter.java`](https://github.com/apache/maven-resolver/blob/maven-resolver-1.9.27/maven-resolver-transport-http/src/main/java/org/eclipse/aether/transport/http/HttpTransporter.java#L623-L624),
  1.9.27) sends `Expect: 100-continue` on a `PUT` with a body, and the server handles it;
- the unreleased master adds an `OPTIONS` before the first `PUT`, then `MKCOL` when the answer has a
  `DAV` header. The server answers `OPTIONS` with `DAV: 1`, and `MKCOL` is a no-op that needs a
  `write` token.

### Bazel HTTP cache (should work, untested)

No test runs Bazel. From Bazel's code
([`HttpCacheClient.java`](https://github.com/bazelbuild/bazel/blob/2ae7b8050d32/src/main/java/com/google/devtools/build/lib/remote/http/HttpCacheClient.java),
[`AbstractHttpHandler.java`](https://github.com/bazelbuild/bazel/blob/2ae7b8050d32/src/main/java/com/google/devtools/build/lib/remote/http/AbstractHttpHandler.java)):

- it sends `GET` and `PUT` on `<url>/ac/<hex>` and `<url>/cas/<hex>`, which are ordinary keys;
- a miss is `404`, an upload succeeds on `200`, `201`, `202` or `204`, and it sends no `HEAD`;
- credentials in the URL become Basic encoded in base64url, which the server accepts.

The server does not check that a `cas/` body hashes to its key, and does not validate action results.

### Not supported

Turborepo needs per-object metadata stored and echoed back as response headers. The server stores
none.
