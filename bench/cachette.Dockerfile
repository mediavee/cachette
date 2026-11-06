FROM rust:1.90.0-alpine3.22 AS build
RUN apk add --no-cache musl-dev cmake make perl
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --target x86_64-unknown-linux-musl
FROM scratch
COPY --from=build /src/target/x86_64-unknown-linux-musl/release/cachette /cachette
ENTRYPOINT ["/cachette"]
