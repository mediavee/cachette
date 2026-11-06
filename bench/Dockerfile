FROM debian:13.1-slim AS nginx
RUN apt-get update && apt-get install -y --no-install-recommends nginx libnginx-mod-http-dav-ext && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["nginx", "-c", "/bench/nginx.conf", "-g", "daemon off;"]

FROM golang:1.27.3-bookworm AS foster-build
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
RUN git clone https://github.com/fosterstack/cache /src && cd /src && git checkout 31e69559cde78277192281173176a9e25d13a3bb
WORKDIR /src
RUN CGO_ENABLED=0 go build -mod=vendor -trimpath -o /fscache ./cmd/fscache
FROM scratch AS foster
COPY --from=foster-build /fscache /fscache
ENTRYPOINT ["/fscache"]

FROM rust:1.90.0-alpine3.22 AS oha-build
RUN apk add --no-cache musl-dev cmake make perl git
RUN git clone https://github.com/hatoo/oha /src && cd /src && git checkout 9766ca2b725b69b7606b99861d01c779b4aeb65d
WORKDIR /src
RUN cargo build --locked --release
FROM scratch AS oha
COPY --from=oha-build /src/target/release/oha /oha
ENTRYPOINT ["/oha"]
