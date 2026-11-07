# syntax=docker/dockerfile:1
#
# Runtime image: the static musl binary and nothing else. The release workflow extracts the very
# binaries it publishes into dist/<amd64|arm64>/cachette, then builds this file from them. There is no
# compiler stage on purpose: the image holds what the archives hold, byte for byte.
#
# Run it with the settings as environment variables, `CACHETTE_MAX_SIZE` and a token being the only
# mandatory ones (`CACHETTE_TOKENS="ci:write:...,dev:read:..."`):
#
#   docker run -v cachette-data:/data -p 5071:5071 -e CACHETTE_MAX_SIZE=20GiB \
#     -e CACHETTE_TOKENS=... --stop-timeout 120 ghcr.io/mediavee/cachette
#
# `--stop-timeout` matters: on SIGTERM the server drains and syncs the disk, and under the default
# `purge-on-crash` durability a stop that reaches SIGKILL empties the cache at the next start.
# Docker's default of 10 s is shorter than the server's own 30 s drain.

# Scratch has no shell, so /data is made here, on the build host's architecture (no emulation).
FROM --platform=$BUILDPLATFORM busybox:1.37 AS data
RUN mkdir /data

FROM scratch
ARG TARGETARCH

LABEL org.opencontainers.image.title="cachette" \
      org.opencontainers.image.description="Remote build cache server for Gradle and sccache" \
      org.opencontainers.image.source="https://github.com/mediavee/cachette" \
      org.opencontainers.image.licenses="MIT"

# Numeric: scratch has no /etc/passwd to resolve a name from. The data directory belongs to this
# user, so a fresh named volume (which copies the image's ownership) is writable.
COPY --from=data --chown=65532:65532 /data /data
COPY --chmod=0555 dist/${TARGETARCH}/cachette /cachette
USER 65532:65532

# The binary listens on 127.0.0.1 by default, which no published port can reach.
ENV CACHETTE_LISTEN=0.0.0.0:5071 \
    CACHETTE_DATA_DIR=/data

VOLUME /data
EXPOSE 5071

# Exec form, so cachette is PID 1 and receives the SIGTERM itself.
ENTRYPOINT ["/cachette"]
