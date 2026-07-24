# syntax=docker/dockerfile:1

# ---- Builder ----
FROM rust:1.89-slim AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock* rust-toolchain.toml ./
COPY crates ./crates

RUN cargo build --release --bin connectforge-node

# ---- Runtime ----
FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd -u 10001 -m appuser \
    && mkdir -p /data && chown 10001:10001 /data

COPY --from=builder /app/target/release/connectforge-node /usr/local/bin/connectforge-node

USER 10001
EXPOSE 8080
ENV CONNECTFORGE_ADDR=0.0.0.0:8080 \
    CONNECTFORGE_DATA_DIR=/data
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/connectforge-node"]
CMD ["serve"]
