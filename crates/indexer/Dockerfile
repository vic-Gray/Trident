# syntax=docker/dockerfile:1

# ── builder ──────────────────────────────────────────────────────────────────
FROM rust:1.81-slim AS builder

WORKDIR /build

# Install build-time system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace manifest and lock file first to allow layer caching
COPY Cargo.toml Cargo.lock ./

# Copy all crates so Cargo can resolve the workspace
COPY crates/ ./crates/

# Build only the indexer binary in release mode
RUN cargo build --release -p trident-indexer

# ── runtime ──────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    libpq5 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -u 1001 -m -s /bin/sh trident

COPY --from=builder /build/target/release/trident-indexer /app/trident-indexer

USER trident

ENTRYPOINT ["/app/trident-indexer"]
