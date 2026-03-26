# HeliosProxy Production Image
# Multi-stage build for optimized image size

# =============================================================================
# Stage 1: Build
# =============================================================================
FROM rust:latest AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    clang \
    libclang-dev \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy source code
COPY . .

# Build release binary with all features + postgres topology provider
RUN cargo build --release \
    --features "all-features,postgres-topology" \
    -p heliosdb-proxy

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 helios

# Create directories
RUN mkdir -p /etc/helios /var/lib/helios && \
    chown -R helios:helios /etc/helios /var/lib/helios

# Copy binary from builder
COPY --from=builder /build/target/release/heliosdb-proxy /usr/local/bin/

# Set ownership
RUN chown helios:helios /usr/local/bin/heliosdb-proxy

USER helios

WORKDIR /var/lib/helios

# Expose ports: 6432 (client connections), 9090 (admin/metrics API)
EXPOSE 6432 9090

# Health check against admin API
HEALTHCHECK --interval=10s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:9090/health || exit 1

# Default command
ENTRYPOINT ["/usr/local/bin/heliosdb-proxy"]
CMD ["--config", "/etc/helios/proxy.toml"]
