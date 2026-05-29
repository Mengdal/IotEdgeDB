# IEDB - High-Performance Time-Series Database (Go)
# Multi-stage build for minimal image size

ARG VERSION=dev

# Build stage
FROM golang:1.26-bookworm AS builder

WORKDIR /build

# Copy go mod files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy source code
COPY . .

# Download frontend from iedb-ui releases
RUN apt-get update && apt-get install -y curl unzip && \
    TAG=$(curl -fsSL https://api.github.com/repos/Mengdal/iedb-ui/releases/latest | grep '"tag_name"' | sed 's/.*"tag_name": *"//;s/".*//') && \
    curl -fsSL "https://github.com/Mengdal/iedb-ui/releases/download/${TAG}/front.zip" -o /tmp/front.zip && \
    unzip -o /tmp/front.zip && \
    rm /tmp/front.zip && \
    apt-get remove -y curl unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Build
ARG VERSION
RUN go build -v \
    -tags=duckdb_arrow \
    -ldflags="-s -w -X main.Version=${VERSION}" \
    -o iedb ./cmd/iedb

# Production stage
FROM debian:bookworm-slim

ARG VERSION

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 iedb && \
    mkdir -p /app/data && \
    chown -R iedb:iedb /app

WORKDIR /app

# Copy binary from builder
COPY --from=builder --chown=iedb:iedb /build/iedb .

# Copy frontend static files
COPY --from=builder --chown=iedb:iedb /build/front ./front

# Copy default config
COPY --chown=iedb:iedb iedb.toml .

# Create VERSION file
RUN echo "${VERSION}" > VERSION && chown iedb:iedb VERSION

# Switch to non-root user
USER iedb

# Data volume
VOLUME ["/app/data"]

# Expose API port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run iedb
ENTRYPOINT ["./iedb"]
