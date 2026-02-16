# =========================
# Build Stage
# =========================
FROM golang:1.21-alpine AS builder

# Create app directory
WORKDIR /app

# Install git (required for some Go modules)
RUN apk add --no-cache git

# Copy Go module files first (better cache)
COPY go.mod go.sum ./
RUN go mod download

# Copy full source
COPY . .

# Build static Linux binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o mirador-nrt-aggregator ./cmd/mirador-nrt-aggregator


# =========================
# Runtime Stage
# =========================
FROM gcr.io/distroless/base-debian12

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/mirador-nrt-aggregator /app/mirador-nrt-aggregator

# Copy default config (optional — can mount instead)
COPY config.yaml /app/config.yaml

# Run as non-root (distroless default)
USER nonroot:nonroot

# Mirador ports
EXPOSE 8080
EXPOSE 4317
EXPOSE 4318

# Start service
ENTRYPOINT ["/app/mirador-nrt-aggregator"]
