# Build stage
FROM golang:1.27-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

WORKDIR /app

# Copy go.mod first for better caching
COPY go.mod ./
# Download dependencies
RUN go mod download

# Copy source code (needed for go mod tidy to analyze imports)
COPY . .

# Generate go.sum with all dependencies
RUN go mod tidy

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w -X main.version=0.1.0" \
    -o /resume ./cmd/worker

# Final stage - distroless for security
FROM gcr.io/distroless/static-debian12:nonroot

# Copy binary from builder
COPY --from=builder /resume /resume

# Expose port (internal only, accessed via Consul/Traefik)
EXPOSE 8080

# Run as non-root user (UID 65532 = nonroot in distroless)
USER nonroot:nonroot

ENTRYPOINT ["/resume"]