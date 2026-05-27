# Build stage
FROM golang:1.25-alpine AS builder

# Install necessary packages
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy go mod files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary with CGO disabled for static linking
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -v -ldflags="-s -w" -o /bin/builder ./services/builder

# Final stage
FROM golang:1.25-alpine

# Install build dependencies for go-plugin (if any CGO were needed, but we use CGO_ENABLED=0)
# We need git for go mod download/tidy
RUN apk add --no-cache git ca-certificates tzdata build-base

# Create non-root user with a home directory
RUN addgroup -S appgroup && adduser -D -S appuser -G appgroup

# Create and grant permissions to Go cache directories
RUN mkdir -p /.cache /tmp/go-cache /tmp/go-mod /tmp/go && \
    chmod -R 777 /.cache /tmp/go-cache /tmp/go-mod /tmp/go

USER appuser

# Set environment variables for Go toolchain
ENV HOME=/tmp
ENV GOCACHE=/tmp/go-cache
ENV GOMODCACHE=/tmp/go-mod
ENV GOPATH=/tmp/go

# Copy the binary from builder
COPY --from=builder /bin/builder /bin/builder

# Copy core project source for building plugins
COPY . /app/core

# Pre-download dependencies to populate the module cache
WORKDIR /app/core
RUN go mod download

# Entry point
WORKDIR /app
ENTRYPOINT ["/bin/builder"]