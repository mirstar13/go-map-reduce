# Build stage
FROM golang:1.25-alpine AS builder

# Install necessary packages for building
RUN apk add --no-cache git ca-certificates

WORKDIR /app

# Copy go mod files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary with CGO disabled for static linking
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /bin/mapreduce ./cmd/cli

# Final stage
FROM alpine:3.19

# Install CA certificates for HTTPS connections
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Copy the binary from builder
COPY --from=builder /bin/mapreduce /bin/mapreduce

# Use non-root user
USER appuser

# Entry point
ENTRYPOINT ["/bin/mapreduce"]