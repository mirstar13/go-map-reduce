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
    go build -ldflags="-s -w" -o /bin/worker ./services/worker

# Final stage
FROM alpine:3.19

# Install CA certificates and python3
RUN apk --no-cache add ca-certificates python3

# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

# Copy the binary from builder
COPY --from=builder /bin/worker /bin/worker

# Entry point
ENTRYPOINT ["/bin/worker"]