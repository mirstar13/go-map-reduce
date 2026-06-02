FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o shuffle-service ./services/shuffle

FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/shuffle-service .
EXPOSE 50051
CMD ["./shuffle-service"]
