# Stage 1: Build minimal static binary
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install root CA certificates
RUN apk add --no-cache ca-certificates git

# Cache Go dependency layers
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Compile optimized static binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -extldflags '-static'" \
    -o /bin/flux-limiter .

# Stage 2: Ultra-lightweight production runtime
FROM alpine:3.20

RUN apk --no-cache add ca-certificates tzdata \
    && addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

COPY --from=builder /bin/flux-limiter /app/flux-limiter

USER appuser

EXPOSE 8080

ENTRYPOINT ["/app/flux-limiter"]
