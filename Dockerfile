# Render deploys this via runtime: docker.
# Builds the cmd/engine entrypoint into a small static binary.

# ---- Build stage ----
FROM golang:1.22-alpine AS builder
WORKDIR /src

# Cache module downloads first (separate layer) so rebuilds are fast.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# -tags netgo: static binary, no cgo DNS issues on the slim runtime.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -tags netgo \
    -ldflags '-s -w' \
    -o /out/matching-engine \
    ./cmd/engine

# ---- Runtime stage ----
# Alpine base keeps it tiny; ca-certificates are REQUIRED for TLS to
# Aiven Postgres/Redis/Kafka.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
RUN addgroup -S app && adduser -S -G app app
COPY --from=builder /out/matching-engine /usr/local/bin/matching-engine
# Kafka CA cert: Render mounts this as a Secret File at /etc/secrets/kafka-ca.pem.
# Set KAFKA_CA_CERT_PATH=/etc/secrets/kafka-ca.pem in the service's env vars.
USER app
# Render sets $PORT and routes to it; main.go now reads PORT (falls back to 8080).
CMD ["matching-engine"]
