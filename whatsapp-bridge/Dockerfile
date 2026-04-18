# syntax=docker/dockerfile:1
# Build context is the repo root (see railway.toml: dockerfilePath = "whatsapp-bridge/Dockerfile").
FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY whatsapp-bridge/go.mod whatsapp-bridge/go.sum ./
RUN go mod download
COPY whatsapp-bridge/ .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/bridge .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 bridge
WORKDIR /app
COPY --from=builder /out/bridge /app/bridge
USER bridge
EXPOSE 8080
ENTRYPOINT ["/app/bridge"]
