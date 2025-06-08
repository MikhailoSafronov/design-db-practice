# --- Stage 1: Builder ---
FROM golang:1.24-rc-alpine AS builder

RUN apk add --no-cache git
WORKDIR /app
COPY . .
RUN go mod download

# Build binaries
RUN go build -o /app/db ./cmd/db || echo "skip db"
RUN go build -o /app/server ./cmd/server || echo "skip server"

# --- Stage 2: Tests ---
FROM builder AS test-runner
CMD ["go", "test", "-v", "./datastore"]

# --- Stage 3: Final Image ---
FROM alpine:latest
COPY --from=builder /app/db /app/db
COPY --from=builder /app/server /app/server
EXPOSE 8000 8080
