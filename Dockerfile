FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /qbrouter ./cmd/qbrouter

FROM alpine:3.21

RUN apk add --no-cache rsync ca-certificates

COPY --from=builder /qbrouter /usr/local/bin/qbrouter

ENTRYPOINT ["qbrouter"]
