# Stage 1: Compile ShardRAFT executables.
ARG GO_VERSION=1.27.3

FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src

ENV CGO_ENABLED=0

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN mkdir -p /out && \
    go build -trimpath -ldflags="-s -w" \
        -o /out/shardraftd ./cmd/shardraftd && \
    go build -trimpath -ldflags="-s -w" \
        -o /out/shardraft-gateway ./cmd/shardraft-gateway && \
    go build -trimpath -ldflags="-s -w" \
        -o /out/shardraft ./cmd/shardraft

# Stage 2: Minimal runtime environment.
FROM alpine:3.22 AS runtime

RUN apk add --no-cache ca-certificates curl && \
    addgroup -S -g 10001 shardraft && \
    adduser -S -D -H -u 10001 \
        -G shardraft shardraft && \
    mkdir -p /data && \
    chown -R shardraft:shardraft /data

COPY --from=builder /out/shardraftd \
    /usr/local/bin/shardraftd

COPY --from=builder /out/shardraft-gateway \
    /usr/local/bin/shardraft-gateway

COPY --from=builder /out/shardraft \
    /usr/local/bin/shardraft

WORKDIR /data

USER shardraft

EXPOSE 8080 8085 9090

CMD ["shardraftd"]
