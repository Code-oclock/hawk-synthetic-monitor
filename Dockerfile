FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/monitor ./cmd/monitor

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 monitor \
    && mkdir /data && chown monitor /data
COPY --from=build /out/monitor /usr/local/bin/monitor
USER monitor
ENV MONITOR_DB_PATH=/data/monitor.db
VOLUME /data
EXPOSE 8080 8085
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["monitor"]
