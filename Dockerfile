FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/monitor ./cmd/monitor \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/watchdog ./cmd/watchdog \
    && mkdir /out/data

FROM alpine:3.21
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/monitor /out/watchdog /usr/local/bin/
COPY --from=build --chown=10001:10001 /out/data /data
USER 10001:10001
ENV MONITOR_DB_PATH=/data/monitor.db
VOLUME /data
EXPOSE 8080 8085
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["monitor"]
