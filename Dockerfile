FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/rinjing-agen ./cmd/rinjing-agen

FROM alpine:3.21
# uid tetap supaya folder data di host bisa di-chown sebelum container pertama jalan
RUN adduser -D -H -u 10001 agen && mkdir -p /data && chown agen:agen /data
COPY --from=build /out/rinjing-agen /usr/local/bin/rinjing-agen
USER agen
VOLUME /data
EXPOSE 9100
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["rinjing-agen", "healthcheck"]
ENTRYPOINT ["rinjing-agen"]
