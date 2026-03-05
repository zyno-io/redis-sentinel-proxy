FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /redis-sentinel-proxy .

FROM alpine:3.21
COPY --from=build /redis-sentinel-proxy /usr/local/bin/redis-sentinel-proxy
ENTRYPOINT ["redis-sentinel-proxy"]
