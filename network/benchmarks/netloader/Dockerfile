# Build image
FROM golang:1.23 AS build-env

ARG gopkg=k8s.io/perf-tests/network/benchmarks/netloader
ADD ["./", "/go/src/${gopkg}"]
WORKDIR /go/src/${gopkg}
RUN ls -la

RUN CGO_ENABLED=0 go build -o /netloader ./cmd

# Runtime image
FROM alpine:3.20
# install curl
RUN apk add --no-cache curl

COPY --from=build-env /netloader /netloader

#Make sure that when compiled on debian, the binary will run on alpine
RUN mkdir -p /lib64 && ln -s /lib/ld-musl-x86_64.so.1 /lib64/ld-linux-x86-64.so.2

ENTRYPOINT ["/netloader"]