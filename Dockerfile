FROM registry.a.tt.se/docker/golang:1.20.3-alpine3.17 AS build

WORKDIR /usr/src

ADD go.mod go.sum ./

ADD . ./

ARG COMMAND

RUN go build -o /build/main ./cmd/${COMMAND}

FROM registry.a.tt.se/docker/alpine:3.17.3

COPY --from=build /build/main /usr/local/bin/main

RUN apk upgrade --no-cache

# API server
EXPOSE 1080

# Debug/profiling server
EXPOSE 1081

ENTRYPOINT ["main"]
