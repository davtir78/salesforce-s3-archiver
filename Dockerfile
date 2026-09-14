# syntax=docker/dockerfile:1

# Builds all binaries into one image; choose the command at run time:
#   docker run IMAGE sf-archive-stream -config /config/config.yml
#   docker run IMAGE sf-archive-eventlog -config /config/config.yml
#   docker run IMAGE mock-salesforce
#   docker run IMAGE sf-archive-verify -bucket ...

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X github.com/davtir78/salesforce-s3-archiver/internal/archive.CollectorVersion=${VERSION}" \
      -o /out/ ./cmd/...

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata \
 && adduser -D -H -u 10001 archiver
COPY --from=build /out/ /usr/local/bin/
USER 10001
ENV LOG_LEVEL=info
ENTRYPOINT []
CMD ["sf-archive-stream", "-config", "/config/config.yml"]
