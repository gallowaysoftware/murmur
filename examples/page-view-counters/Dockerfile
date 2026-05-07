# Multi-binary image for the page-view-counters example. Both binaries land in /
# so the Terraform pipeline-counter module's default `command = ["/murmur-worker"]`
# / `["/murmur-query"]` work out of the box.
#
# Build at the repo root:
#   docker build -f examples/page-view-counters/Dockerfile -t murmur-page-views:dev .
FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=arm64

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" \
    -o /out/murmur-worker ./examples/page-view-counters/cmd/worker

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" \
    -o /out/murmur-query ./examples/page-view-counters/cmd/query

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/murmur-worker /murmur-worker
COPY --from=build /out/murmur-query /murmur-query

# Default to the query server; the Terraform module overrides command for the worker.
ENTRYPOINT ["/murmur-query"]
