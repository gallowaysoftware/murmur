# Multi-binary image for the recently-interacted-topk example.
# Both the long-running Kafka ECS worker AND the gRPC query server
# land in / so a single image powers either role. The lambda binary
# (Kinesis trigger) ships as a separate AWS Lambda zip via the
# build pipeline; not in this image.
#
# Build at the repo root:
#   docker build -f examples/recently-interacted-topk/Dockerfile -t murmur-topk:dev .
FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=arm64

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" \
    -o /out/murmur-worker ./examples/recently-interacted-topk/cmd/worker

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" \
    -o /out/murmur-query ./examples/recently-interacted-topk/cmd/query

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/murmur-worker /murmur-worker
COPY --from=build /out/murmur-query /murmur-query

# No default ENTRYPOINT — runners pick /murmur-worker or
# /murmur-query explicitly.
