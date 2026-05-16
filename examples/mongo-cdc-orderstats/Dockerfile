# Multi-binary image for the mongo-cdc-orderstats example. Both
# binaries land in / so the same image can run as either the one-shot
# bootstrap (Mongo collection scan) or the long-running CDC worker
# (Kafka Change Stream consumer).
#
# Build at the repo root:
#   docker build -f examples/mongo-cdc-orderstats/Dockerfile -t murmur-mongo-cdc:dev .
#
# Run (locally, with the docker-compose stack):
#   docker run --network host -e MONGO_URI=... -e DDB_LOCAL_ENDPOINT=... \
#       --entrypoint /murmur-bootstrap murmur-mongo-cdc:dev
FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=arm64

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" \
    -o /out/murmur-bootstrap ./examples/mongo-cdc-orderstats/cmd/bootstrap

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" \
    -o /out/murmur-worker ./examples/mongo-cdc-orderstats/cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/murmur-bootstrap /murmur-bootstrap
COPY --from=build /out/murmur-worker /murmur-worker

# No default ENTRYPOINT — runners pick /murmur-bootstrap or
# /murmur-worker explicitly. Matches the testcontainers test-pattern.
