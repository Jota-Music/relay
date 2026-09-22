# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY cmd/relay ./cmd/relay
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /relay ./cmd/relay

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /relay /relay
EXPOSE 8080
ENTRYPOINT ["/relay"]
