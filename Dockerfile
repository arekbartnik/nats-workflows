FROM golang:1.26.5-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /nats-workflows ./cmd/nats-workflows \
    && mkdir /data

# Static binary + CA certs (workflows call swapi/OpenRouter over HTTPS).
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /nats-workflows /nats-workflows

# JetStream store; mount a volume here to keep runs across container restarts.
# Pre-created and chowned so the nonroot user (65532) can write to it — Docker
# seeds anonymous/named volumes from this directory's ownership.
COPY --from=build --chown=nonroot:nonroot /data /data
ENV NATS_DATA=/data
VOLUME /data

EXPOSE 4222 8080

ENTRYPOINT ["/nats-workflows"]
# No args = serve. Override to run one-shot commands: docker run <img> list
