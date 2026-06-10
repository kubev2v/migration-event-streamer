# Builder container
FROM --platform=linux/amd64 registry.access.redhat.com/ubi9/go-toolset AS builder

USER 0

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
RUN mkdir /gocache

COPY . .

RUN GOCACHE=/gocache CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /streamer main.go

FROM registry.access.redhat.com/ubi9/ubi-micro

WORKDIR /app

COPY --from=builder /streamer /app/

# Use non-root user
RUN chown -R 1000:1000 /app
USER 1000

ENTRYPOINT ["/app/streamer"]
