include .env

GIT_COMMIT ?=$(shell git rev-parse --short "HEAD^{commit}" 2>/dev/null)

LD_FLAGS := -ldflags "-X github.com/kubev2v/migration-event-streamer/cmd.GitCommit=$(GIT_COMMIT)"

vendor:
	go mod tidy
	go mod vendor

build:
	go build $(LD_FLAGS) -o bin/streamer main.go

build.producer:
	go build -o bin/producer $(PWD)/samples/producer/main.go

run:
	bin/streamer run --config $(PWD)/resources/config.yaml

build.podman:
	@podman build . -t quay.io/ctupangiu/migration-event-streamer:latest
	@podman push quay.io/ctupangiu/migration-event-streamer:latest

infra.up:
	@podman play kube resources/dev.yml
	@podman play kube --network host resources/observability.yml
	

infra.down:
	@podman kube down resources/dev.yml
	@podman kube down resources/prometheus.yml

.PHONY: vendor build run
