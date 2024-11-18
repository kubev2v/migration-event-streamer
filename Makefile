include .env

vendor:
	go mod tidy
	go mod vendor

build:
	go build -o bin/streamer main.go

run:
	@env $(cat $(PWD)/.env | xargs) bin/streamer

build.podman:
	@podman build . -t quay.io/ctupangiu/migration-event-streamer:latest
	@podman push quay.io/ctupangiu/migration-event-streamer:latest

infra.up:
	@podman play kube resources/dev.yml

infra.down:
	@podman kube down resources/dev.yml

.PHONY: vendor build run
