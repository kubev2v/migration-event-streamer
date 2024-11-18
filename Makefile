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

.PHONY: vendor build run
