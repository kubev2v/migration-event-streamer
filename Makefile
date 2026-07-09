-include .env

GOBASE=$(shell pwd)
GOBIN=$(GOBASE)/bin

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

KAFKA_TOPICS := assisted.migration.events \
	assessment \
	visitor \
	partner_customer \
	user_action

infra.up:
	@podman play kube resources/dev.yml
	@podman play kube --network host resources/observability.yml
	@echo "Waiting for Kafka broker to be ready..."
	@for i in $$(seq 1 30); do \
		podman exec event-streamer-broker /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1 && break; \
		sleep 1; \
	done
	@for topic in $(KAFKA_TOPICS); do \
		podman exec event-streamer-broker /opt/kafka/bin/kafka-topics.sh \
			--bootstrap-server localhost:9092 \
			--create --if-not-exists \
			--topic $$topic \
			--partitions 1 \
			--replication-factor 1; \
	done

infra.down:
	@podman kube down resources/dev.yml
	@podman kube down resources/observability.yml

tidy:
	@echo "🧹 Tidying go modules..."
	@go mod tidy
	@echo "✅ Go modules tidied successfully."

tidy-check: tidy
	@echo "🔍 Checking if go.mod and go.sum are tidy..."
	@git diff --quiet go.mod go.sum || (echo "❌ Detected uncommitted changes after tidy. Run 'make tidy' and commit the result." && git diff go.mod go.sum && exit 1)
	@echo "✅ go.mod and go.sum are tidy."

format:
	@echo "🧹 Formatting Go code..."
	@gofmt -s -w .
	@echo "✅ Format complete."

check-format: format
	@echo "🔍 Checking if formatting is up to date..."
	@git diff --quiet || (echo "❌ Detected uncommitted changes after format. Run 'make format' and commit the result." && git status && exit 1)
	@echo "✅ All formatted files are up to date."

##################### "make test" support start ##########################
GINKGO := $(GOBIN)/ginkgo
UNIT_TEST_PACKAGES := ./...

$(GINKGO):
	@echo "Installing ginkgo..."
	@mkdir -p $(GOBIN)
	@GOBIN=$(GOBIN) go install -v github.com/onsi/ginkgo/v2/ginkgo@v2.27.2
	@echo "'ginkgo' installed successfully."

COVERAGE_DIR := $(CURDIR)/coverage
COVERAGE_PROFILE := $(COVERAGE_DIR)/coverage.out
COVERAGE_HTML := $(COVERAGE_DIR)/coverage.html

test: $(GINKGO)
	@echo "Running Unit tests..."
	@mkdir -p $(COVERAGE_DIR)
	@$(GINKGO) -v --show-node-events --coverprofile=coverage.out --output-dir=$(COVERAGE_DIR) $(UNIT_TEST_PACKAGES)
	@echo "All Unit tests passed successfully."
	@go tool cover -func=$(COVERAGE_PROFILE) | tail -1
	@go tool cover -html=$(COVERAGE_PROFILE) -o $(COVERAGE_HTML)
	@echo "Coverage report: $(COVERAGE_HTML)"
##################### "make test" support end   ##########################

##################### "make lint" support start ##########################
GOLANGCI_LINT_VERSION := v2.10.1
GOLANGCI_LINT := $(GOBIN)/golangci-lint

check-golangci-lint-version:
	@if [ -f '$(GOLANGCI_LINT)' ]; then \
		installed=$$('$(GOLANGCI_LINT)' version 2>/dev/null | sed -n 's/.*version \([0-9.]*\).*/\1/p' | head -1); \
		required=$$(echo '$(GOLANGCI_LINT_VERSION)' | sed 's/^v//'); \
		if [ -n "$$installed" ] && [ "$$installed" != "$$required" ]; then \
			echo "🔍 Installed golangci-lint $$installed != required $(GOLANGCI_LINT_VERSION), re-installing..."; \
			rm -f '$(GOLANGCI_LINT)'; \
		fi; \
	fi

# Download golangci-lint if not present
$(GOLANGCI_LINT):
	@echo "📦 Installing golangci-lint $(GOLANGCI_LINT_VERSION)..."
	@mkdir -p $(GOBIN)
	@curl -sSfL https://golangci-lint.run/install.sh | \
	   sh -s -- -b $(GOBIN) $(GOLANGCI_LINT_VERSION)
	@echo "✅ 'golangci-lint' installed successfully."

# Run linter
lint: check-golangci-lint-version $(GOLANGCI_LINT)
	@echo "🔍 Running golangci-lint..."
	@$(GOLANGCI_LINT) run --timeout=5m
	@echo "✅ Lint passed successfully!"
##################### "make lint" support end   ##########################

.PHONY: vendor build run tidy tidy-check format check-format test lint check-golangci-lint-version
