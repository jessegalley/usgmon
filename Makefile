.PHONY: build clean install test lint

# Build variables
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -ldflags "-s -w \
	-X github.com/jgalley/usgmon/internal/cli.Version=$(VERSION) \
	-X github.com/jgalley/usgmon/internal/cli.Commit=$(COMMIT) \
	-X github.com/jgalley/usgmon/internal/cli.BuildDate=$(BUILD_DATE)"

# Build static binary (CGO_ENABLED=0 for pure Go SQLite driver)
build:
	CGO_ENABLED=0 go build $(LDFLAGS) -o bin/usgmon ./cmd/usgmon

# Build for multiple platforms
build-all: build-linux build-darwin

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o bin/usgmon-linux-amd64 ./cmd/usgmon
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o bin/usgmon-linux-arm64 ./cmd/usgmon

build-darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o bin/usgmon-darwin-amd64 ./cmd/usgmon
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o bin/usgmon-darwin-arm64 ./cmd/usgmon

# Install binary and config
install: build
	install -d /usr/local/bin
	install -m 755 bin/usgmon /usr/local/bin/usgmon
	install -d /etc/usgmon
	install -m 644 configs/usgmon.example.yaml /etc/usgmon/usgmon.yaml.example
	install -d /var/lib/usgmon
	install -d /etc/systemd/system
	install -m 644 systemd/usgmon.service /etc/systemd/system/usgmon.service

# Run tests
test:
	go test -v -race ./...

# Run linter
lint:
	golangci-lint run ./...

# Clean build artifacts
clean:
	rm -rf bin/

# Download dependencies
deps:
	go mod download

# Tidy dependencies
tidy:
	go mod tidy

# Run locally for development
run: build
	./bin/usgmon serve --config configs/usgmon.example.yaml

# Show version
version:
	@echo "Version: $(VERSION)"
	@echo "Commit: $(COMMIT)"
	@echo "Build Date: $(BUILD_DATE)"
