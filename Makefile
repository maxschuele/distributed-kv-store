.PHONY: all build test clean run fmt vet deps help

BINARY_NAME=node
BINARY_PATH=./bin/$(BINARY_NAME)
CMD_PATH=./cmd/node
GO=go

all: build

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p ./bin
	$(GO) build -o $(BINARY_PATH) $(CMD_PATH)

run: build
	@echo "Running $(BINARY_NAME)..."
	$(BINARY_PATH)

test:
	@echo "Running tests..."
	$(GO) test -v ./...

fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...

vet:
	@echo "Running go vet..."
	$(GO) vet ./...

deps:
	@echo "Downloading dependencies..."
	$(GO) mod download
	$(GO) mod tidy

help:
	@echo "Available targets:"
	@echo "  build         - Build the binary"
	@echo "  run           - Build and run the application"
	@echo "  test          - Run tests"
	@echo "  fmt           - Format code"
	@echo "  vet           - Run go vet"
	@echo "  deps          - Download and tidy dependencies"
	@echo "  clean         - Remove build artifacts and logs"
	@echo "  help          - Show this help message"
