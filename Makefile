.PHONY: help build test run clean install deps fmt lint

# Variables
BINARY_NAME=iedb
GO=go
GOFLAGS=-v -tags=duckdb_arrow
MAIN_PATH=./cmd/iedb

# windows go build -v -tags=duckdb_arrow -o iedb.exe ./cmd/iedb

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

deps: ## Download Go dependencies
	$(GO) mod download
	$(GO) mod verify

install: ## Install dependencies (alias for deps)
	@make deps

build: ## Build the binary
	$(GO) build $(GOFLAGS) -o $(BINARY_NAME) $(MAIN_PATH)

build-linux-arm64: ## Build for Linux ARM64 (requires aarch64-linux-gnu-gcc) 放进main目录编译防止交叉编译失败  CGO_ENABLED=1 GOOS=linux GOARCH=arm64 CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ go build -v -tags=duckdb_arrow -o iedb_arm64 ./
	CGO_ENABLED=1 GOOS=linux GOARCH=arm64 CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ $(GO) build $(GOFLAGS) $(GOTAGS) -o ${BINARY_NAME}_arm64 ./

run: ## Run iedb directly (without building)
	$(GO) run $(GOFLAGS) $(MAIN_PATH)

test: ## Run all tests
	$(GO) test $(GOFLAGS) -race -coverprofile=coverage.out ./...

test-coverage: test ## Run tests with coverage report
	$(GO) tool cover -html=coverage.out

bench: ## Run benchmarks
	$(GO) test -bench=. -benchmem ./...

fmt: ## Format Go code
	$(GO) fmt ./...
	gofmt -s -w .

lint: ## Run linter (requires golangci-lint)
	golangci-lint run

clean: ## Clean build artifacts
	rm -f $(BINARY_NAME)
	rm -f coverage.out
	rm -rf ./data/iedb/*

dev: ## Run in development mode with hot reload (requires air)
	air

docker-build: ## Build Docker image
	docker build -t iedb:latest .

docker-run: ## Run Docker container
	docker run -p 8000:8000 iedb:latest

# Development helpers
watch-test: ## Watch and run tests on file changes (requires entr)
	find . -name '*.go' | entr -c make test

.DEFAULT_GOAL := help
