# TelFS Makefile
# Run `make help` for an overview.

BIN       := bin/telfs
PKG       := ./...
GO        ?= go
GOFLAGS   ?=
LDFLAGS   ?= -s -w
MOUNTPOINT?= ./mnt

.PHONY: help build run test test-race lint fmt vet tidy clean mount umount

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} \
	  /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

build: ## Build the telfs binary into bin/
	@mkdir -p bin
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/telfs

run: build ## Build then run telfs with no args (prints usage)
	$(BIN)

test: ## Run unit tests
	$(GO) test $(GOFLAGS) $(PKG)

test-race: ## Run unit tests with the race detector
	$(GO) test -race $(GOFLAGS) $(PKG)

lint: ## Run go vet (add golangci-lint later if desired)
	$(GO) vet $(PKG)

fmt: ## Format all Go files
	$(GO) fmt $(PKG)

vet: lint ## Alias for lint

tidy: ## Run go mod tidy
	$(GO) mod tidy

clean: ## Remove build artifacts
	rm -rf bin

mount: build ## Mount the filesystem at $(MOUNTPOINT)
	@mkdir -p $(MOUNTPOINT)
	$(BIN) mount $(MOUNTPOINT)

umount: ## Unmount the filesystem at $(MOUNTPOINT)
	fusermount -u $(MOUNTPOINT)
