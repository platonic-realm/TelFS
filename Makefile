# TelFS Makefile
# Run `make help` for an overview.

BIN       := bin/telfs
PKG       := ./...
GO        ?= go
GOFLAGS   ?=
LDFLAGS   ?= -s -w
MOUNTPOINT?= ./mnt

# Release metadata, embedded via -ldflags. Sourced from git when invoked
# from a working tree; CI can override via `make release VERSION=v0.4`.
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT     ?= $(shell git rev-parse --short=12 HEAD 2>/dev/null || echo none)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
RELEASE_LDFLAGS := -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.buildDate=$(BUILD_DATE)

# Cross-compile matrix for `make release-all`.
RELEASE_PLATFORMS := linux/amd64 linux/arm64

# Release artefact layout: dist/telfs-<ver>-<os>-<arch>/{telfs,LICENSE,README.md}
DIST   := dist
GOOS   ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

# Docker image tag (override via `make docker DOCKER_TAG=...`).
DOCKER_TAG ?= telfs:$(VERSION)

.PHONY: help build run test test-race lint fmt vet tidy clean mount umount \
        release release-all docker docker-run

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} \
	  /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

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
	rm -rf bin dist

mount: build ## Mount the filesystem at $(MOUNTPOINT)
	@mkdir -p $(MOUNTPOINT)
	$(BIN) mount $(MOUNTPOINT)

umount: ## Unmount the filesystem at $(MOUNTPOINT)
	fusermount -u $(MOUNTPOINT)

# ── Release ──────────────────────────────────────────────────────────

# Statically-linked release build for the current platform. Output:
#   dist/telfs-<ver>-<os>-<arch>/{telfs, LICENSE, README.md}
#   dist/telfs-<ver>-<os>-<arch>.tar.gz
release: ## Build a static binary + tar.gz for the current platform
	@mkdir -p $(DIST)
	$(MAKE) _release-one GOOS=$(GOOS) GOARCH=$(GOARCH)

# Cross-compile every entry of RELEASE_PLATFORMS.
release-all: ## Cross-build release archives for all RELEASE_PLATFORMS
	@mkdir -p $(DIST)
	@for p in $(RELEASE_PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		echo "→ $$os/$$arch"; \
		$(MAKE) -s _release-one GOOS=$$os GOARCH=$$arch || exit $$?; \
	done
	@ls -lh $(DIST)/*.tar.gz

# Internal — one platform. CGO_ENABLED=0 + -trimpath produces a static,
# reproducible binary. -ldflags embeds version/commit/buildDate.
_release-one:
	@target=$(DIST)/telfs-$(VERSION)-$(GOOS)-$(GOARCH); \
	mkdir -p $$target; \
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) \
	  $(GO) build -trimpath -ldflags '$(RELEASE_LDFLAGS)' \
	  -o $$target/telfs ./cmd/telfs; \
	cp LICENSE README.md $$target/; \
	tar czf $$target.tar.gz -C $(DIST) telfs-$(VERSION)-$(GOOS)-$(GOARCH); \
	echo "  $$(du -sh $$target.tar.gz | cut -f1)  $$target.tar.gz"

# ── Container ────────────────────────────────────────────────────────

docker: ## Build the Alpine container image (tag: $(DOCKER_TAG))
	docker build \
	  --build-arg VERSION=$(VERSION) \
	  --build-arg COMMIT=$(COMMIT) \
	  --build-arg BUILD_DATE=$(BUILD_DATE) \
	  -t $(DOCKER_TAG) .

# Example invocation — adjust to your paths.
docker-run: ## Run the container with FUSE caps + propagated bind mount
	docker run --rm -it \
	  --cap-add SYS_ADMIN --device /dev/fuse \
	  --security-opt apparmor:unconfined \
	  -v $$HOME/.config/telfs:/root/.config/telfs \
	  --mount type=bind,source=$$PWD/mnt,target=/mnt/telfs,bind-propagation=rshared \
	  -e TELFS_PROFILE=$${TELFS_PROFILE:-main} \
	  $(DOCKER_TAG) mount /mnt/telfs
