# syntax=docker/dockerfile:1.6
#
# TelFS container — Alpine + fuse3 + static telfs binary.
#
# Intended for headless / NAS / server deployments where the host
# already has FUSE but a Go toolchain + glibc are inconvenient. For
# interactive desktop use, prefer the static binary from `make release`.
#
# Build:
#   docker build -t telfs:latest .
#
# Run a mount (host mountpoint must use shared propagation so the FUSE
# mount inside the container is visible to host processes):
#   docker run --rm -it \
#     --cap-add SYS_ADMIN --device /dev/fuse \
#     --security-opt apparmor:unconfined \
#     -v $HOME/.config/telfs:/root/.config/telfs \
#     --mount type=bind,source=/srv/external,target=/mnt/telfs,bind-propagation=rshared \
#     -e TELFS_PROFILE=main \
#     telfs:latest mount /mnt/telfs
#
# Run the management UI:
#   docker run --rm -it -p 8080:8080 \
#     -v $HOME/.config/telfs:/root/.config/telfs \
#     telfs:latest web --listen 0.0.0.0:8080 --token $YOUR_TOKEN

# ── builder ──────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

WORKDIR /src

# Cache deps separately from the source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static build: CGO_ENABLED=0, -trimpath, embedded build metadata.
RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags "-s -w \
      -X main.version=${VERSION} \
      -X main.commit=${COMMIT} \
      -X main.buildDate=${BUILD_DATE}" \
    -o /out/telfs ./cmd/telfs

# ── runtime ──────────────────────────────────────────────────────────
FROM alpine:3.20

# fuse3 provides fusermount3 (needed for clean unmount fallback);
# ca-certificates is needed for outbound TLS to Telegram.
RUN apk add --no-cache fuse3 ca-certificates tini \
    && ln -sf fusermount3 /usr/bin/fusermount

# The container expects two bind mounts at runtime:
#   /root/.config/telfs  — host's ~/.config/telfs (rw; holds session +
#                          db + cache + config.toml per profile)
#   /mnt/telfs           — host mountpoint where the FUSE FS will live
#                          (bind-propagation=rshared so host sees it)
#
# We don't override XDG_CONFIG_HOME; the default UserHomeDir lookup
# (HOME=/root inside the container) lands at /root/.config/telfs,
# which the bind mount maps to the host.

COPY --from=builder /out/telfs /usr/local/bin/telfs

# tini reaps the FUSE-mount child and forwards signals cleanly.
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/telfs"]
CMD ["--help"]
