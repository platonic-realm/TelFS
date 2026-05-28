// Package tg wraps gotd/td to provide TelFS-specific Telegram operations:
// MTProto authentication, channel resolution, message posting and fetching,
// document upload/download, and FLOOD_WAIT-aware retry.
//
// The package serializes MTProto calls behind a single connection; bulk
// operations use bounded concurrency.
package tg
