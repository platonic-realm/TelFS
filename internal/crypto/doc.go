// Package crypto defines the chunk-cipher interface used by the chunk
// pipeline. In v1 the implementation is a no-op (identity transform). In v2
// it will be AES-256-GCM with per-chunk nonces and a key derived from a
// user passphrase via Argon2id.
//
// Keeping the interface in place from v1 lets the chunk pipeline stay
// unchanged when real encryption lands.
package crypto
