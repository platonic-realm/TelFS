// Package chunk implements the file-content data path: a fixed-size chunker
// (4 MiB), an LRU on-disk cache, and the upload/download coordination with
// internal/tg.
//
// Reads pull missing chunks from Telegram on demand and write them to the
// cache. Writes stage dirty chunks in the cache; on flush/fsync they are
// passed through internal/crypto and uploaded via internal/tg.
package chunk
