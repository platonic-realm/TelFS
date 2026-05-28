// Package meta owns the authoritative filesystem metadata, stored in a local
// SQLite database (modernc.org/sqlite — pure Go).
//
// Schema: inodes, dirents, chunk_map, xattrs, journal, meta_kv. See
// docs/architecture.md for the full schema and docs/channel-format.md for
// how mutations are serialized when posted to the channel.
package meta
