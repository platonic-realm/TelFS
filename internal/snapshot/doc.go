// Package snapshot serializes the local SQLite database, gzips it, and posts
// it to the configured Telegram channel as a snapshot message. It also
// handles cold-mount recovery: scanning the channel for the most recent
// snapshot, restoring the DB, and replaying any subsequent meta-op messages.
//
// Cadence: every N metadata mutations or every T minutes, whichever comes
// first, plus on clean unmount. After a successful snapshot post, superseded
// snapshots and replayed meta-ops are deleted from the channel.
package snapshot
