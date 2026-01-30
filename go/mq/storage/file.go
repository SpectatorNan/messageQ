// Deprecated: FileStorage is deprecated — use WALStorage (mq/storage/wal.go) instead for persistence.
package storage

// FileStorage has been removed in favor of WALStorage (binary WAL) in mq/storage/wal.go.
// This file is intentionally left blank to signal a full migration to WAL-based persistence.

// If you need a legacy text-based storage implementation, refer to the project's history
// or re-implement a compatible adapter that satisfies the Storage interface.
