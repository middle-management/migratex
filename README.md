# migratex

A simple migration tool for sqlite, originally based on https://david.rothlis.net/declarative-schema-migration-for-sqlite

## PRAGMAs

Connection-level PRAGMAs (`foreign_keys`, `journal_mode`, etc.) are the
caller's responsibility — migratex does not set or restore them. If your
schema uses foreign keys, set `PRAGMA foreign_keys = OFF` on the connection
before calling `Migrate`/`Apply`, since table rebuilds drop and recreate
tables and `foreign_keys` cannot be changed from inside the migration's
transaction.
