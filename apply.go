package migratex

import (
	"context"
	"database/sql"
)

// Apply runs operations as a single transaction.
//
// PRAGMA configuration is the caller's responsibility. In particular, table
// rebuilds drop and recreate tables, so callers whose schema uses foreign
// keys should set `PRAGMA foreign_keys = OFF` on the connection before
// calling — `PRAGMA foreign_keys` cannot be changed from inside a transaction
// and so cannot be managed by Apply itself.
func Apply(ctx context.Context, actual *sql.DB, operations []Operation) error {
	tx, err := actual.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, op := range operations {
		_, err := tx.ExecContext(ctx, string(op))
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
