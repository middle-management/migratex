package migratex

import (
	"context"
	"database/sql"
)

func Apply(ctx context.Context, actual *sql.DB, operations []Operation) error {
	tx, err := actual.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "PRAGMA foreign_keys = OFF")
	if err != nil {
		return err
	}

	for _, op := range operations {
		_, err := tx.ExecContext(ctx, string(op))
		if err != nil {
			return err
		}
	}

	_, err = tx.ExecContext(ctx, "PRAGMA foreign_keys = ON")
	if err != nil {
		return err
	}

	return tx.Commit()
}
