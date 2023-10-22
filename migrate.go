package migratex

import (
	"context"
	"database/sql"
)

func Migrate(ctx context.Context, actual *sql.DB, schema string, allowDeletions bool) error {
	ops, err := Plan(ctx, actual, schema, allowDeletions)
	if err != nil {
		return err
	}
	err = Apply(ctx, actual, ops)
	if err != nil {
		return err
	}
	return nil
}
