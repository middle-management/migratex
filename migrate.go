package migratex

import (
	"context"
	"database/sql"
)

func Migrate(ctx context.Context, actual *sql.DB, schema string, allowDeletions bool, exclusions *Exclusions) error {
	ops, err := Plan(ctx, actual, schema, allowDeletions, exclusions)
	if err != nil {
		return err
	}
	err = Apply(ctx, actual, ops)
	if err != nil {
		return err
	}
	return nil
}
