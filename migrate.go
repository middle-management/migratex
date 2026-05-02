package migratex

import (
	"context"
	"database/sql"
)

// Migrate plans and applies a migration to bring `actual` in line with
// `schema`. See Apply for the contract around connection-level PRAGMAs.
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
