package migratex_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/middle-management/migratex"
	_ "modernc.org/sqlite"
)

func TestMigrate(t *testing.T) {
	db, err := sql.Open("sqlite", "test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("test.db")
	defer db.Close()

	ctx := context.TODO()
	err = migratex.Migrate(ctx, db, `
		CREATE TABLE "Node" (
			A TEXT,
			C TEXT,
			"E F G" TEXT,
			h TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_node_a ON "Node" (A);
	`, false)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.ExecContext(ctx, `INSERT INTO Node (A) VALUES ('a')`)
	if err != nil {
		t.Fatal(err)
	}

	ops, err := migratex.Plan(ctx, db, `
		CREATE TABLE "Node" (
			A TEXT,
			C TEXT,
			"E F G" TEXT,
			h TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_node_a ON "Node" (A);
	`, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 0 {
		t.Fatalf("expected no ops, got %d", len(ops))
	}

	ops, err = migratex.Plan(ctx, db, `
			CREATE TABLE "Node" (
				A TEXT,
				C TEXT,
				"E F G" TEXT,
				h TEXT
			);
			CREATE INDEX IF NOT EXISTS idx_node_a ON "Node" (A);
			CREATE TABLE "Other" (x INT);
		`, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 {
		t.Fatalf("expected 1 ops, got %d", len(ops))
	}

	ops, err = migratex.Plan(ctx, db, `
				CREATE TABLE "Node" (
					A TEXT,
					C TEXT,
					"E F G" TEXT,
					h TEXT,
					x INT
				);
				CREATE INDEX IF NOT EXISTS idx_node_a ON "Node" (A);
				CREATE TABLE "Other" (x INT);
			`, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 5 { // 1 op for new table and 4 ops to alter table safely
		t.Fatalf("expected 5 ops, got %d", len(ops))
	}

	// make sure migration does not pass with invalid schema
	err = migratex.Migrate(ctx, db, `
			CREATE TABLE "Node" (
				A TEXT,
				C TEXT,
				"E F G" TEXT,
				h TEXT,
				x TEXT DEFAULT 'x', -- invalid because of extra comma
			);
			CREATE INDEX IF NOT EXISTS idx_node_a ON "Node" (A);
		`, false)
	if err == nil {
		t.Fatal("expected error")
	}

	// should add x column
	err = migratex.Migrate(ctx, db, `
		CREATE TABLE "Node" (
			A TEXT,
			C TEXT,
			"E F G" TEXT,
			h TEXT,
			x TEXT DEFAULT 'x'
		);
		CREATE INDEX IF NOT EXISTS idx_node_a ON "Node" (A);
	`, false)
	if err != nil {
		t.Fatal(err)
	}

	// make sure the migration moved data
	var A, C, EFG, h, x sql.NullString
	err = db.QueryRowContext(ctx, `SELECT A,C,"E F G",h,x FROM Node`).Scan(
		&A,
		&C,
		&EFG,
		&h,
		&x,
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("queried", A, C, EFG, h, x)
	if A.String != "a" {
		t.Error("invalid A", A)
	}
	if C.Valid || EFG.Valid || h.Valid {
		t.Error("invalid C, EFG or h", A, C, EFG, h)
	}
	if x.String != "x" {
		t.Error("invalid x", x)
	}
}
