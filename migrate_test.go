package migratex_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
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
	`, false, nil)
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
	`, false, nil)
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
		`, false, nil)
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
			`, false, nil)
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
		`, false, nil)
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
	`, false, nil)
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

func TestExcludeTables(t *testing.T) {
	db, err := sql.Open("sqlite", "test_exclude.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("test_exclude.db")
	defer db.Close()

	ctx := context.TODO()

	// Create the database with a table and some "internal" tables (like litestream creates)
	_, err = db.ExecContext(ctx, `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
		CREATE TABLE _litestream_seq (id INTEGER PRIMARY KEY, seq INTEGER);
		CREATE TABLE _litestream_lock (id INTEGER PRIMARY KEY);
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Schema only defines the users table - without exclude, Plan would want to drop the internal tables
	schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

	// Without exclude and without allow-deletions, Plan should error about removing tables
	_, err = migratex.Plan(ctx, db, schema, false, nil)
	if err == nil {
		t.Fatal("expected error about removing tables without allow-deletions")
	}

	// With exclude, Plan should produce no ops since the internal tables are ignored
	ops, err := migratex.Plan(ctx, db, schema, false, &migratex.Exclusions{
		Tables: []string{"_litestream_seq", "_litestream_lock"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 0 {
		for i, op := range ops {
			t.Logf("  op[%d]: %s", i, op.Normalized())
		}
		t.Fatalf("expected 0 ops with excluded tables, got %d", len(ops))
	}

	// Excluded tables should still exist in the actual database
	var count int
	err = db.QueryRowContext(ctx, `SELECT count(*) FROM _litestream_seq`).Scan(&count)
	if err != nil {
		t.Fatal("excluded table _litestream_seq should still exist:", err)
	}
}

func TestExcludeColumns(t *testing.T) {
	db, err := sql.Open("sqlite", "test_exclude_cols.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("test_exclude_cols.db")
	defer db.Close()

	ctx := context.TODO()

	// Create a table with an extra column that only exists in the actual DB
	_, err = db.ExecContext(ctx, `
		CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, internal_seq INTEGER);
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Schema doesn't include internal_seq - without exclusion this would error or try to modify
	schema := `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`

	// Without exclusion, Plan detects a modification (column removal blocked by default)
	_, err = migratex.Plan(ctx, db, schema, false, nil)
	if err == nil {
		t.Fatal("expected error about removing columns without allow-deletions")
	}

	// With column exclusion, the extra column is ignored during diff
	ops, err := migratex.Plan(ctx, db, schema, false, &migratex.Exclusions{
		Columns: map[string][]string{
			"users": {"internal_seq"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 0 {
		for i, op := range ops {
			t.Logf("  op[%d]: %s", i, op.Normalized())
		}
		t.Fatalf("expected 0 ops with excluded columns, got %d", len(ops))
	}
}

func TestMigrateVirtualTable(t *testing.T) {
	db, err := sql.Open("sqlite", "test_vt.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("test_vt.db")
	defer db.Close()

	ctx := context.TODO()

	schema := `
		CREATE TABLE product (
			id INTEGER PRIMARY KEY,
			name TEXT,
			normalized_name TEXT
		);
		CREATE VIRTUAL TABLE product_fts USING fts5(
			name, normalized_name,
			content=product, content_rowid=id,
			tokenize='unicode61 remove_diacritics 2'
		);
	`

	// Initial migration should succeed
	err = migratex.Migrate(ctx, db, schema, false, nil)
	if err != nil {
		t.Fatal("initial migration failed:", err)
	}

	// Plan with same schema should produce no operations (idempotent)
	ops, err := migratex.Plan(ctx, db, schema, false, nil)
	if err != nil {
		t.Fatal("plan failed:", err)
	}
	if len(ops) != 0 {
		for i, op := range ops {
			t.Logf("  op[%d]: %s", i, op.Normalized())
		}
		t.Fatalf("expected 0 ops for identical schema, got %d", len(ops))
	}

	// Insert data and verify FTS works
	_, err = db.ExecContext(ctx, `INSERT INTO product (id, name, normalized_name) VALUES (1, 'Café Latte', 'cafe latte')`)
	if err != nil {
		t.Fatal("insert failed:", err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO product_fts (rowid, name, normalized_name) VALUES (1, 'Café Latte', 'cafe latte')`)
	if err != nil {
		t.Fatal("fts insert failed:", err)
	}

	var matchedName string
	err = db.QueryRowContext(ctx, `SELECT name FROM product_fts WHERE product_fts MATCH 'cafe'`).Scan(&matchedName)
	if err != nil {
		t.Fatal("fts query failed:", err)
	}
	if matchedName != "Café Latte" {
		t.Errorf("expected 'Café Latte', got %q", matchedName)
	}

	// Modified virtual table schema should produce drop + create ops
	modifiedSchema := `
		CREATE TABLE product (
			id INTEGER PRIMARY KEY,
			name TEXT,
			normalized_name TEXT
		);
		CREATE VIRTUAL TABLE product_fts USING fts5(
			name, normalized_name,
			content=product, content_rowid=id,
			tokenize='unicode61 remove_diacritics 1'
		);
	`
	ops, err = migratex.Plan(ctx, db, modifiedSchema, false, nil)
	if err != nil {
		t.Fatal("plan with modified virtual table failed:", err)
	}
	if len(ops) != 2 {
		for i, op := range ops {
			t.Logf("  op[%d]: %s", i, op.Normalized())
		}
		t.Fatalf("expected 2 ops (drop + create) for modified virtual table, got %d", len(ops))
	}
	if !strings.Contains(string(ops[0]), "DROP TABLE") {
		t.Errorf("expected first op to be DROP TABLE, got: %s", ops[0])
	}
	if !strings.Contains(string(ops[1]), "CREATE VIRTUAL TABLE") {
		t.Errorf("expected second op to be CREATE VIRTUAL TABLE, got: %s", ops[1])
	}

	// Adding a new virtual table alongside existing one
	extendedSchema := `
		CREATE TABLE product (
			id INTEGER PRIMARY KEY,
			name TEXT,
			normalized_name TEXT
		);
		CREATE VIRTUAL TABLE product_fts USING fts5(
			name, normalized_name,
			content=product, content_rowid=id,
			tokenize='unicode61 remove_diacritics 2'
		);
		CREATE TABLE category (
			id INTEGER PRIMARY KEY,
			name TEXT
		);
		CREATE VIRTUAL TABLE category_fts USING fts5(
			name,
			content=category, content_rowid=id
		);
	`
	ops, err = migratex.Plan(ctx, db, extendedSchema, false, nil)
	if err != nil {
		t.Fatal("plan with added virtual table failed:", err)
	}
	// Should add category table + category_fts virtual table (2 ops)
	if len(ops) != 2 {
		for i, op := range ops {
			t.Logf("  op[%d]: %s", i, op.Normalized())
		}
		t.Fatalf("expected 2 ops (add table + add virtual table), got %d", len(ops))
	}
}
