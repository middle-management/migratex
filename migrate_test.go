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

func TestMigrateFTSWithTriggers(t *testing.T) {
	db, err := sql.Open("sqlite", "test_fts_triggers.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("test_fts_triggers.db")
	defer db.Close()

	ctx := context.TODO()

	// Canonical FTS5 external-content setup: a content table whose rows are
	// mirrored into the FTS index by AFTER INSERT/UPDATE/DELETE triggers.
	// This exercises the interaction between the virtual table, the shadow
	// tables it creates (docs_fts_data, _idx, _docsize, _config, _content),
	// and the sync triggers all at once.
	schema := `
		CREATE TABLE docs (
			id INTEGER PRIMARY KEY,
			title TEXT,
			body TEXT
		);
		CREATE VIRTUAL TABLE docs_fts USING fts5(
			title, body,
			content='docs', content_rowid='id'
		);
		CREATE TRIGGER docs_ai AFTER INSERT ON docs BEGIN
			INSERT INTO docs_fts (rowid, title, body) VALUES (new.id, new.title, new.body);
		END;
		CREATE TRIGGER docs_ad AFTER DELETE ON docs BEGIN
			INSERT INTO docs_fts (docs_fts, rowid, title, body) VALUES ('delete', old.id, old.title, old.body);
		END;
		CREATE TRIGGER docs_au AFTER UPDATE ON docs BEGIN
			INSERT INTO docs_fts (docs_fts, rowid, title, body) VALUES ('delete', old.id, old.title, old.body);
			INSERT INTO docs_fts (rowid, title, body) VALUES (new.id, new.title, new.body);
		END;
	`

	// Initial migration should create the table, the FTS index, and the triggers.
	if err := migratex.Migrate(ctx, db, schema, false, nil); err != nil {
		t.Fatal("initial migration failed:", err)
	}

	// Re-planning with the identical schema must produce no operations. This
	// proves the FTS5 shadow tables are filtered out (rather than detected as
	// tables to drop) and that the triggers round-trip cleanly.
	ops, err := migratex.Plan(ctx, db, schema, false, nil)
	if err != nil {
		t.Fatal("plan failed:", err)
	}
	if len(ops) != 0 {
		for i, op := range ops {
			t.Logf("  op[%d]: %s", i, op.Normalized())
		}
		t.Fatalf("expected 0 ops for identical FTS+triggers schema, got %d", len(ops))
	}

	// Writing only to the content table should keep the FTS index in sync via
	// the triggers, so a MATCH query returns the inserted row.
	if _, err := db.ExecContext(ctx, `INSERT INTO docs (id, title, body) VALUES (1, 'Hello', 'the quick brown fox')`); err != nil {
		t.Fatal("insert failed:", err)
	}
	var title string
	if err := db.QueryRowContext(ctx, `SELECT title FROM docs_fts WHERE docs_fts MATCH 'brown'`).Scan(&title); err != nil {
		t.Fatal("fts match after insert failed:", err)
	}
	if title != "Hello" {
		t.Errorf("expected 'Hello', got %q", title)
	}

	// Updating the content table should re-index via the AFTER UPDATE trigger:
	// the old term no longer matches and the new term does.
	if _, err := db.ExecContext(ctx, `UPDATE docs SET body = 'lazy dog' WHERE id = 1`); err != nil {
		t.Fatal("update failed:", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM docs_fts WHERE docs_fts MATCH 'brown'`).Scan(&count); err != nil {
		t.Fatal("fts match after update failed:", err)
	}
	if count != 0 {
		t.Errorf("expected stale term 'brown' to be re-indexed away, got %d matches", count)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM docs_fts WHERE docs_fts MATCH 'dog'`).Scan(&count); err != nil {
		t.Fatal("fts match for new term failed:", err)
	}
	if count != 1 {
		t.Errorf("expected new term 'dog' to be indexed, got %d matches", count)
	}

	// Deleting from the content table should remove the row from the FTS index
	// via the AFTER DELETE trigger.
	if _, err := db.ExecContext(ctx, `DELETE FROM docs WHERE id = 1`); err != nil {
		t.Fatal("delete failed:", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM docs_fts WHERE docs_fts MATCH 'dog'`).Scan(&count); err != nil {
		t.Fatal("fts match after delete failed:", err)
	}
	if count != 0 {
		t.Errorf("expected deleted row to be removed from FTS index, got %d matches", count)
	}
}

func TestMigrateGeneratedColumn(t *testing.T) {
	db, err := sql.Open("sqlite", "test_gencol.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("test_gencol.db")
	defer db.Close()

	ctx := context.TODO()

	schema := `
		CREATE TABLE item (
			id INTEGER PRIMARY KEY,
			price INTEGER NOT NULL,
			tax INTEGER NOT NULL,
			total INTEGER GENERATED ALWAYS AS (price + tax) VIRTUAL
		);
	`

	if err := migratex.Migrate(ctx, db, schema, false, nil); err != nil {
		t.Fatal("initial migration failed:", err)
	}

	ops, err := migratex.Plan(ctx, db, schema, false, nil)
	if err != nil {
		t.Fatal("plan failed:", err)
	}
	if len(ops) != 0 {
		for i, op := range ops {
			t.Logf("  op[%d]: %s", i, op.Normalized())
		}
		t.Fatalf("expected 0 ops for identical generated-column schema, got %d", len(ops))
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO item (price, tax) VALUES (100, 7)`); err != nil {
		t.Fatal("insert failed:", err)
	}
	var total int
	if err := db.QueryRowContext(ctx, `SELECT total FROM item`).Scan(&total); err != nil {
		t.Fatal("select failed:", err)
	}
	if total != 107 {
		t.Errorf("expected total=107, got %d", total)
	}
}

func TestMigrateStrictTable(t *testing.T) {
	db, err := sql.Open("sqlite", "test_strict.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("test_strict.db")
	defer db.Close()

	ctx := context.TODO()

	schema := `
		CREATE TABLE account (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			balance INTEGER NOT NULL
		) STRICT;
	`

	if err := migratex.Migrate(ctx, db, schema, false, nil); err != nil {
		t.Fatal("initial migration failed:", err)
	}

	ops, err := migratex.Plan(ctx, db, schema, false, nil)
	if err != nil {
		t.Fatal("plan failed:", err)
	}
	if len(ops) != 0 {
		for i, op := range ops {
			t.Logf("  op[%d]: %s", i, op.Normalized())
		}
		t.Fatalf("expected 0 ops for identical STRICT schema, got %d", len(ops))
	}

	// STRICT should reject non-INTEGER values for INTEGER columns
	if _, err := db.ExecContext(ctx, `INSERT INTO account (name, balance) VALUES ('a', 'not-an-int')`); err == nil {
		t.Error("expected STRICT table to reject non-integer balance")
	}
}

func TestMigrateWithoutRowid(t *testing.T) {
	db, err := sql.Open("sqlite", "test_worid.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("test_worid.db")
	defer db.Close()

	ctx := context.TODO()

	schema := `
		CREATE TABLE kv (
			k TEXT PRIMARY KEY,
			v TEXT
		) WITHOUT ROWID;
	`

	if err := migratex.Migrate(ctx, db, schema, false, nil); err != nil {
		t.Fatal("initial migration failed:", err)
	}

	ops, err := migratex.Plan(ctx, db, schema, false, nil)
	if err != nil {
		t.Fatal("plan failed:", err)
	}
	if len(ops) != 0 {
		for i, op := range ops {
			t.Logf("  op[%d]: %s", i, op.Normalized())
		}
		t.Fatalf("expected 0 ops for identical WITHOUT ROWID schema, got %d", len(ops))
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO kv VALUES ('hello', 'world')`); err != nil {
		t.Fatal("insert failed:", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO kv VALUES ('hello', 'dup')`); err == nil {
		t.Error("expected primary key conflict on duplicate insert")
	}
}

func TestMigrateCheckConstraint(t *testing.T) {
	db, err := sql.Open("sqlite", "test_check.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("test_check.db")
	defer db.Close()

	ctx := context.TODO()

	schema := `
		CREATE TABLE event (
			id INTEGER PRIMARY KEY,
			level TEXT CHECK (level IN ('info', 'warn', 'error'))
		);
	`

	if err := migratex.Migrate(ctx, db, schema, false, nil); err != nil {
		t.Fatal("initial migration failed:", err)
	}

	ops, err := migratex.Plan(ctx, db, schema, false, nil)
	if err != nil {
		t.Fatal("plan failed:", err)
	}
	if len(ops) != 0 {
		for i, op := range ops {
			t.Logf("  op[%d]: %s", i, op.Normalized())
		}
		t.Fatalf("expected 0 ops for identical CHECK schema, got %d", len(ops))
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO event (level) VALUES ('debug')`); err == nil {
		t.Error("expected CHECK constraint to reject 'debug'")
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO event (level) VALUES ('info')`); err != nil {
		t.Errorf("expected 'info' to be accepted: %v", err)
	}
}

func TestMigratePartialIndex(t *testing.T) {
	db, err := sql.Open("sqlite", "test_partial.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("test_partial.db")
	defer db.Close()

	ctx := context.TODO()

	schema := `
		CREATE TABLE task (
			id INTEGER PRIMARY KEY,
			status TEXT,
			priority INTEGER
		);
		CREATE INDEX idx_active_high_priority ON task (priority) WHERE status = 'active';
	`

	if err := migratex.Migrate(ctx, db, schema, false, nil); err != nil {
		t.Fatal("initial migration failed:", err)
	}

	ops, err := migratex.Plan(ctx, db, schema, false, nil)
	if err != nil {
		t.Fatal("plan failed:", err)
	}
	if len(ops) != 0 {
		for i, op := range ops {
			t.Logf("  op[%d]: %s", i, op.Normalized())
		}
		t.Fatalf("expected 0 ops for identical partial-index schema, got %d", len(ops))
	}

	// Changing the WHERE clause should regenerate the index
	changed := `
		CREATE TABLE task (
			id INTEGER PRIMARY KEY,
			status TEXT,
			priority INTEGER
		);
		CREATE INDEX idx_active_high_priority ON task (priority) WHERE status = 'pending';
	`
	ops, err = migratex.Plan(ctx, db, changed, false, nil)
	if err != nil {
		t.Fatal("plan with changed partial index failed:", err)
	}
	if len(ops) != 2 {
		for i, op := range ops {
			t.Logf("  op[%d]: %s", i, op.Normalized())
		}
		t.Fatalf("expected 2 ops (drop + create) for changed partial index, got %d", len(ops))
	}
}

func TestMigrateForeignKeyRebuild(t *testing.T) {
	db, err := sql.Open("sqlite", "test_fk.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove("test_fk.db")
	defer db.Close()

	ctx := context.TODO()

	schema := `
		CREATE TABLE author (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		);
		CREATE TABLE book (
			id INTEGER PRIMARY KEY,
			author_id INTEGER NOT NULL REFERENCES author(id),
			title TEXT NOT NULL
		);
	`

	if err := migratex.Migrate(ctx, db, schema, false, nil); err != nil {
		t.Fatal("initial migration failed:", err)
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO author (id, name) VALUES (1, 'Ada')`); err != nil {
		t.Fatal("author insert failed:", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO book (author_id, title) VALUES (1, 'Notes')`); err != nil {
		t.Fatal("book insert failed:", err)
	}

	// Add a column to the parent table; this triggers a rebuild of `author`.
	updated := `
		CREATE TABLE author (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			bio TEXT
		);
		CREATE TABLE book (
			id INTEGER PRIMARY KEY,
			author_id INTEGER NOT NULL REFERENCES author(id),
			title TEXT NOT NULL
		);
	`
	if err := migratex.Migrate(ctx, db, updated, false, nil); err != nil {
		t.Fatal("rebuild migration failed:", err)
	}

	// Existing data must survive the rebuild.
	var name, title string
	if err := db.QueryRowContext(ctx, `
		SELECT a.name, b.title FROM author a JOIN book b ON b.author_id = a.id
	`).Scan(&name, &title); err != nil {
		t.Fatal("post-rebuild join failed:", err)
	}
	if name != "Ada" || title != "Notes" {
		t.Errorf("expected Ada/Notes, got %q/%q", name, title)
	}

	// FK enforcement should still reject orphan inserts.
	if _, err := db.ExecContext(ctx, `PRAGMA foreign_keys = ON`); err != nil {
		t.Fatal("enabling foreign_keys failed:", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO book (author_id, title) VALUES (999, 'orphan')`); err == nil {
		t.Error("expected FK violation for orphan author_id")
	}
}
