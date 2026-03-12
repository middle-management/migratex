package migratex

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
)

type Exclusions struct {
	Tables  []string            // table names to fully exclude from migration
	Columns map[string][]string // table name -> column names to exclude from diff
}

type Operation string

func (op Operation) Normalized() string {
	return normaliseSql(string(op))
}

const sqlIndices = `
	SELECT
		name,
		sql
	FROM
		sqlite_master
	WHERE
		type = "index"
	ORDER BY
		name
`

const sqlTables = `
	SELECT
		name,
		sql
	FROM
		sqlite_master
	WHERE
		type = "table"
	AND
		name != "sqlite_sequence"
	AND
		sql NOT LIKE 'CREATE VIRTUAL TABLE%'
	ORDER BY
		name
`

const sqlVirtualTables = `
	SELECT
		name,
		sql
	FROM
		sqlite_master
	WHERE
		type = "table"
	AND
		sql LIKE 'CREATE VIRTUAL TABLE%'
	ORDER BY
		name
`

const sqlViews = `
	SELECT
		name,
		sql
	FROM
		sqlite_master
	WHERE
		type = "view"
	AND
		name != "sqlite_sequence"
	ORDER BY
		name
`

const sqlTriggers = `
	SELECT
		name,
		sql
	FROM
		sqlite_master
	WHERE
		type = "trigger"
	AND
		name != "sqlite_sequence"
	ORDER BY
		name
`

const sqlColumns = `
	SELECT
        name,
        json_array(cid, type, "notnull", dflt_value, pk)
    FROM
        pragma_table_info($1)
    ORDER BY
        cid
`

func Plan(ctx context.Context, actual *sql.DB, schema string, allowDeletions bool, exclusions *Exclusions) ([]Operation, error) {
	wanted, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	_, err = wanted.ExecContext(ctx, schema)
	if err != nil {
		return nil, err
	}

	var ops []Operation
	var addOperation = func(query string) {
		ops = append(ops, Operation(query))
	}

	wantedTables, err := mapKeyValue(ctx, wanted, sqlTables)
	if err != nil {
		return nil, err
	}
	wantedIndices, err := mapKeyValue(ctx, wanted, sqlIndices)
	if err != nil {
		return nil, err
	}
	wantedTriggers, err := mapKeyValue(ctx, wanted, sqlTriggers)
	if err != nil {
		return nil, err
	}
	wantedViews, err := mapKeyValue(ctx, wanted, sqlViews)
	if err != nil {
		return nil, err
	}
	wantedVirtualTables, err := mapKeyValue(ctx, wanted, sqlVirtualTables)
	if err != nil {
		return nil, err
	}

	actualTables, err := mapKeyValue(ctx, actual, sqlTables)
	if err != nil {
		return nil, err
	}
	actualIndices, err := mapKeyValue(ctx, actual, sqlIndices)
	if err != nil {
		return nil, err
	}
	actualTriggers, err := mapKeyValue(ctx, actual, sqlTriggers)
	if err != nil {
		return nil, err
	}
	actualViews, err := mapKeyValue(ctx, actual, sqlViews)
	if err != nil {
		return nil, err
	}
	actualVirtualTables, err := mapKeyValue(ctx, actual, sqlVirtualTables)
	if err != nil {
		return nil, err
	}

	// Filter out shadow tables created by virtual tables (e.g. FTS5)
	wantedTables = filterShadowTables(wantedTables, wantedVirtualTables, actualVirtualTables)
	actualTables = filterShadowTables(actualTables, wantedVirtualTables, actualVirtualTables)

	// Filter out explicitly excluded tables
	if exclusions != nil {
		for _, name := range exclusions.Tables {
			delete(wantedTables, name)
			delete(actualTables, name)
		}
	}

	eqSql := func(a, b string) bool {
		return normaliseSql(a) == normaliseSql(b)
	}

	addedTables := diffKeys(actualTables, wantedTables)
	removedTables := diffKeys(wantedTables, actualTables)
	modifiedTables := diffValues(wantedTables, actualTables, eqSql)
	addedVirtualTables := diffKeys(actualVirtualTables, wantedVirtualTables)
	removedVirtualTables := diffKeys(wantedVirtualTables, actualVirtualTables)
	modifiedVirtualTables := diffValues(wantedVirtualTables, actualVirtualTables, eqSql)
	addedIndices := diffKeys(actualIndices, wantedIndices)
	removedIndices := diffKeys(wantedIndices, actualIndices)
	modifiedIndices := diffValues(wantedIndices, actualIndices, eqSql)
	addedViews := diffKeys(actualViews, wantedViews)
	removedViews := diffKeys(wantedViews, actualViews)
	modifiedViews := diffValues(wantedViews, actualViews, eqSql)
	addedTriggers := diffKeys(actualTriggers, wantedTriggers)
	removedTriggers := diffKeys(wantedTriggers, actualTriggers)
	modifiedTriggers := diffValues(wantedTriggers, actualTriggers, eqSql)

	if len(removedTables) > 0 && !allowDeletions {
		return nil, fmt.Errorf("will not remove tables: %v", removedTables)
	}
	if len(removedVirtualTables) > 0 && !allowDeletions {
		return nil, fmt.Errorf("will not remove virtual tables: %v", removedVirtualTables)
	}

	// Drop removed/modified virtual tables before regular table operations,
	// since virtual tables may reference regular tables being modified.
	for _, name := range removedVirtualTables {
		addOperation(fmt.Sprintf(`DROP TABLE "%s"`, name))
	}
	for _, name := range modifiedVirtualTables {
		addOperation(fmt.Sprintf(`DROP TABLE "%s"`, name))
	}

	for _, name := range addedTables {
		addOperation(wantedTables[name])
	}

	for _, name := range removedTables {
		addOperation(fmt.Sprintf(`DROP TABLE "%s"`, name))
	}

	suffix := "_migratex_" + strconv.Itoa(rand.Int())
	for _, tableName := range modifiedTables {
		wantedColumns, err := mapKeyValue(ctx, wanted, sqlColumns, tableName)
		if err != nil {
			return nil, err
		}

		actualColumns, err := mapKeyValue(ctx, actual, sqlColumns, tableName)
		if err != nil {
			return nil, err
		}

		// Filter out excluded columns for this table
		if exclusions != nil {
			for _, col := range exclusions.Columns[tableName] {
				delete(wantedColumns, col)
				delete(actualColumns, col)
			}
		}

		// After filtering excluded columns, check if columns actually differ.
		// If the only differences were in excluded columns, skip this table.
		addedColumns := diffKeys(actualColumns, wantedColumns)
		removedColumns := diffKeys(wantedColumns, actualColumns)
		modifiedColumns := diffValues(wantedColumns, actualColumns, func(a, b string) bool { return a == b })
		if len(addedColumns) == 0 && len(removedColumns) == 0 && len(modifiedColumns) == 0 {
			continue
		}

		if len(removedColumns) > 0 && !allowDeletions {
			return nil, fmt.Errorf("will not remove columns from table %s: %v", tableName, removedColumns)
		}

		// TODO make the replace a bit safer (this would replace columns and everything)
		tmpName := tableName + suffix
		sql := strings.ReplaceAll(wantedTables[tableName], tableName, tmpName)
		addOperation(sql)

		commonColumns := intersectKeys(wantedColumns, actualColumns)
		var commonColumnsString string
		for i, c := range commonColumns {
			if i != 0 {
				commonColumnsString += ", "
			}
			commonColumnsString += ident(c)
		}
		addOperation(fmt.Sprintf(`INSERT INTO "%s" (%s) SELECT %s FROM "%s"`, tmpName, commonColumnsString, commonColumnsString, tableName))

		// remove original table and rename new table
		addOperation(fmt.Sprintf(`DROP TABLE "%s"`, tableName))
		addOperation(fmt.Sprintf(`ALTER TABLE "%s" RENAME TO "%s"`, tmpName, tableName))
	}

	// modified can just be recreated
	for _, name := range modifiedIndices {
		removedIndices = append(removedIndices, name)
		addedIndices = append(addedIndices, name)
	}
	for _, name := range modifiedViews {
		removedViews = append(removedViews, name)
		addedViews = append(addedViews, name)
	}
	for _, name := range modifiedTriggers {
		removedTriggers = append(removedTriggers, name)
		addedTriggers = append(addedTriggers, name)
	}

	for _, name := range removedIndices {
		addOperation(fmt.Sprintf(`DROP INDEX "%s"`, name))
	}
	for _, name := range removedViews {
		addOperation(fmt.Sprintf(`DROP VIEW "%s"`, name))
	}
	for _, name := range removedTriggers {
		addOperation(fmt.Sprintf(`DROP TRIGGER "%s"`, name))
	}

	for _, name := range addedIndices {
		addOperation(wantedIndices[name])
	}
	for _, name := range addedViews {
		addOperation(wantedViews[name])
	}
	for _, name := range addedTriggers {
		addOperation(wantedTriggers[name])
	}

	// Create added/modified virtual tables after regular tables exist,
	// since virtual tables may reference them (e.g. FTS5 content tables).
	for _, name := range addedVirtualTables {
		addOperation(wantedVirtualTables[name])
	}
	for _, name := range modifiedVirtualTables {
		addOperation(wantedVirtualTables[name])
	}

	return ops, nil
}

// filterShadowTables removes shadow tables (created internally by virtual tables
// like FTS5) from a table map. Shadow tables have names prefixed with the virtual
// table name followed by an underscore.
func filterShadowTables(tables map[string]string, virtualTableSets ...map[string]string) map[string]string {
	filtered := make(map[string]string, len(tables))
	for name, sql := range tables {
		if !isShadowTable(name, virtualTableSets...) {
			filtered[name] = sql
		}
	}
	return filtered
}

func isShadowTable(name string, virtualTableSets ...map[string]string) bool {
	for _, vts := range virtualTableSets {
		for vtName := range vts {
			if strings.HasPrefix(name, vtName+"_") {
				return true
			}
		}
	}
	return false
}

func ident(s string) string {
	return fmt.Sprintf(`"%s"`, s)
}

func diffKeys[K comparable, V any](a, b map[K]V) (d []K) {
	for k := range b {
		if _, ok := a[k]; !ok {
			d = append(d, k)
		}
	}
	return d
}

func intersectKeys[K comparable, V any](a, b map[K]V) (d []K) {
	for k := range b {
		if _, ok := a[k]; ok {
			d = append(d, k)
		}
	}
	return d
}

func diffValues[K comparable, V any](a, b map[K]V, comparator func(a, b V) bool) []K {
	var c []K
	for k, av := range a {
		if bv, ok := b[k]; ok && !comparator(av, bv) {
			c = append(c, k)
		}
	}
	return c
}

func mapKeyValue(ctx context.Context, db *sql.DB, query string, args ...any) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	kv := map[string]string{}
	for rows.Next() {
		var key, value sql.NullString
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		kv[key.String] = value.String
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return kv, nil
}

var reRemoveComments = regexp.MustCompile(`--.*?\n`)
var reNormalizeWhitespace = regexp.MustCompile(`\s+`)
var reNormalizeParens = regexp.MustCompile(` *([(),]) *`)
var reRemoveQuotes = regexp.MustCompile(`"(\w+)"`)

func normaliseSql(sql string) string {
	sql = reRemoveComments.ReplaceAllString(sql, "")
	sql = reNormalizeWhitespace.ReplaceAllString(sql, " ")
	sql = reNormalizeParens.ReplaceAllString(sql, "$1")
	sql = reRemoveQuotes.ReplaceAllString(sql, "$1")
	return strings.TrimSpace(sql)
}
