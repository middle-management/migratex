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

type Operation string

func (op Operation) Normalized() string {
	return normaliseSql(string(op))
}

func Plan(ctx context.Context, actual *sql.DB, schema string, allowDeletions bool) ([]Operation, error) {
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

	wantedTables, err := listTables(ctx, wanted)
	if err != nil {
		return nil, err
	}
	wantedIndices, err := listIndices(ctx, wanted)
	if err != nil {
		return nil, err
	}

	actualTables, err := listTables(ctx, actual)
	if err != nil {
		return nil, err
	}
	actualIndices, err := listIndices(ctx, actual)
	if err != nil {
		return nil, err
	}

	eqSql := func(a, b string) bool {
		return normaliseSql(a) == normaliseSql(b)
	}

	// TODO support migration of views and triggers too
	addedTables := diffKeys(actualTables, wantedTables)
	removedTables := diffKeys(wantedTables, actualTables)
	modifiedTables := diffValues(wantedTables, actualTables, eqSql)
	addedIndices := diffKeys(actualIndices, wantedIndices)
	removedIndices := diffKeys(wantedIndices, actualIndices)
	modifiedIndices := diffValues(wantedIndices, actualIndices, eqSql)

	if len(removedTables) > 0 && !allowDeletions {
		return nil, fmt.Errorf("will not remove tables: %v", removedTables)
	}

	for _, name := range addedTables {
		addOperation(wantedTables[name])
	}

	for _, name := range removedTables {
		addOperation(fmt.Sprintf(`DROP TABLE "%s"`, name))
	}

	suffix := "_migratex_" + strconv.Itoa(rand.Int())
	for _, tableName := range modifiedTables {
		// TODO make the replace a bit safer (this would replace columns and everything)
		tmpName := tableName + suffix
		sql := strings.ReplaceAll(wantedTables[tableName], tableName, tmpName)
		addOperation(sql)

		wantedColumns, err := listColumns(ctx, wanted, tableName)
		if err != nil {
			return nil, err
		}

		actualColumns, err := listColumns(ctx, actual, tableName)
		if err != nil {
			return nil, err
		}

		removedColumns := diffKeys(wantedColumns, actualColumns)
		if len(removedColumns) > 0 && !allowDeletions {
			return nil, fmt.Errorf("will not remove columns from table %s: %v", tableName, removedColumns)
		}

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

	for _, name := range removedIndices {
		addOperation(fmt.Sprintf(`DROP INDEX "%s"`, name))
	}

	for _, name := range addedIndices {
		addOperation(wantedIndices[name])
	}

	return ops, nil
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

func listTables(ctx context.Context, db *sql.DB) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT name, sql
		FROM sqlite_master
		WHERE type = "table"
		AND name != "sqlite_sequence"
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := map[string]string{}
	for rows.Next() {
		var name, schemaString string
		if err := rows.Scan(&name, &schemaString); err != nil {
			return nil, err
		}
		tables[name] = schemaString
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return tables, nil
}

type Column struct {
	ID         int64
	Name       string
	Type       string
	NotNull    bool
	Default    []byte
	PrimaryKey int
}

func listColumns(ctx context.Context, db *sql.DB, tableName string) (map[string]Column, error) {
	// TODO schema name might be required in pragma_table_info()
	rows, err := db.QueryContext(ctx, `
		SELECT
            cid,
            name,
            type,
            "notnull",
            dflt_value,
            pk
        FROM
            pragma_table_info($1)
        ORDER BY
            cid
    `, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := map[string]Column{}
	for rows.Next() {
		var col Column
		if err := rows.Scan(
			&col.ID,
			&col.Name,
			&col.Type,
			&col.NotNull,
			&col.Default,
			&col.PrimaryKey,
		); err != nil {
			return nil, err
		}
		columns[col.Name] = col
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return columns, nil
}

func listIndices(ctx context.Context, db *sql.DB) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT name, sql FROM sqlite_master WHERE type = "index"`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := map[string]string{}
	for rows.Next() {
		var name, sql string
		if err := rows.Scan(&name, &sql); err != nil {
			return nil, err
		}
		tables[name] = sql
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return tables, nil
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
