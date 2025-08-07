package snowflake

import (
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

func Create(db *gorm.DB) {
	if db.Statement.Schema != nil && !db.Statement.Unscoped {
		for _, c := range db.Statement.Schema.CreateClauses {
			db.Statement.AddClause(c)
		}
	}

	if db.Statement.SQL.String() == "" {
		var (
			values                  = callbacks.ConvertToCreateValues(db.Statement)
			c                       = db.Statement.Clauses["ON CONFLICT"]
			onConflict, hasConflict = c.Expression.(clause.OnConflict)
		)

		if hasConflict {
			if len(db.Statement.Schema.PrimaryFields) > 0 {
				columnsMap := make(map[string]bool, len(values.Columns))
				for _, column := range values.Columns {
					columnsMap[column.Name] = true
				}

				for _, field := range db.Statement.Schema.PrimaryFields {
					if !columnsMap[field.DBName] {
						hasConflict = false
						break
					}
				}
			} else {
				hasConflict = false
			}
		}

		if hasConflict {
			MergeCreate(db, onConflict, values)
		} else {
			// Use optimized insert generation
			buildOptimizedInsert(db, values)
		}
	}

	if !db.DryRun && db.Error == nil {
		db.RowsAffected = 0

		// exec the merge/insert first
		if result, err := db.Statement.ConnPool.ExecContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...); err == nil {
			db.RowsAffected, _ = result.RowsAffected()
		} else {
			_ = db.AddError(err)
		}

		db.Logger.Info(db.Statement.Context, fmt.Sprintf("This is the result of insert %s, values %v, rows affected %d", db.Statement.SQL.String(), db.Statement.Vars, db.RowsAffected))

		// do another select on last inserted values to populate default values (e.g. ID)
		// this relies on the result of SELECT * FROM CHANGES to align with the order of the VALUES in MERGE statement
		if sch := db.Statement.Schema; sch != nil && len(sch.FieldsWithDefaultDBValue) > 0 {
			populateDefaultValues(db, sch)
		}
	}
}

// buildOptimizedInsert creates an optimized INSERT statement with UNION SELECT for Snowflake
func buildOptimizedInsert(db *gorm.DB, values clause.Values) {
	db.Statement.AddClauseIfNotExists(clause.Insert{})
	db.Statement.Build("INSERT")
	db.Statement.WriteByte(' ')
	db.Statement.AddClause(values)

	if values, ok := db.Statement.Clauses["VALUES"].Expression.(clause.Values); ok {
		columnCount := len(values.Columns)

		if columnCount == 0 {
			db.Statement.WriteString("VALUES (DEFAULT);")
			return
		}

		if len(values.Values) == 1 {
			buildSingleRowInsert(db, values)
		} else {
			buildInsert(db, values)
		}
	}
}

// buildSingleRowInsert optimizes the common case of inserting a single row
func buildSingleRowInsert(db *gorm.DB, values clause.Values) {
	columnCount := len(values.Columns)
	estimatedSize := (columnCount * 15) + 50 // columns + basic structure

	var builder strings.Builder
	builder.Grow(estimatedSize)

	builder.WriteByte('(')
	for idx, column := range values.Columns {
		if idx > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(db.Statement.Quote(column.Name))
	}
	builder.WriteString(") SELECT ")

	// Single row of values
	row := values.Values[0]
	for i, v := range row {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteByte('?')
		db.Statement.AddVar(db.Statement, v)
	}

	builder.WriteByte(';')
	db.Statement.WriteString(builder.String())
}

// buildInsert builds a single INSERT statement with UNION SELECT
func buildInsert(db *gorm.DB, values clause.Values) {
	// Pre-allocate string builder with more accurate capacity estimation
	columnCount := len(values.Columns)
	valueCount := len(values.Values)

	avgColumnNameLen := 10
	estimatedSize := (columnCount * (avgColumnNameLen + 3)) + // quoted column names
		(valueCount * 13) + // " UNION SELECT " per row
		(valueCount * columnCount * 2) + // "?," per value
		50 // base structure

	var builder strings.Builder
	builder.Grow(estimatedSize)

	// Build column list once - avoid repeated Quote() calls by caching
	builder.WriteByte('(')
	for idx, column := range values.Columns {
		if idx > 0 {
			builder.WriteByte(',')
		}
		// Cache quoted column name to avoid repeated quoting
		builder.WriteString(db.Statement.Quote(column.Name))
	}
	builder.WriteString(") SELECT ")

	// Optimize UNION SELECT generation - minimize string operations
	const unionSelect = " UNION SELECT "
	for idx, value := range values.Values {
		if idx > 0 {
			builder.WriteString(unionSelect)
		}
		// Pre-calculate the exact number of placeholders needed
		valueLen := len(value)
		for i := 0; i < valueLen; i++ {
			if i > 0 {
				builder.WriteByte(',')
			}
			builder.WriteByte('?')
			db.Statement.AddVar(db.Statement, value[i])
		}
	}

	builder.WriteByte(';')
	db.Statement.WriteString(builder.String())
}

// populateDefaultValues efficiently populates default values using CHANGES table
func populateDefaultValues(db *gorm.DB, sch *schema.Schema) {
	fieldsWithDefaults := sch.FieldsWithDefaultDBValue
	fieldCount := len(fieldsWithDefaults)

	// Early return if no fields to populate
	if fieldCount == 0 {
		return
	}

	// Pre-allocate slices with exact capacity
	fields := make([]*schema.Field, fieldCount)
	values := make([]interface{}, fieldCount)

	db.Statement.SQL.Reset()

	// Build SELECT query using string builder with precise capacity
	// SELECT + field names + FROM + table + CHANGES clause
	estimatedQuerySize := 7 + (fieldCount * 20) + 6 + 30 + 80
	var builder strings.Builder
	builder.Grow(estimatedQuerySize)

	builder.WriteString("SELECT ")

	// Cache quoted field names to avoid repeated Quote() calls
	quotedTable := db.Statement.Quote(sch.Table)
	for idx, field := range fieldsWithDefaults {
		if idx > 0 {
			builder.WriteByte(',')
		}
		fields[idx] = field
		builder.WriteString(db.Statement.Quote(field.DBName))
	}

	builder.WriteString(" FROM ")
	builder.WriteString(quotedTable)
	builder.WriteString(" CHANGES(INFORMATION => APPEND_ONLY) BEFORE(statement=>LAST_QUERY_ID());")

	db.Statement.WriteString(builder.String())

	rows, err := db.Statement.ConnPool.QueryContext(db.Statement.Context, db.Statement.SQL.String(), db.Statement.Vars...)
	if err != nil {
		db.AddError(err)
		return
	}
	defer rows.Close()

	reflectValue := db.Statement.ReflectValue
	reflectKind := reflectValue.Kind()

	switch reflectKind {
	case reflect.Slice, reflect.Array:
		// Optimized slice/array processing with bounds checking
		maxLen := reflectValue.Len()
		reflectIndex := 0

		for rows.Next() && reflectIndex < maxLen {
			// Find the next valid struct for insertion
			for reflectIndex < maxLen {
				currentValue := reflectValue.Index(reflectIndex)
				if reflect.Indirect(currentValue).Kind() != reflect.Struct {
					break
				}

				// Check if this row has zero defaults (indicates INSERT operation)
				hasNonZeroDefaults := false
				for _, field := range fields {
					fieldValue := field.ReflectValueOf(db.Statement.Context, currentValue)
					if !fieldValue.IsZero() {
						hasNonZeroDefaults = true
						break
					}
				}

				if hasNonZeroDefaults {
					// Skip this row, move to next record
					reflectIndex++
					continue
				}

				// Found a valid INSERT row - populate interface slice for scanning
				for idx, field := range fields {
					fieldValue := field.ReflectValueOf(db.Statement.Context, currentValue)
					values[idx] = fieldValue.Addr().Interface()
				}

				if err := rows.Scan(values...); err != nil {
					db.AddError(err)
				}
				reflectIndex++
				break
			}
		}

	case reflect.Struct:
		// Single struct case - most efficient path
		for idx, field := range fields {
			fieldValue := field.ReflectValueOf(db.Statement.Context, reflectValue)
			values[idx] = fieldValue.Addr().Interface()
		}

		if rows.Next() {
			if err := rows.Scan(values...); err != nil {
				db.AddError(err)
			}
		}
	}
}

func MergeCreate(db *gorm.DB, onConflict clause.OnConflict, values clause.Values) {
	buildSingleMerge(db, onConflict, values)
}

// buildSingleMerge creates a single MERGE statement
func buildSingleMerge(db *gorm.DB, onConflict clause.OnConflict, values clause.Values) {
	// More accurate capacity estimation for MERGE statements
	columnCount := len(values.Columns)
	valueCount := len(values.Values)
	primaryFieldCount := len(db.Statement.Schema.PrimaryFields)

	estimatedSize := 100 + 30 +
		(columnCount * 60) + // column names used multiple times
		(valueCount * columnCount * 2) + // placeholders
		(primaryFieldCount * 50) // WHERE conditions

	var builder strings.Builder
	builder.Grow(estimatedSize)

	tableName := db.Statement.Quote(db.Statement.Table)
	builder.WriteString("MERGE INTO ")
	builder.WriteString(tableName)
	builder.WriteString(" USING (VALUES")

	// Optimize VALUES generation
	for idx, value := range values.Values {
		if idx > 0 {
			builder.WriteByte(',')
		}
		builder.WriteByte('(')
		valueLen := len(value)
		for i := 0; i < valueLen; i++ {
			if i > 0 {
				builder.WriteByte(',')
			}
			builder.WriteByte('?')
			db.Statement.AddVar(db.Statement, value[i])
		}
		builder.WriteByte(')')
	}

	builder.WriteString(") AS EXCLUDED (")
	// Cache column names to avoid repeated processing
	for idx, column := range values.Columns {
		if idx > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(column.Name)
	}
	builder.WriteString(") ON ")

	// Pre-allocate and build WHERE conditions efficiently
	if primaryFieldCount > 0 {
		where := make([]string, primaryFieldCount)
		for i, field := range db.Statement.Schema.PrimaryFields {
			// Use string concatenation instead of fmt.Sprintf for better performance
			where[i] = `"` + db.Statement.Table + `"."` + field.DBName + `" = EXCLUDED.` + field.DBName
		}
		builder.WriteString(strings.Join(where, " AND "))
	}

	if len(onConflict.DoUpdates) > 0 {
		builder.WriteString(" WHEN MATCHED THEN UPDATE SET ")
		// Flush builder before letting DoUpdates write directly to statement
		db.Statement.WriteString(builder.String())
		builder.Reset()
		onConflict.DoUpdates.Build(db.Statement)
	}

	builder.WriteString(" WHEN NOT MATCHED THEN INSERT (")

	// Optimize column filtering for INSERT clause
	var insertColumns []string
	autoIncrementField := db.Statement.Schema.PrioritizedPrimaryField
	for _, column := range values.Columns {
		// Skip auto-increment columns
		if autoIncrementField == nil || !autoIncrementField.AutoIncrement || autoIncrementField.DBName != column.Name {
			insertColumns = append(insertColumns, db.Statement.Quote(column.Name))
		}
	}
	builder.WriteString(strings.Join(insertColumns, ","))

	builder.WriteString(") VALUES (")

	// Build VALUES clause for excluded columns
	var excludedValues []string
	for _, column := range values.Columns {
		if autoIncrementField == nil || !autoIncrementField.AutoIncrement || autoIncrementField.DBName != column.Name {
			excludedValues = append(excludedValues, "EXCLUDED."+column.Name)
		}
	}
	builder.WriteString(strings.Join(excludedValues, ","))

	builder.WriteString(");")
	db.Statement.WriteString(builder.String())
}
