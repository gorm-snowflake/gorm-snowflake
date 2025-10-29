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
				// Pre-allocate map with exact capacity
				columnsMap := make(map[string]bool, len(values.Columns))
				for _, column := range values.Columns {
					columnsMap[column.Name] = true
				}

				// Early exit on first missing field
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
			db.Statement.AddClauseIfNotExists(clause.Insert{})
			db.Statement.Build("INSERT")
			db.Statement.WriteByte(' ')
			db.Statement.AddClause(values)

			if values, ok := db.Statement.Clauses["VALUES"].Expression.(clause.Values); ok {
				columnCount := len(values.Columns)
				if columnCount > 0 {
					// Determine insertion method based on configuration
					useUnionSelect := shouldUseUnionSelect(db)

					if useUnionSelect {
						buildUnionSelectInsert(db, values)
					} else {
						buildValuesInsert(db, values)
					}
				} else {
					// only one autoincrement column
					db.Statement.WriteString("VALUES (DEFAULT);")
				}
			}
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
			fieldCount := len(sch.FieldsWithDefaultDBValue)
			fields := make([]*schema.Field, fieldCount)
			values := make([]interface{}, fieldCount)

			db.Statement.SQL.Reset()

			// Pre-allocate query builder capacity
			estimatedQuerySize := 7 + (fieldCount * 25) + len(sch.Table) + 80
			db.Statement.SQL.Grow(estimatedQuerySize)

			// write select
			db.Statement.WriteString("SELECT ")
			// populate fields
			for idx, field := range sch.FieldsWithDefaultDBValue {
				if idx > 0 {
					db.Statement.WriteByte(',')
				}

				fields[idx] = field
				db.Statement.WriteQuoted(field.DBName)
			}
			db.Statement.WriteString(" FROM ")
			db.Statement.WriteQuoted(sch.Table)
			db.Statement.WriteString(" CHANGES(INFORMATION => APPEND_ONLY) BEFORE(statement=>LAST_QUERY_ID());")

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
				reflectIndex := 0
				maxLen := reflectValue.Len()

				// the strategy here is to match the returned rows with INSERT only values
				for rows.Next() && reflectIndex < maxLen {
					// Find next valid struct for insertion
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
							if reflectIndex >= maxLen {
								return
							}
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
				for idx, field := range fields {
					values[idx] = field.ReflectValueOf(db.Statement.Context, reflectValue).Addr().Interface()
				}

				if rows.Next() {
					if err := rows.Scan(values...); err != nil {
						db.AddError(err)
					}
				}
			}
		}
	}
}

func MergeCreate(db *gorm.DB, onConflict clause.OnConflict, values clause.Values) {
	// Transform any column references in DoUpdates to EXCLUDED.column format upfront
	// This prevents GORM from incorrectly quoting "excluded" as a table reference
	onConflict = prepareOnConflictForMerge(db, onConflict)

	valueCount := len(values.Values)
	columnCount := len(values.Columns)
	primaryFieldCount := len(db.Statement.Schema.PrimaryFields)

	// Pre-allocate statement capacity for better performance
	estimatedSize := 100 + len(db.Statement.Table)*2 +
		(valueCount * columnCount * 3) + // VALUES content
		(columnCount * 25) + // column names
		(primaryFieldCount * 50) // WHERE conditions
	db.Statement.SQL.Grow(estimatedSize)

	db.Statement.WriteString("MERGE INTO ")
	db.Statement.WriteQuoted(db.Statement.Table)
	db.Statement.WriteString(" USING (VALUES")

	for idx, value := range values.Values {
		if idx > 0 {
			db.Statement.WriteByte(',')
		}

		db.Statement.WriteByte('(')
		db.Statement.AddVar(db.Statement, value...)
		db.Statement.WriteByte(')')
	}

	db.Statement.WriteString(") AS EXCLUDED (")
	for idx, column := range values.Columns {
		if idx > 0 {
			db.Statement.WriteByte(',')
		}
		db.Statement.WriteQuoted(column.Name)
	}
	db.Statement.WriteString(") ON ")

	// Build ON clause with proper quoting based on QuoteFields setting
	for i, field := range db.Statement.Schema.PrimaryFields {
		if i > 0 {
			db.Statement.WriteString(" AND ")
		}
		db.Statement.WriteQuoted(db.Statement.Table)
		db.Statement.WriteByte('.')
		db.Statement.WriteQuoted(field.DBName)
		db.Statement.WriteString(" = EXCLUDED.")
		db.Statement.WriteQuoted(field.DBName)
	}

	if len(onConflict.DoUpdates) > 0 {
		db.Statement.WriteString(" WHEN MATCHED THEN UPDATE SET ")
		onConflict.DoUpdates.Build(db.Statement)
	}

	db.Statement.WriteString(" WHEN NOT MATCHED THEN INSERT (")

	// Cache auto-increment field check
	autoIncrementField := db.Statement.Schema.PrioritizedPrimaryField
	written := false
	for _, column := range values.Columns {
		if autoIncrementField == nil || !autoIncrementField.AutoIncrement || autoIncrementField.DBName != column.Name {
			if written {
				db.Statement.WriteByte(',')
			}
			written = true
			db.Statement.WriteQuoted(column.Name)
		}
	}

	db.Statement.WriteString(") VALUES (")

	written = false
	for _, column := range values.Columns {
		if autoIncrementField == nil || !autoIncrementField.AutoIncrement || autoIncrementField.DBName != column.Name {
			if written {
				db.Statement.WriteByte(',')
			}
			written = true
			// Write EXCLUDED.<column> - use QuoteTo to handle quoting consistently
			db.Statement.WriteString("EXCLUDED.")
			db.Statement.WriteQuoted(column.Name)
		}
	}

	db.Statement.WriteString(")")
	db.Statement.WriteString(";")
}

// prepareOnConflictForMerge prepares the OnConflict clause for use in MERGE statements
// It converts column references to raw SQL expressions to prevent incorrect quoting
// GORM doesn't support unquoted table-qualified columns, so we use clause.Expr
func prepareOnConflictForMerge(db *gorm.DB, onConflict clause.OnConflict) clause.OnConflict {
	if len(onConflict.DoUpdates) == 0 {
		return onConflict
	}

	// Check if we should quote fields
	shouldQuote := false
	if dialector, ok := db.Dialector.(*Dialector); ok && dialector.Config != nil {
		shouldQuote = dialector.Config.QuoteFields
	}

	// Create a new Set with converted assignments
	transformed := make(clause.Set, len(onConflict.DoUpdates))

	for i, assignment := range onConflict.DoUpdates {
		transformed[i] = assignment

		// Convert clause.Column references to EXCLUDED.column format
		// We use clause.Expr because GORM's QuoteTo is called separately for
		// table and column parts, making it impossible to keep both unquoted
		if col, ok := assignment.Value.(clause.Column); ok {
			colName := col.Name

			// Check if user already provided "excluded.column" (case-insensitive)
			colNameLower := strings.ToLower(colName)
			if strings.HasPrefix(colNameLower, "excluded.") {
				// User provided excluded.column - transform to proper case
				// Extract the column name after "excluded."
				columnPart := colName[len("excluded."):]

				if shouldQuote {
					transformed[i].Value = clause.Expr{
						SQL: fmt.Sprintf(`EXCLUDED."%s"`, columnPart),
					}
				} else {
					transformed[i].Value = clause.Expr{
						SQL: fmt.Sprintf(`EXCLUDED.%s`, columnPart),
					}
				}
				continue
			}

			// Normal case: simple column name, wrap with EXCLUDED prefix
			if shouldQuote {
				transformed[i].Value = clause.Expr{
					SQL: fmt.Sprintf(`EXCLUDED."%s"`, colName),
				}
			} else {
				transformed[i].Value = clause.Expr{
					SQL: fmt.Sprintf(`EXCLUDED.%s`, colName),
				}
			}
		}
	}

	// Return a new OnConflict with the converted DoUpdates
	onConflict.DoUpdates = transformed
	return onConflict
}

// shouldUseUnionSelect determines whether to use UNION SELECT or VALUES syntax
func shouldUseUnionSelect(db *gorm.DB) bool {
	// Try to get the config from the dialector
	if d, ok := db.Dialector.(*Dialector); ok && d.Config != nil {
		// If explicitly set to false, use VALUES syntax
		// If not set or true, use UNION SELECT (maintains backward compatibility)
		return d.Config.UseUnionSelect
	}
	// Default to UNION SELECT for backward compatibility
	return true
}

// buildUnionSelectInsert builds INSERT statement using UNION SELECT syntax
// This supports SQL functions in values but is slower than VALUES syntax
func buildUnionSelectInsert(db *gorm.DB, values clause.Values) {
	columnCount := len(values.Columns)
	valueCount := len(values.Values)

	// Pre-allocate variables slice with exact capacity for better performance
	totalVars := valueCount * columnCount
	if cap(db.Statement.Vars) < len(db.Statement.Vars)+totalVars {
		// Grow the vars slice if needed
		newVars := make([]interface{}, len(db.Statement.Vars), len(db.Statement.Vars)+totalVars)
		copy(newVars, db.Statement.Vars)
		db.Statement.Vars = newVars
	}

	// Pre-allocate statement builder capacity for better performance
	estimatedSize := (columnCount * 20) + // column names with quotes
		(valueCount * 15) + // " UNION SELECT " strings
		(valueCount * columnCount * 2) + // placeholders
		50 // base structure
	db.Statement.SQL.Grow(estimatedSize)

	db.Statement.WriteByte('(')
	for idx, column := range values.Columns {
		if idx > 0 {
			db.Statement.WriteByte(',')
		}
		db.Statement.WriteQuoted(column)
	}

	db.Statement.WriteString(") SELECT ")

	// Cache the union string to avoid repeated allocations
	const unionSelect = " UNION SELECT "
	for idx, value := range values.Values {
		if idx > 0 {
			db.Statement.WriteString(unionSelect)
		}

		valueLen := len(value)
		for i := 0; i < valueLen; i++ {
			if i > 0 {
				db.Statement.WriteByte(',')
			}
			db.Statement.AddVar(db.Statement, value[i])
		}
	}

	db.Statement.WriteString(";")
}

// buildValuesInsert builds INSERT statement using traditional VALUES syntax
// This is faster than UNION SELECT but doesn't support SQL functions in values
func buildValuesInsert(db *gorm.DB, values clause.Values) {
	columnCount := len(values.Columns)
	valueCount := len(values.Values)

	// Pre-allocate variables slice with exact capacity for better performance
	totalVars := valueCount * columnCount
	if cap(db.Statement.Vars) < len(db.Statement.Vars)+totalVars {
		// Grow the vars slice if needed
		newVars := make([]interface{}, len(db.Statement.Vars), len(db.Statement.Vars)+totalVars)
		copy(newVars, db.Statement.Vars)
		db.Statement.Vars = newVars
	}

	// Pre-allocate statement builder capacity for better performance
	estimatedSize := (columnCount * 15) + // column names with quotes
		(valueCount * 10) + // "(),(),()," patterns
		(valueCount * columnCount * 2) + // placeholders
		50 // base structure
	db.Statement.SQL.Grow(estimatedSize)

	db.Statement.WriteByte('(')
	for idx, column := range values.Columns {
		if idx > 0 {
			db.Statement.WriteByte(',')
		}
		db.Statement.WriteQuoted(column)
	}
	db.Statement.WriteByte(')')

	db.Statement.WriteString(" VALUES ")

	for idx, value := range values.Values {
		if idx > 0 {
			db.Statement.WriteByte(',')
		}

		db.Statement.WriteByte('(')
		db.Statement.AddVar(db.Statement, value...)
		db.Statement.WriteByte(')')
	}

	db.Statement.WriteString(";")
}
