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
				columnsMap := map[string]bool{}
				for _, column := range values.Columns {
					columnsMap[column.Name] = true
				}

				for _, field := range db.Statement.Schema.PrimaryFields {
					if _, ok := columnsMap[field.DBName]; !ok {
						hasConflict = false
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
				if len(values.Columns) > 0 {
					db.Statement.WriteByte('(')
					for idx, column := range values.Columns {
						if idx > 0 {
							db.Statement.WriteByte(',')
						}
						db.Statement.WriteQuoted(column)
					}

					db.Statement.WriteString(") SELECT ")

					for idx, value := range values.Values {
						if idx > 0 {
							db.Statement.WriteString(", UNION SELECT ")
						}

						for i, v := range value {
							if i > 0 {
								db.Statement.WriteByte(',')
							}
							db.Statement.AddVar(db.Statement, v)
						}
					}

					db.Statement.WriteString(";")
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
		if sch := db.Statement.Schema; sch != nil && len(db.Statement.Schema.FieldsWithDefaultDBValue) > 0 {
			var (
				fields = make([]*schema.Field, len(sch.FieldsWithDefaultDBValue))
				values = make([]interface{}, len(sch.FieldsWithDefaultDBValue))
			)

			db.Statement.SQL.Reset()

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
			reflectIndex := 0
			if err == nil {
				defer rows.Close()

				switch db.Statement.ReflectValue.Kind() {
				case reflect.Slice, reflect.Array:
					// the strategy here is to match the returned rows with INSERT only values
					for rows.Next() {
					BEGIN:
						reflectValue := db.Statement.ReflectValue.Index(reflectIndex)
						if reflect.Indirect(reflectValue).Kind() != reflect.Struct {
							break
						}

						for idx, field := range fields {
							fieldValue := field.ReflectValueOf(db.Statement.Context, reflectValue)

							// skip where default are zeros (non-insert in MERGE)
							if !fieldValue.IsZero() {
								reflectIndex++

								if reflectIndex >= db.Statement.ReflectValue.Len() {
									return
								}

								goto BEGIN
							}
							values[idx] = fieldValue.Addr().Interface()
						}

						if err := rows.Scan(values...); err != nil {
							_ = db.AddError(err)
						}
					}
				case reflect.Struct:
					for idx, field := range fields {
						values[idx] = field.ReflectValueOf(db.Statement.Context, db.Statement.ReflectValue).Addr().Interface()
					}

					if rows.Next() {
						db.AddError(rows.Scan(values...))
					}
				}
			} else {
				db.AddError(err)
			}
		}
	}
}

func MergeCreate(db *gorm.DB, onConflict clause.OnConflict, values clause.Values) {
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
		db.Statement.WriteString(column.Name)
	}
	db.Statement.WriteString(") ON ")

	var where []string
	for _, field := range db.Statement.Schema.PrimaryFields {
		where = append(where, fmt.Sprintf(`"%s"."%s" = EXCLUDED.%s`, db.Statement.Table, field.DBName, field.DBName))
	}

	db.Statement.WriteString(strings.Join(where, " AND "))

	if len(onConflict.DoUpdates) > 0 {
		db.Statement.WriteString(" WHEN MATCHED THEN UPDATE SET ")
		onConflict.DoUpdates.Build(db.Statement)
	}

	db.Statement.WriteString(" WHEN NOT MATCHED THEN INSERT (")

	written := false
	for _, column := range values.Columns {
		if db.Statement.Schema.PrioritizedPrimaryField == nil || !db.Statement.Schema.PrioritizedPrimaryField.AutoIncrement || db.Statement.Schema.PrioritizedPrimaryField.DBName != column.Name {
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
		if db.Statement.Schema.PrioritizedPrimaryField == nil || !db.Statement.Schema.PrioritizedPrimaryField.AutoIncrement || db.Statement.Schema.PrioritizedPrimaryField.DBName != column.Name {
			if written {
				db.Statement.WriteByte(',')
			}
			written = true
			db.Statement.WriteString(fmt.Sprintf("EXCLUDED.%s", column.Name))
		}
	}

	db.Statement.WriteString(")")
	db.Statement.WriteString(";")
}
