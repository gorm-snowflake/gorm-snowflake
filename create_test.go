package snowflake

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// TestCreateEdgeCases tests various edge cases for the Create function
func TestCreateEdgeCases(t *testing.T) {
	t.Run("Create with existing SQL", func(t *testing.T) {
		db := setupMockDB(t)

		// Create a statement with existing SQL
		stmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
		stmt.Statement.SQL.WriteString("EXISTING SQL")

		// Create should not modify existing SQL
		Create(stmt)

		if stmt.Statement.SQL.String() != "EXISTING SQL" {
			t.Error("Create should not modify existing SQL")
		}
	})

	t.Run("Create with Unscoped", func(t *testing.T) {
		db := setupMockDB(t)

		model := TestModel{Name: "John", Age: 25}
		stmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
		if err := stmt.Statement.Parse(&TestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		stmt.Statement.Dest = model
		stmt.Statement.ReflectValue = reflect.ValueOf(model)
		stmt.Statement.Unscoped = true // Set unscoped

		Create(stmt)

		// Should still generate valid SQL
		sql := stmt.Statement.SQL.String()
		if !strings.Contains(sql, "INSERT INTO") {
			t.Errorf("Expected INSERT statement, got: %s", sql)
		}
	})

	t.Run("Create with Schema CreateClauses", func(t *testing.T) {
		db := setupMockDB(t)

		model := TestModel{Name: "John", Age: 25}
		stmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
		if err := stmt.Statement.Parse(&TestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		// Add a create clause to the schema
		stmt.Statement.Schema.CreateClauses = []clause.Interface{
			clause.Insert{},
		}

		stmt.Statement.Dest = model
		stmt.Statement.ReflectValue = reflect.ValueOf(model)

		Create(stmt)

		sql := stmt.Statement.SQL.String()
		if sql == "" {
			t.Error("Expected SQL to be generated")
		}
	})
}

func TestShouldUseUnionSelect(t *testing.T) {
	t.Run("Default behavior - true", func(t *testing.T) {
		db := setupMockDBWithConfig(t, true, true)
		result := shouldUseUnionSelect(db)
		if !result {
			t.Error("Expected shouldUseUnionSelect to return true by default")
		}
	})

	t.Run("Explicitly set to false", func(t *testing.T) {
		db := setupMockDBWithConfig(t, false, true)
		result := shouldUseUnionSelect(db)
		if result {
			t.Error("Expected shouldUseUnionSelect to return false when explicitly set")
		}
	})

	t.Run("Non-Snowflake dialector", func(t *testing.T) {
		// Create a mock DB with a different dialector
		mockDB, _ := gorm.Open(&mockDialector{}, &gorm.Config{})
		result := shouldUseUnionSelect(mockDB)
		if !result {
			t.Error("Expected shouldUseUnionSelect to return true for non-Snowflake dialector")
		}
	})
}

// Mock dialector for testing
type mockDialector struct{}

func (m *mockDialector) Name() string                          { return "mock" }
func (m *mockDialector) Initialize(db *gorm.DB) error          { return nil }
func (m *mockDialector) Migrator(db *gorm.DB) gorm.Migrator    { return nil }
func (m *mockDialector) DataTypeOf(field *schema.Field) string { return "TEXT" }
func (m *mockDialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "NULL"}
}
func (m *mockDialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {}
func (m *mockDialector) QuoteTo(writer clause.Writer, str string)                            {}
func (m *mockDialector) Explain(sql string, vars ...interface{}) string                      { return sql }

func TestBuildUnionSelectInsert(t *testing.T) {
	db := setupMockDBWithConfig(t, true, true)

	// Create test values
	values := clause.Values{
		Columns: []clause.Column{
			{Name: "name"},
			{Name: "age"},
		},
		Values: [][]interface{}{
			{"John", 25},
			{"Jane", 30},
		},
	}

	// Parse the model schema first
	tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
	if err := tempStmt.Statement.Parse(&TestModel{}); err != nil {
		t.Fatalf("Failed to parse model: %v", err)
	}

	// Reset SQL and vars
	tempStmt.Statement.SQL.Reset()
	tempStmt.Statement.Vars = nil

	buildUnionSelectInsert(tempStmt, values)

	sql := tempStmt.Statement.SQL.String()
	expectedSQL := `("name","age") SELECT ?,? UNION SELECT ?,?;`

	if sql != expectedSQL {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expectedSQL, sql)
	}

	// Check variables
	expectedVars := []interface{}{"John", 25, "Jane", 30}
	if len(tempStmt.Statement.Vars) != len(expectedVars) {
		t.Errorf("Expected %d variables, got %d", len(expectedVars), len(tempStmt.Statement.Vars))
	}
}

func TestBuildValuesInsert(t *testing.T) {
	db := setupMockDBWithConfig(t, false, true)

	// Create test values
	values := clause.Values{
		Columns: []clause.Column{
			{Name: "name"},
			{Name: "age"},
		},
		Values: [][]interface{}{
			{"John", 25},
			{"Jane", 30},
		},
	}

	// Parse the model schema first
	tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
	if err := tempStmt.Statement.Parse(&TestModel{}); err != nil {
		t.Fatalf("Failed to parse model: %v", err)
	}

	// Reset SQL and vars
	tempStmt.Statement.SQL.Reset()
	tempStmt.Statement.Vars = nil

	buildValuesInsert(tempStmt, values)

	sql := tempStmt.Statement.SQL.String()
	expectedSQL := `("name","age") VALUES (?,?),(?,?);`

	if sql != expectedSQL {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expectedSQL, sql)
	}

	// Check variables
	expectedVars := []interface{}{"John", 25, "Jane", 30}
	if len(tempStmt.Statement.Vars) != len(expectedVars) {
		t.Errorf("Expected %d variables, got %d", len(expectedVars), len(tempStmt.Statement.Vars))
	}
}

func TestMergeCreateComplex(t *testing.T) {
	t.Run("Merge with DoUpdates", func(t *testing.T) {
		db := setupMockDBWithConfig(t, true, true)

		// Create test data with primary keys
		models := []TestModel{
			{ID: 1, Name: "John", Age: 25},
			{ID: 2, Name: "Jane", Age: 30},
		}

		// Parse the model schema first
		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
		if err := tempStmt.Statement.Parse(&TestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		// Set up values and conflict
		tempStmt.Statement.Dest = models
		tempStmt.Statement.ReflectValue = reflect.ValueOf(models)

		// Add OnConflict clause with updates
		onConflict := clause.OnConflict{
			DoUpdates: clause.Assignments(map[string]interface{}{
				"name": clause.Column{Name: "name"},
				"age":  clause.Column{Name: "age"},
			}),
		}

		values := clause.Values{
			Columns: []clause.Column{
				{Name: "name"},
				{Name: "age"},
				{Name: "id"},
			},
			Values: [][]interface{}{
				{"John", 25, uint(1)},
				{"Jane", 30, uint(2)},
			},
		}

		// Reset SQL
		tempStmt.Statement.SQL.Reset()
		tempStmt.Statement.Vars = nil

		MergeCreate(tempStmt, onConflict, values)

		sql := tempStmt.Statement.SQL.String()

		// Should contain MERGE statement structure
		if !strings.Contains(sql, "MERGE INTO") {
			t.Errorf("Expected MERGE statement, got: %s", sql)
		}
		if !strings.Contains(sql, "WHEN MATCHED THEN UPDATE SET") {
			t.Errorf("Expected UPDATE clause in MERGE, got: %s", sql)
		}
		if !strings.Contains(sql, "WHEN NOT MATCHED THEN INSERT") {
			t.Errorf("Expected INSERT clause in MERGE, got: %s", sql)
		}
	})

	t.Run("Merge without DoUpdates", func(t *testing.T) {
		db := setupMockDBWithConfig(t, true, true)

		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
		if err := tempStmt.Statement.Parse(&TestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		onConflict := clause.OnConflict{} // No DoUpdates

		values := clause.Values{
			Columns: []clause.Column{
				{Name: "name"},
				{Name: "age"},
				{Name: "id"},
			},
			Values: [][]interface{}{
				{"John", 25, uint(1)},
			},
		}

		tempStmt.Statement.SQL.Reset()
		tempStmt.Statement.Vars = nil

		MergeCreate(tempStmt, onConflict, values)

		sql := tempStmt.Statement.SQL.String()

		// Should not contain UPDATE clause
		if strings.Contains(sql, "WHEN MATCHED THEN UPDATE SET") {
			t.Errorf("Expected no UPDATE clause without DoUpdates, got: %s", sql)
		}
		if !strings.Contains(sql, "WHEN NOT MATCHED THEN INSERT") {
			t.Errorf("Expected INSERT clause in MERGE, got: %s", sql)
		}
	})
}

func TestCreateWithFieldsWithDefaultDBValue(t *testing.T) {
	// This test is more complex as it involves the post-execution behavior
	// We'll test the SQL generation part that queries for default values

	t.Run("Schema with default fields", func(t *testing.T) {
		db := setupMockDB(t)

		// Create a model with default fields
		type ModelWithDefaults struct {
			ID        uint      `gorm:"primaryKey;autoIncrement"`
			Name      string    `gorm:"not null"`
			CreatedAt time.Time `gorm:"autoCreateTime"`
			UpdatedAt time.Time `gorm:"autoUpdateTime"`
		}

		model := &ModelWithDefaults{Name: "Test"}

		stmt := db.Session(&gorm.Session{DryRun: true}).Model(&ModelWithDefaults{})
		if err := stmt.Statement.Parse(&ModelWithDefaults{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		stmt.Statement.Dest = model
		stmt.Statement.ReflectValue = reflect.ValueOf(model)

		// The Create function should work without panicking
		Create(stmt)

		// Verify that SQL was generated
		if stmt.Statement.SQL.String() == "" {
			t.Error("Expected SQL to be generated")
		}
	})
}

func TestCreateConflictDetection(t *testing.T) {
	t.Run("Conflict with primary key present", func(t *testing.T) {
		db := setupMockDB(t)

		models := []TestModel{
			{ID: 1, Name: "John", Age: 25},
		}

		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
		if err := tempStmt.Statement.Parse(&TestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		// Add OnConflict clause
		tempStmt.Statement.AddClause(clause.OnConflict{
			DoUpdates: clause.Assignments(map[string]interface{}{
				"age": clause.Column{Name: "age"},
			}),
		})

		tempStmt.Statement.Dest = models
		tempStmt.Statement.ReflectValue = reflect.ValueOf(models)

		Create(tempStmt)

		sql := tempStmt.Statement.SQL.String()

		// Should generate MERGE statement
		if !strings.Contains(sql, "MERGE INTO") {
			t.Errorf("Expected MERGE statement for conflict with primary key, got: %s", sql)
		}
	})

	t.Run("Conflict with primary key missing", func(t *testing.T) {
		db := setupMockDB(t)

		// Create model without ID (primary key missing from values)
		models := []TestModel{
			{Name: "John", Age: 25}, // No ID set
		}

		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
		if err := tempStmt.Statement.Parse(&TestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		// Add OnConflict clause
		tempStmt.Statement.AddClause(clause.OnConflict{
			DoUpdates: clause.Assignments(map[string]interface{}{
				"age": clause.Column{Name: "age"},
			}),
		})

		tempStmt.Statement.Dest = models
		tempStmt.Statement.ReflectValue = reflect.ValueOf(models)

		Create(tempStmt)

		sql := tempStmt.Statement.SQL.String()

		// Should generate regular INSERT statement, not MERGE
		if strings.Contains(sql, "MERGE INTO") {
			t.Errorf("Expected INSERT statement when primary key missing, got: %s", sql)
		}
		if !strings.Contains(sql, "INSERT INTO") {
			t.Errorf("Expected INSERT statement, got: %s", sql)
		}
	})
}

func TestVariablePreallocation(t *testing.T) {
	t.Run("Variables slice growth", func(t *testing.T) {
		db := setupMockDB(t)

		// Create many models to test variable preallocation
		var models []TestModel
		for i := 0; i < 100; i++ {
			models = append(models, TestModel{
				Name: fmt.Sprintf("User%d", i),
				Age:  20 + i,
			})
		}

		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
		if err := tempStmt.Statement.Parse(&TestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		tempStmt.Statement.Dest = models
		tempStmt.Statement.ReflectValue = reflect.ValueOf(models)

		Create(tempStmt)

		// Should have variables for all models
		expectedVarCount := len(models) * 2 // name and age for each model
		if len(tempStmt.Statement.Vars) != expectedVarCount {
			t.Errorf("Expected %d variables, got %d", expectedVarCount, len(tempStmt.Statement.Vars))
		}
	})
}

// TestGORMSaveExcludedQuotingBug specifically tests the bug where GORM's Save method
// auto-quotes "excluded" when building DoUpdates clauses. According to GORM docs:
// https://gorm.io/docs/update.html#Save-All-Fields
// Save will automatically handle upserts, but there was a bug where "excluded" gets auto-quoted
func TestGORMSaveExcludedQuotingBug(t *testing.T) {
	t.Run("Test EXCLUDED pseudo-table quoting bug", func(t *testing.T) {
		db := setupMockDBWithConfig(t, true, true)

		// Create a test model
		type SaveTestModel struct {
			ID    uint   `gorm:"primaryKey"`
			Name  string `gorm:"not null"`
			Email string `gorm:"unique"`
		}

		model := SaveTestModel{
			ID:    1,
			Name:  "Test User",
			Email: "test@example.com",
		}

		// Use DryRun to capture the generated SQL without executing
		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&SaveTestModel{})
		if err := tempStmt.Statement.Parse(&SaveTestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		// Add OnConflict clause with DoUpdates that should reference EXCLUDED
		tempStmt.Statement.AddClause(clause.OnConflict{
			DoUpdates: clause.AssignmentColumns([]string{"name", "email"}),
		})

		tempStmt.Statement.Dest = model
		tempStmt.Statement.ReflectValue = reflect.ValueOf(model)

		// Call Create function directly to generate SQL
		Create(tempStmt)

		sql := tempStmt.Statement.SQL.String()
		if sql == "" {
			t.Fatal("Expected SQL to be generated")
		}

		t.Logf("Generated SQL: %s", sql)

		// Check that EXCLUDED is not quoted when used as pseudo-table
		if strings.Contains(sql, `"EXCLUDED"`) {
			t.Error("Found incorrectly quoted EXCLUDED pseudo-table in SQL")
		}

		// Check that EXCLUDED.column_name references are not quoted (case insensitive)
		if strings.Contains(sql, `"EXCLUDED.name"`) || strings.Contains(sql, `"EXCLUDED.email"`) ||
			strings.Contains(sql, `"excluded.name"`) || strings.Contains(sql, `"excluded.email"`) {
			t.Error("Found incorrectly quoted EXCLUDED column references in SQL")
		}

		// Verify that EXCLUDED references are present and correctly formatted
		// Should be EXCLUDED."column_name" format
		hasExcludedName := strings.Contains(sql, `EXCLUDED."name"`) || strings.Contains(sql, "EXCLUDED.name")
		hasExcludedEmail := strings.Contains(sql, `EXCLUDED."email"`) || strings.Contains(sql, "EXCLUDED.email")

		if !hasExcludedName || !hasExcludedEmail {
			t.Error("Missing expected EXCLUDED column references in SQL")
		}

		// Log the specific references found for debugging
		t.Logf("Found EXCLUDED references in UPDATE SET clause: name=%v, email=%v",
			strings.Contains(sql, "excluded.name") || strings.Contains(sql, "EXCLUDED.name"),
			strings.Contains(sql, "excluded.email") || strings.Contains(sql, "EXCLUDED.email"))
	})

	t.Run("Test GORM Save method upsert behavior", func(t *testing.T) {
		db := setupMockDBWithConfig(t, true, true)

		// Create a test model
		type SaveTestModel struct {
			ID    uint   `gorm:"primaryKey"`
			Name  string `gorm:"not null"`
			Email string `gorm:"unique"`
		}

		model := SaveTestModel{
			ID:    1,
			Name:  "Test User",
			Email: "test@example.com",
		}

		// Test what happens when we use Save (which GORM converts to upsert)
		// This simulates the actual bug scenario where Save auto-creates DoUpdates
		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&SaveTestModel{})
		if err := tempStmt.Statement.Parse(&SaveTestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		// Simulate what GORM's Save method does - creates DoUpdates that reference excluded
		// This is the problematic behavior that the user wants to fix
		doUpdates := clause.AssignmentColumns([]string{"name", "email"})
		tempStmt.Statement.AddClause(clause.OnConflict{DoUpdates: doUpdates})

		tempStmt.Statement.Dest = model
		tempStmt.Statement.ReflectValue = reflect.ValueOf(model)

		// Call Create to trigger SQL generation
		Create(tempStmt)

		sql := tempStmt.Statement.SQL.String()
		if sql == "" {
			t.Fatal("Expected SQL to be generated")
		}

		t.Logf("Generated GORM Save-style SQL: %s", sql)

		// The bug would manifest as quoted EXCLUDED references
		// Check if any EXCLUDED references are incorrectly quoted
		problematicPatterns := []string{
			`"EXCLUDED."`, // Quoted EXCLUDED pseudo-table
			`"excluded."`, // Quoted lowercase excluded pseudo-table
		}

		for _, pattern := range problematicPatterns {
			if strings.Contains(sql, pattern) {
				t.Errorf("Found problematic quoted EXCLUDED pattern: %s in SQL: %s", pattern, sql)
			}
		}

		// Verify that the EXCLUDED references are properly unquoted
		if !strings.Contains(sql, "EXCLUDED.") && !strings.Contains(sql, "excluded.") {
			t.Error("Missing EXCLUDED pseudo-table references in generated SQL")
		}
	})

	t.Run("Test manual OnConflict with excluded.column syntax", func(t *testing.T) {
		db := setupMockDBWithConfig(t, true, true)

		// Create a test model
		type TokenModel struct {
			UID          string `gorm:"primaryKey"`
			AccessToken  string
			RefreshToken string
			Expiry       int64
			TokenType    string
		}

		model := TokenModel{
			UID:          "user123",
			AccessToken:  "new_access_token",
			RefreshToken: "new_refresh_token",
			Expiry:       1234567890,
			TokenType:    "Bearer",
		}

		// Simulate user manually constructing OnConflict with excluded.COLUMN syntax
		// This is the edge case we need to handle in QuoteTo()
		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TokenModel{})
		if err := tempStmt.Statement.Parse(&TokenModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		// User manually provides "excluded.COLUMN" in clause.Column.Name
		tempStmt.Statement.AddClause(clause.OnConflict{
			Columns: []clause.Column{{Name: "UID"}},
			DoUpdates: []clause.Assignment{
				{
					Column: clause.Column{Name: "ACCESS_TOKEN"},
					Value:  clause.Column{Name: "excluded.ACCESS_TOKEN"}, // User-provided excluded.COLUMN
				},
				{
					Column: clause.Column{Name: "REFRESH_TOKEN"},
					Value:  clause.Column{Name: "excluded.REFRESH_TOKEN"}, // Lowercase excluded
				},
				{
					Column: clause.Column{Name: "EXPIRY"},
					Value:  clause.Column{Name: "EXCLUDED.EXPIRY"}, // Uppercase EXCLUDED
				},
				{
					Column: clause.Column{Name: "TOKEN_TYPE"},
					Value:  clause.Column{Name: "Excluded.TOKEN_TYPE"}, // Mixed case
				},
			},
		})

		tempStmt.Statement.Dest = model
		tempStmt.Statement.ReflectValue = reflect.ValueOf(model)

		// Call Create to trigger SQL generation
		Create(tempStmt)

		sql := tempStmt.Statement.SQL.String()
		if sql == "" {
			t.Fatal("Expected SQL to be generated")
		}

		t.Logf("Generated SQL with manual excluded.column syntax: %s", sql)

		// Verify EXCLUDED is not quoted as a table name
		if strings.Contains(sql, `"EXCLUDED"`) || strings.Contains(sql, `"excluded"`) || strings.Contains(sql, `"Excluded"`) {
			t.Error("Found incorrectly quoted EXCLUDED table name in SQL")
		}

		// Verify all EXCLUDED references are properly formatted as EXCLUDED."COLUMN"
		expectedPatterns := []string{
			`EXCLUDED."ACCESS_TOKEN"`,  // Should be uppercase EXCLUDED with quoted column
			`EXCLUDED."REFRESH_TOKEN"`, // lowercase excluded should become uppercase
			`EXCLUDED."EXPIRY"`,        // Already uppercase should stay uppercase
			`EXCLUDED."TOKEN_TYPE"`,    // Mixed case should become uppercase
		}

		for _, pattern := range expectedPatterns {
			if !strings.Contains(sql, pattern) {
				t.Errorf("Missing expected pattern %q in SQL: %s", pattern, sql)
			}
		}

		// Ensure we don't have any lowercase "excluded" in the final SQL
		if strings.Contains(sql, "excluded.") {
			t.Error("Found lowercase 'excluded.' in SQL - should be uppercase 'EXCLUDED.'")
		}

		// Verify it's a MERGE statement (since we have ON CONFLICT)
		if !strings.Contains(sql, "MERGE INTO") {
			t.Error("Expected MERGE statement for ON CONFLICT operation")
		}
	})

	t.Run("Test edge case: just EXCLUDED as column name", func(t *testing.T) {
		db := setupMockDBWithConfig(t, true, true)

		type TestModel struct {
			ID   uint `gorm:"primaryKey"`
			Name string
		}

		model := TestModel{
			ID:   1,
			Name: "Test",
		}

		// This is a semantically incorrect scenario - someone using "EXCLUDED" as a column reference
		// without a column name. This will produce invalid SQL regardless of how we handle it.
		// We now treat it as a regular identifier rather than special-casing it.
		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
		if err := tempStmt.Statement.Parse(&TestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		tempStmt.Statement.AddClause(clause.OnConflict{
			Columns: []clause.Column{{Name: "ID"}},
			DoUpdates: []clause.Assignment{
				{
					Column: clause.Column{Name: "name"},
					Value:  clause.Column{Name: "EXCLUDED"}, // Just "EXCLUDED" - semantically wrong
				},
			},
		})

		tempStmt.Statement.Dest = model
		tempStmt.Statement.ReflectValue = reflect.ValueOf(model)

		Create(tempStmt)

		sql := tempStmt.Statement.SQL.String()
		if sql == "" {
			t.Fatal("Expected SQL to be generated")
		}

		t.Logf("Generated SQL with just EXCLUDED as column: %s", sql)

		// Since we removed special handling, standalone "EXCLUDED" is now treated as a regular identifier
		// and will be quoted. This produces invalid SQL, but we're not special-casing it anymore.
		if !strings.Contains(sql, `"EXCLUDED"`) {
			t.Error("Expected EXCLUDED to be quoted as a regular identifier")
		}

		// The SQL will be semantically incorrect (UPDATE SET "name"="EXCLUDED" which makes no sense)
		// We let this fail naturally rather than trying to prevent it.
		t.Log("Note: This generates semantically incorrect SQL - standalone EXCLUDED is treated as regular identifier")
	})

	t.Run("Test QuoteTo method with various inputs", func(t *testing.T) {
		dialector := New(Config{QuoteFields: true})

		testCases := []struct {
			input    string
			expected string
			name     string
		}{
			{
				input:    "table_name.column_name",
				expected: `"table_name"."column_name"`,
				name:     "regular table.column should be quoted",
			},
			{
				input:    "column_name",
				expected: `"column_name"`,
				name:     "regular column should be quoted",
			},
			// Note: "EXCLUDED" is never passed to QuoteTo() in production
			// It's always written as WriteString("EXCLUDED.") + WriteQuoted(columnName)
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				var buf strings.Builder
				writer := &clauseWriter{&buf}

				dialector.QuoteTo(writer, tc.input)
				result := buf.String()

				if result != tc.expected {
					t.Errorf("QuoteTo(%q) = %q, expected %q", tc.input, result, tc.expected)
				}
			})
		}

		// Test production pattern: WriteString("EXCLUDED.") + WriteQuoted(column)
		t.Run("production pattern for EXCLUDED columns", func(t *testing.T) {
			testColumns := []struct {
				column   string
				expected string
			}{
				{"name", `EXCLUDED."name"`},
				{"email", `EXCLUDED."email"`},
				{"field_name", `EXCLUDED."field_name"`},
				{"field_123", `EXCLUDED."field_123"`},
			}

			for _, tc := range testColumns {
				var buf strings.Builder
				writer := &clauseWriter{&buf}

				// This is how production code uses it in create.go
				writer.WriteString("EXCLUDED.")
				dialector.QuoteTo(writer, tc.column)

				result := buf.String()
				if result != tc.expected {
					t.Errorf("EXCLUDED.%s pattern = %q, expected %q", tc.column, result, tc.expected)
				}
			}
		})
	})

	t.Run("Test DoUpdates clause building with EXCLUDED", func(t *testing.T) {
		db := setupMockDBWithConfig(t, true, true)

		// Create assignments that should reference EXCLUDED
		assignments := clause.Assignments(map[string]interface{}{
			"name":  clause.Expr{SQL: "EXCLUDED.name"},
			"email": clause.Expr{SQL: "EXCLUDED.email"},
		})

		// Build the assignments clause
		assignments.Build(db.Statement)

		sql := db.Statement.SQL.String()
		t.Logf("Generated DoUpdates SQL: %s", sql)

		// Verify EXCLUDED references are not quoted
		if strings.Contains(sql, `"EXCLUDED"`) {
			t.Error("Found incorrectly quoted EXCLUDED in DoUpdates clause")
		}

		if !strings.Contains(sql, "EXCLUDED.name") || !strings.Contains(sql, "EXCLUDED.email") {
			t.Error("Missing expected EXCLUDED references in DoUpdates clause")
		}
	})

	t.Run("Test problematic DoUpdates scenario", func(t *testing.T) {
		db := setupMockDBWithConfig(t, true, true)

		// Create a scenario that might trigger the bug - explicit EXCLUDED references in DoUpdates
		// This tests the scenario where DoUpdates contains explicit SQL with EXCLUDED
		doUpdates := clause.Set{
			// This is the problematic case - when someone explicitly uses EXCLUDED in assignments
			clause.Assignment{Column: clause.Column{Name: "name"}, Value: clause.Expr{SQL: "EXCLUDED.name"}},
			clause.Assignment{Column: clause.Column{Name: "email"}, Value: clause.Expr{SQL: "EXCLUDED.email"}},
		}

		tempStmt := db.Session(&gorm.Session{DryRun: true})
		tempStmt.Statement.SQL.Reset()
		tempStmt.Statement.Vars = []interface{}{}

		// Build just the DoUpdates part to see what happens
		doUpdates.Build(tempStmt.Statement)

		sql := tempStmt.Statement.SQL.String()
		t.Logf("DoUpdates SQL: %s", sql)

		// This is where the bug would manifest - EXCLUDED getting quoted
		if strings.Contains(sql, `"EXCLUDED.`) {
			t.Errorf("Found incorrectly quoted EXCLUDED in DoUpdates: %s", sql)
		}

		// Verify the EXCLUDED references are preserved correctly
		if !strings.Contains(sql, "EXCLUDED.name") || !strings.Contains(sql, "EXCLUDED.email") {
			t.Error("Missing expected EXCLUDED references in DoUpdates")
		}
	})

	t.Run("Test edge case with mixed assignments", func(t *testing.T) {
		db := setupMockDBWithConfig(t, true, true)

		// Test a mixture of regular assignments and EXCLUDED references
		doUpdates := clause.Set{
			clause.Assignment{Column: clause.Column{Name: "name"}, Value: "fixed_value"},
			clause.Assignment{Column: clause.Column{Name: "email"}, Value: clause.Expr{SQL: "EXCLUDED.email"}},
			clause.Assignment{Column: clause.Column{Name: "updated_at"}, Value: clause.Expr{SQL: "NOW()"}},
		}

		tempStmt := db.Session(&gorm.Session{DryRun: true})
		tempStmt.Statement.SQL.Reset()
		tempStmt.Statement.Vars = []interface{}{}

		doUpdates.Build(tempStmt.Statement)

		sql := tempStmt.Statement.SQL.String()
		t.Logf("Mixed assignments SQL: %s", sql)

		// Verify EXCLUDED is not quoted
		if strings.Contains(sql, `"EXCLUDED.`) {
			t.Errorf("Found incorrectly quoted EXCLUDED in mixed assignments: %s", sql)
		}

		// Note: Column names in SET clause may not be quoted here because we're testing DoUpdates in isolation
		// This is expected behavior when building assignments directly
		t.Logf("Column quoting in DoUpdates clause depends on context - this is expected")

		// Verify EXCLUDED reference is preserved
		if !strings.Contains(sql, "EXCLUDED.email") {
			t.Error("Missing EXCLUDED.email reference")
		}
	})

	t.Run("Test QuoteTo with column names (real-world usage)", func(t *testing.T) {
		dialector := New(Config{QuoteFields: true})

		// Test how QuoteTo handles plain column names
		// In production, WriteString("EXCLUDED.") is used before WriteQuoted(column.Name)
		testCases := []struct {
			input       string
			expected    string
			description string
		}{
			{
				input:       "RISK",
				expected:    `"RISK"`, // Plain column name gets quoted
				description: "RISK becomes \"RISK\"",
			},
			{
				input:       "SME",
				expected:    `"SME"`, // Plain column name gets quoted
				description: "SME becomes \"SME\"",
			},
			{
				input:       "email",
				expected:    `"email"`, // Plain column name gets quoted
				description: "email becomes \"email\"",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.description, func(t *testing.T) {
				var buf strings.Builder
				writer := &clauseWriter{&buf}

				dialector.QuoteTo(writer, tc.input)
				result := buf.String()

				t.Logf("QuoteTo(%q) = %q", tc.input, result)

				if result != tc.expected {
					t.Errorf("QuoteTo(%q) = %q, expected %q", tc.input, result, tc.expected)
				}
			})
		}

		// Test the production pattern: WriteString("EXCLUDED.") + WriteQuoted(column)
		t.Run("Production pattern: EXCLUDED. + quoted column", func(t *testing.T) {
			var buf strings.Builder
			writer := &clauseWriter{&buf}

			// This is how create.go uses it in production
			writer.WriteString("EXCLUDED.")
			dialector.QuoteTo(writer, "RISK")

			result := buf.String()
			expected := `EXCLUDED."RISK"`

			t.Logf("WriteString(\"EXCLUDED.\") + QuoteTo(\"RISK\") = %q", result)

			if result != expected {
				t.Errorf("Production pattern result = %q, expected %q", result, expected)
			}

			// Ensure EXCLUDED table name itself is not quoted
			if strings.Contains(result, `"EXCLUDED"`) {
				t.Errorf("FOUND BUG: EXCLUDED table name is incorrectly quoted: %s", result)
			}
		})

		// Test that prepareOnConflictForMerge transforms user-provided excluded.column
		t.Run("prepareOnConflictForMerge transforms excluded.column", func(t *testing.T) {
			db := setupMockDBWithConfig(t, true, true)

			testCases := []struct {
				input       string
				expected    string
				description string
			}{
				{
					input:       "excluded.ACCESS_TOKEN",
					expected:    `EXCLUDED."ACCESS_TOKEN"`,
					description: "lowercase excluded.ACCESS_TOKEN",
				},
				{
					input:       "EXCLUDED.ACCESS_TOKEN",
					expected:    `EXCLUDED."ACCESS_TOKEN"`,
					description: "uppercase EXCLUDED.ACCESS_TOKEN",
				},
				{
					input:       "Excluded.REFRESH_TOKEN",
					expected:    `EXCLUDED."REFRESH_TOKEN"`,
					description: "mixed case Excluded.REFRESH_TOKEN",
				},
			}

			for _, tc := range testCases {
				t.Run(tc.description, func(t *testing.T) {
					// Test 1: Verify prepareOnConflictForMerge transformation
					onConflict := clause.OnConflict{
						DoUpdates: []clause.Assignment{
							{
								Column: clause.Column{Name: "test_col"},
								Value:  clause.Column{Name: tc.input},
							},
						},
					}

					// Call prepareOnConflictForMerge
					transformed := prepareOnConflictForMerge(db, onConflict)

					// The DoUpdates should now contain clause.Expr with the transformed SQL
					if len(transformed.DoUpdates) != 1 {
						t.Fatalf("Expected 1 DoUpdate, got %d", len(transformed.DoUpdates))
					}

					assignment := transformed.DoUpdates[0]
					expr, ok := assignment.Value.(clause.Expr)
					if !ok {
						t.Fatalf("Expected clause.Expr, got %T", assignment.Value)
					}

					t.Logf("Input: %q -> Transformed SQL: %q", tc.input, expr.SQL)

					if expr.SQL != tc.expected {
						t.Errorf("prepareOnConflictForMerge(%q) = %q, expected %q", tc.input, expr.SQL, tc.expected)
					}

					// Ensure EXCLUDED table name is not quoted
					if strings.Contains(expr.SQL, `"EXCLUDED"`) || strings.Contains(expr.SQL, `"excluded"`) {
						t.Errorf("FOUND BUG: EXCLUDED table name is incorrectly quoted: %s", expr.SQL)
					}

					// Test 2: Verify the SQL output in a full MERGE statement
					type TestModel struct {
						ID      uint `gorm:"primaryKey"`
						TestCol string
					}

					model := TestModel{ID: 1, TestCol: "test"}

					tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
					if err := tempStmt.Statement.Parse(&TestModel{}); err != nil {
						t.Fatalf("Failed to parse model: %v", err)
					}

					tempStmt.Statement.AddClause(clause.OnConflict{
						Columns: []clause.Column{{Name: "ID"}},
						DoUpdates: []clause.Assignment{
							{
								Column: clause.Column{Name: "test_col"},
								Value:  clause.Column{Name: tc.input},
							},
						},
					})

					tempStmt.Statement.Dest = model
					tempStmt.Statement.ReflectValue = reflect.ValueOf(model)

					Create(tempStmt)

					sql := tempStmt.Statement.SQL.String()
					t.Logf("Full SQL: %s", sql)

					// Verify the exact EXCLUDED pattern appears in the SQL
					expectedPattern := fmt.Sprintf(`"test_col"=%s`, tc.expected)
					if !strings.Contains(sql, expectedPattern) {
						t.Errorf("Expected SQL to contain %q, got: %s", expectedPattern, sql)
					}

					// Ensure EXCLUDED keyword is uppercase and unquoted
					if strings.Contains(sql, `"EXCLUDED"`) || strings.Contains(sql, `"excluded"`) || strings.Contains(sql, `"Excluded"`) {
						t.Errorf("FOUND BUG: EXCLUDED table name is quoted in final SQL: %s", sql)
					}
				})
			}
		})
	})
}

// clauseWriter implements clause.Writer for testing
type clauseWriter struct {
	*strings.Builder
}

func (w *clauseWriter) WriteByte(b byte) error {
	w.Builder.WriteByte(b)
	return nil
}

func (w *clauseWriter) WriteString(s string) (int, error) {
	return w.Builder.WriteString(s)
}
