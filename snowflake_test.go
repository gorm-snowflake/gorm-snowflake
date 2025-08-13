package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type clauseBuilder struct{}

func (c clauseBuilder) WriteByte(b byte) error {
	writeOut(string(b))
	return nil
}

func (c clauseBuilder) WriteString(s string) (int, error) {
	writeOut(s)
	return len(s), nil
}

var out string

func writeOut(s string) {
	out += s
}

func TestQuoteToFunction(t *testing.T) {
	t.Run("Quotes Enabled", func(t *testing.T) {
		t.Cleanup(teardown)
		c := clauseBuilder{}

		dialector := New(Config{QuoteFields: true})

		dialector.QuoteTo(c, "TEST_FUNCTION1(test)")

		const expected = `TEST_FUNCTION1("test")`
		if out != expected {
			t.Errorf("Expected %s got %s", expected, out)
		}
	})

	t.Run("Quotes Disabled", func(t *testing.T) {
		t.Cleanup(teardown)
		c := clauseBuilder{}

		dialector := New(Config{})

		const input = "TEST_FUNCTION1(test)"

		dialector.QuoteTo(c, input)

		expected := strings.ToLower(input)
		if out != expected {
			t.Errorf("Expected %s got %s", expected, out)
		}
	})
}

func TestQuoteToExcluded(t *testing.T) {
	t.Cleanup(teardown)
	c := clauseBuilder{}

	dialector := New(Config{QuoteFields: true})

	const expected = "excluded.test"

	dialector.QuoteTo(c, expected)

	if out != expected {
		t.Errorf("Expected %s got %s", expected, out)
	}
}

func TestQuoteToEXCLUDED(t *testing.T) {
	t.Cleanup(teardown)
	c := clauseBuilder{}

	dialector := New(Config{QuoteFields: true})

	const expected = "EXCLUDED.TEST"

	dialector.QuoteTo(c, expected)

	if out != expected {
		t.Errorf("Expected %s got %s", expected, out)
	}
}

func teardown() {
	out = ""
}

// Mock connection pool for testing
type mockConnPool struct{}

func (m *mockConnPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return &mockResult{rowsAffected: int64(len(args) / 3)}, nil // Assume 3 columns per row
}

func (m *mockConnPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return nil, fmt.Errorf("no rows for test")
}

func (m *mockConnPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return nil
}

func (m *mockConnPool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockConnPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	return m, nil
}

func (m *mockConnPool) Ping() error {
	return nil
}

type mockResult struct {
	rowsAffected int64
}

func (m *mockResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (m *mockResult) RowsAffected() (int64, error) {
	return m.rowsAffected, nil
}

// Test model
type TestModel struct {
	ID   uint   `gorm:"primaryKey;autoIncrement"`
	Name string `gorm:"not null"`
	Age  int
}

func TestBatchInsert(t *testing.T) {
	t.Run("Batch Insert with UNION SELECT", func(t *testing.T) {
		// Setup GORM DB with mock
		db := setupMockDB(t)

		// Create test data
		models := []TestModel{
			{Name: "John", Age: 25},
			{Name: "Jane", Age: 30},
			{Name: "Bob", Age: 35},
		}

		// Parse the model schema first to get the proper schema
		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
		if err := tempStmt.Statement.Parse(&TestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		// Now set up the values for create
		tempStmt.Statement.Dest = models
		tempStmt.Statement.ReflectValue = reflect.ValueOf(models)

		// Reset SQL to ensure we're starting fresh
		tempStmt.Statement.SQL.Reset()
		tempStmt.Statement.Vars = nil

		// Call our Create function directly
		Create(tempStmt)

		// Verify the generated SQL
		sql := tempStmt.Statement.SQL.String()

		// Assert the complete SQL structure
		expectedSQL := "INSERT INTO test_models (name,age) SELECT ?,? UNION SELECT ?,? UNION SELECT ?,?;"
		if sql != expectedSQL {
			t.Errorf("Expected exact SQL:\n%s\nGot:\n%s", expectedSQL, sql)
		}

		// Verify variables are correct
		expectedVars := []interface{}{"John", 25, "Jane", 30, "Bob", 35}
		if len(tempStmt.Statement.Vars) != len(expectedVars) {
			t.Errorf("Expected %d variables, got %d", len(expectedVars), len(tempStmt.Statement.Vars))
		}
		for i, expected := range expectedVars {
			if i < len(tempStmt.Statement.Vars) && tempStmt.Statement.Vars[i] != expected {
				t.Errorf("Variable %d: expected %v, got %v", i, expected, tempStmt.Statement.Vars[i])
			}
		}

		t.Logf("Generated SQL: %s", sql)
		t.Logf("Variables: %v", tempStmt.Statement.Vars)
	})

	t.Run("Single Row Insert", func(t *testing.T) {
		db := setupMockDB(t)

		// Create single test model
		model := TestModel{Name: "John", Age: 25}

		// Parse the model schema first to get the proper schema
		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
		if err := tempStmt.Statement.Parse(&TestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		// Now set up the values for create
		tempStmt.Statement.Dest = model
		tempStmt.Statement.ReflectValue = reflect.ValueOf(model)

		// Reset SQL to ensure we're starting fresh
		tempStmt.Statement.SQL.Reset()
		tempStmt.Statement.Vars = nil

		// Call our Create function directly
		Create(tempStmt)

		sql := tempStmt.Statement.SQL.String()

		// Assert the complete SQL structure
		expectedSQL := "INSERT INTO test_models (name,age) SELECT ?,?;"
		if sql != expectedSQL {
			t.Errorf("Expected exact SQL:\n%s\nGot:\n%s", expectedSQL, sql)
		}

		// Verify variables are correct
		expectedVars := []interface{}{"John", 25}
		if len(tempStmt.Statement.Vars) != len(expectedVars) {
			t.Errorf("Expected %d variables, got %d", len(expectedVars), len(tempStmt.Statement.Vars))
		}
		for i, expected := range expectedVars {
			if i < len(tempStmt.Statement.Vars) && tempStmt.Statement.Vars[i] != expected {
				t.Errorf("Variable %d: expected %v, got %v", i, expected, tempStmt.Statement.Vars[i])
			}
		}

		t.Logf("Single row SQL: %s", sql)
	})

	t.Run("Empty Values - Auto Increment Only", func(t *testing.T) {
		db := setupMockDB(t)

		// Create a model with only auto-increment field
		type AutoIncrementOnly struct {
			ID uint `gorm:"primaryKey;autoIncrement"`
		}

		model := AutoIncrementOnly{}

		// Parse the model schema first to get the proper schema
		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&AutoIncrementOnly{})
		if err := tempStmt.Statement.Parse(&AutoIncrementOnly{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		// Now set up the values for create
		tempStmt.Statement.Dest = model
		tempStmt.Statement.ReflectValue = reflect.ValueOf(model)

		// Reset SQL to ensure we're starting fresh
		tempStmt.Statement.SQL.Reset()
		tempStmt.Statement.Vars = nil

		// Call our Create function directly
		Create(tempStmt)

		sql := tempStmt.Statement.SQL.String()

		// Assert the complete SQL structure
		expectedSQL := "INSERT INTO auto_increment_onlies VALUES (DEFAULT);"
		if sql != expectedSQL {
			t.Errorf("Expected exact SQL:\n%s\nGot:\n%s", expectedSQL, sql)
		}

		// Verify no variables for DEFAULT values
		if len(tempStmt.Statement.Vars) != 0 {
			t.Errorf("Expected 0 variables for DEFAULT insert, got %d: %v", len(tempStmt.Statement.Vars), tempStmt.Statement.Vars)
		}

		t.Logf("Auto increment only SQL: %s", sql)
	})
}

func TestBatchInsertMethods(t *testing.T) {
	t.Run("VALUES Syntax for Performance", func(t *testing.T) {
		// Setup GORM DB with VALUES syntax (UseUnionSelect: false)
		db := setupMockDBWithConfig(t, false)

		// Create test data
		models := []TestModel{
			{Name: "John", Age: 25},
			{Name: "Jane", Age: 30},
			{Name: "Bob", Age: 35},
		}

		// Parse the model schema first to get the proper schema
		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
		if err := tempStmt.Statement.Parse(&TestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		// Now set up the values for create
		tempStmt.Statement.Dest = models
		tempStmt.Statement.ReflectValue = reflect.ValueOf(models)

		// Reset SQL to ensure we're starting fresh
		tempStmt.Statement.SQL.Reset()
		tempStmt.Statement.Vars = nil

		// Call our Create function directly
		Create(tempStmt)

		// Verify the generated SQL uses VALUES syntax
		sql := tempStmt.Statement.SQL.String()

		// Assert the complete SQL structure
		expectedSQL := "INSERT INTO test_models (name,age) VALUES (?,?),(?,?),(?,?);"
		if sql != expectedSQL {
			t.Errorf("Expected exact SQL:\n%s\nGot:\n%s", expectedSQL, sql)
		}

		// Verify it does NOT contain UNION SELECT
		if strings.Contains(sql, "UNION SELECT") {
			t.Errorf("VALUES syntax should not contain 'UNION SELECT', got: %s", sql)
		}

		// Verify variables are correct
		expectedVars := []interface{}{"John", 25, "Jane", 30, "Bob", 35}
		if len(tempStmt.Statement.Vars) != len(expectedVars) {
			t.Errorf("Expected %d variables, got %d", len(expectedVars), len(tempStmt.Statement.Vars))
		}
		for i, expected := range expectedVars {
			if i < len(tempStmt.Statement.Vars) && tempStmt.Statement.Vars[i] != expected {
				t.Errorf("Variable %d: expected %v, got %v", i, expected, tempStmt.Statement.Vars[i])
			}
		}

		t.Logf("Generated SQL (VALUES): %s", sql)
		t.Logf("Variables: %v", tempStmt.Statement.Vars)
	})

	t.Run("UNION SELECT Syntax for Function Support", func(t *testing.T) {
		// Setup GORM DB with UNION SELECT syntax (UseUnionSelect: true)
		db := setupMockDBWithConfig(t, true)

		// Create test data
		models := []TestModel{
			{Name: "John", Age: 25},
			{Name: "Jane", Age: 30},
		}

		// Parse the model schema first to get the proper schema
		tempStmt := db.Session(&gorm.Session{DryRun: true}).Model(&TestModel{})
		if err := tempStmt.Statement.Parse(&TestModel{}); err != nil {
			t.Fatalf("Failed to parse model: %v", err)
		}

		// Now set up the values for create
		tempStmt.Statement.Dest = models
		tempStmt.Statement.ReflectValue = reflect.ValueOf(models)

		// Reset SQL to ensure we're starting fresh
		tempStmt.Statement.SQL.Reset()
		tempStmt.Statement.Vars = nil

		// Call our Create function directly
		Create(tempStmt)

		// Verify the generated SQL uses UNION SELECT syntax
		sql := tempStmt.Statement.SQL.String()

		// Assert the complete SQL structure
		expectedSQL := "INSERT INTO test_models (name,age) SELECT ?,? UNION SELECT ?,?;"
		if sql != expectedSQL {
			t.Errorf("Expected exact SQL:\n%s\nGot:\n%s", expectedSQL, sql)
		}

		// Verify it contains UNION SELECT
		if !strings.Contains(sql, "UNION SELECT") {
			t.Errorf("UNION SELECT syntax should contain 'UNION SELECT', got: %s", sql)
		}

		// Verify variables are correct
		expectedVars := []interface{}{"John", 25, "Jane", 30}
		if len(tempStmt.Statement.Vars) != len(expectedVars) {
			t.Errorf("Expected %d variables, got %d", len(expectedVars), len(tempStmt.Statement.Vars))
		}
		for i, expected := range expectedVars {
			if i < len(tempStmt.Statement.Vars) && tempStmt.Statement.Vars[i] != expected {
				t.Errorf("Variable %d: expected %v, got %v", i, expected, tempStmt.Statement.Vars[i])
			}
		}

		t.Logf("Generated SQL (UNION SELECT): %s", sql)
		t.Logf("Variables: %v", tempStmt.Statement.Vars)
	})
}

func TestBatchInsertWithConflict(t *testing.T) {
	t.Run("Merge Create with Conflict", func(t *testing.T) {
		db := setupMockDB(t)

		// Create test data with IDs for conflict detection
		models := []TestModel{
			{ID: 1, Name: "John", Age: 25},
			{ID: 2, Name: "Jane", Age: 30},
		}

		// Parse the model schema first to get the proper schema
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

		// Now set up the values for create
		tempStmt.Statement.Dest = models
		tempStmt.Statement.ReflectValue = reflect.ValueOf(models)

		// Reset SQL to ensure we're starting fresh
		tempStmt.Statement.SQL.Reset()
		tempStmt.Statement.Vars = nil

		// Call our Create function directly
		Create(tempStmt)

		sql := tempStmt.Statement.SQL.String()

		// Assert the complete SQL structure (the exact format may vary slightly)
		// We'll check key components and overall structure
		expectedSQLPattern := "MERGE INTO test_models USING (VALUES(?,?,?),(?,?,?)) AS EXCLUDED (name,age,id) ON \"test_models\".\"id\" = EXCLUDED.id WHEN MATCHED THEN UPDATE SET age=age WHEN NOT MATCHED THEN INSERT (name,age) VALUES (EXCLUDED.name,EXCLUDED.age);"
		if sql != expectedSQLPattern {
			t.Errorf("Expected exact SQL:\n%s\nGot:\n%s", expectedSQLPattern, sql)
		}

		// Verify variables are correct (name, age, id for each row)
		expectedVars := []interface{}{"John", 25, uint(1), "Jane", 30, uint(2)}
		if len(tempStmt.Statement.Vars) != len(expectedVars) {
			t.Errorf("Expected %d variables, got %d", len(expectedVars), len(tempStmt.Statement.Vars))
		}
		for i, expected := range expectedVars {
			if i < len(tempStmt.Statement.Vars) && tempStmt.Statement.Vars[i] != expected {
				t.Errorf("Variable %d: expected %v, got %v", i, expected, tempStmt.Statement.Vars[i])
			}
		}

		t.Logf("Merge SQL: %s", sql)
	})
}

func setupMockDB(t *testing.T) *gorm.DB {
	return setupMockDBWithConfig(t, true) // Default to UNION SELECT for backward compatibility
}

func setupMockDBWithConfig(t *testing.T, useUnionSelect bool) *gorm.DB {
	// Create a dialector with a mock connection
	mockPool := &mockConnPool{}
	dialector := &Dialector{
		Config: &Config{
			Conn:           mockPool,
			DriverName:     "snowflake",
			UseUnionSelect: useUnionSelect,
		},
	}

	db, err := gorm.Open(dialector, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("Failed to setup mock DB: %v", err)
	}

	return db
}
