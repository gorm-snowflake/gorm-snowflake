package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
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

// Note: EXCLUDED handling is tested in create_test.go via integration tests
// QuoteTo() never receives "EXCLUDED" as input in production - it's always
// written as WriteString("EXCLUDED.") + WriteQuoted(columnName)

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
		expectedSQL := "INSERT INTO \"test_models\" (\"name\",\"age\") SELECT ?,? UNION SELECT ?,? UNION SELECT ?,?;"
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
		expectedSQL := "INSERT INTO \"test_models\" (\"name\",\"age\") SELECT ?,?;"
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
		expectedSQL := "INSERT INTO \"auto_increment_onlies\" VALUES (DEFAULT);"
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
		db := setupMockDBWithConfig(t, false, true)

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
		expectedSQL := "INSERT INTO \"test_models\" (\"name\",\"age\") VALUES (?,?),(?,?),(?,?);"
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
		db := setupMockDBWithConfig(t, true, true)

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
		expectedSQL := "INSERT INTO \"test_models\" (\"name\",\"age\") SELECT ?,? UNION SELECT ?,?;"
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
		expectedSQLPattern := "MERGE INTO \"test_models\" USING (VALUES(?,?,?),(?,?,?)) AS EXCLUDED (\"name\",\"age\",\"id\") ON \"test_models\".\"id\" = EXCLUDED.\"id\" WHEN MATCHED THEN UPDATE SET \"age\"=EXCLUDED.\"age\" WHEN NOT MATCHED THEN INSERT (\"name\",\"age\") VALUES (EXCLUDED.\"name\",EXCLUDED.\"age\");"
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

	t.Run("Merge Create without Quoting", func(t *testing.T) {
		// Setup DB without quoting enabled
		mockPool := &mockConnPool{}
		dialector := &Dialector{
			Config: &Config{
				Conn:           mockPool,
				DriverName:     "snowflake",
				UseUnionSelect: true,
				QuoteFields:    false, // Disable quoting
			},
		}

		db, err := gorm.Open(dialector, &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		if err != nil {
			t.Fatalf("Failed to setup mock DB: %v", err)
		}

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

		// When QuoteFields is false, identifiers should be unquoted (Snowflake will uppercase them)
		expectedSQLPattern := "MERGE INTO test_models USING (VALUES(?,?,?),(?,?,?)) AS EXCLUDED (name,age,id) ON test_models.id = EXCLUDED.id WHEN MATCHED THEN UPDATE SET age=EXCLUDED.age WHEN NOT MATCHED THEN INSERT (name,age) VALUES (EXCLUDED.name,EXCLUDED.age);"
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

		t.Logf("Merge SQL (no quotes): %s", sql)
	})
}

func setupMockDB(t *testing.T) *gorm.DB {
	return setupMockDBWithConfig(t, true, true) // Default to UNION SELECT for backward compatibility
}

func setupMockDBWithConfig(t *testing.T, useUnionSelect bool, quoteFields bool) *gorm.DB {
	// Create a dialector with a mock connection
	mockPool := &mockConnPool{}
	dialector := &Dialector{
		Config: &Config{
			Conn:           mockPool,
			DriverName:     "snowflake",
			UseUnionSelect: useUnionSelect,
			QuoteFields:    true, // Enable quoting for realistic testing
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

// Additional test models for comprehensive testing
type UserModel struct {
	ID       uint   `gorm:"primaryKey;autoIncrement"`
	Name     string `gorm:"not null;size:255"`
	Email    string `gorm:"unique;size:100"`
	Age      int    `gorm:"check:age >= 0"`
	IsActive bool   `gorm:"default:true"`
	Balance  float64
	Data     []byte
	CreateAt *time.Time `gorm:"autoCreateTime"`
}

type PostModel struct {
	ID     uint      `gorm:"primaryKey;autoIncrement"`
	Title  string    `gorm:"not null;size:500"`
	UserID uint      `gorm:"not null"`
	User   UserModel `gorm:"foreignKey:UserID;references:ID"`
}

type TagModel struct {
	ID    uint        `gorm:"primaryKey;autoIncrement"`
	Name  string      `gorm:"not null;unique;size:50"`
	Posts []PostModel `gorm:"many2many:post_tags;"`
}

// TestDialectorName tests the Name method
func TestDialectorName(t *testing.T) {
	dialector := New(Config{})
	if dialector.Name() != SnowflakeDriverName {
		t.Errorf("Expected dialector name to be %s, got %s", SnowflakeDriverName, dialector.Name())
	}
}

// TestDialectorOpen tests the Open function
func TestDialectorOpen(t *testing.T) {
	dsn := "user:password@account/database"
	dialector := Open(dsn)

	if dialector.Config.DSN != dsn {
		t.Errorf("Expected DSN to be %s, got %s", dsn, dialector.Config.DSN)
	}

	if dialector.Config.DriverName != SnowflakeDriverName {
		t.Errorf("Expected DriverName to be %s, got %s", SnowflakeDriverName, dialector.Config.DriverName)
	}

	if !dialector.Config.UseUnionSelect {
		t.Error("Expected UseUnionSelect to be true by default")
	}
}

// TestDialectorNew tests the New function
func TestDialectorNew(t *testing.T) {
	config := Config{
		QuoteFields:    true,
		DriverName:     "custom-snowflake",
		DSN:            "custom-dsn",
		UseUnionSelect: false,
	}

	dialector := New(config)
	d := dialector.(*Dialector)

	if d.Config.QuoteFields != true {
		t.Error("Expected QuoteFields to be true")
	}

	if d.Config.DriverName != "custom-snowflake" {
		t.Errorf("Expected DriverName to be custom-snowflake, got %s", d.Config.DriverName)
	}

	if d.Config.UseUnionSelect {
		t.Error("Expected UseUnionSelect to be false")
	}
}

// TestDialectorInitialize tests the Initialize method
func TestDialectorInitialize(t *testing.T) {
	config := Config{
		Conn:       &mockConnPool{},
		DriverName: "snowflake",
	}

	dialector := New(config)
	db, err := gorm.Open(dialector, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})

	if err != nil {
		t.Fatalf("Failed to initialize dialector: %v", err)
	}

	// Test that the create callback is registered
	if db.Callback().Create().Get("gorm:create") == nil {
		t.Error("Expected gorm:create callback to be registered")
	}

	// Test that clause builders are registered
	if _, exists := db.ClauseBuilders["LIMIT"]; !exists {
		t.Error("Expected LIMIT clause builder to be registered")
	}
}

// TestDialectorClauseBuilders tests the ClauseBuilders method
func TestDialectorClauseBuilders(t *testing.T) {
	dialector := New(Config{}).(*Dialector)
	builders := dialector.ClauseBuilders()

	if _, exists := builders["LIMIT"]; !exists {
		t.Error("Expected LIMIT clause builder to exist")
	}

	// Test LIMIT with offset
	limit := clause.Limit{Offset: 10, Limit: &[]int{5}[0]}
	c := clause.Clause{Expression: limit}

	// Create a mock statement that implements clause.Builder
	db := setupMockDB(t)
	mockStmt := &gorm.Statement{
		DB: db,
		Schema: &schema.Schema{
			PrioritizedPrimaryField: &schema.Field{DBName: "id"},
		},
		Clauses: make(map[string]clause.Clause),
		SQL:     strings.Builder{},
	}

	// Call the LIMIT clause builder
	builders["LIMIT"](c, mockStmt)

	result := mockStmt.SQL.String()
	if !strings.Contains(result, "OFFSET 10 ROWS") {
		t.Errorf("Expected OFFSET clause in result, got: %s", result)
	}
	if !strings.Contains(result, "FETCH NEXT 5 ROWS ONLY") {
		t.Errorf("Expected FETCH NEXT clause in result, got: %s", result)
	}
}

// TestDialectorDefaultValueOf tests the DefaultValueOf method
func TestDialectorDefaultValueOf(t *testing.T) {
	dialector := New(Config{})
	field := &schema.Field{Name: "test"}

	defaultValue := dialector.DefaultValueOf(field)
	expr, ok := defaultValue.(clause.Expr)
	if !ok {
		t.Fatal("Expected clause.Expr")
	}

	if expr.SQL != "NULL" {
		t.Errorf("Expected default value to be NULL, got %s", expr.SQL)
	}
}

// TestDialectorBindVarTo tests the BindVarTo method
func TestDialectorBindVarTo(t *testing.T) {
	dialector := New(Config{})
	builder := &strings.Builder{}
	mockWriter := &mockClauseWriter{builder: builder}

	stmt := &gorm.Statement{}
	dialector.BindVarTo(mockWriter, stmt, "test")

	if builder.String() != "?" {
		t.Errorf("Expected '?', got %s", builder.String())
	}
}

// TestDialectorExplain tests the Explain method
func TestDialectorExplain(t *testing.T) {
	dialector := New(Config{})

	sql := "SELECT * FROM users WHERE id = ?"
	vars := []interface{}{1}

	result := dialector.Explain(sql, vars...)

	// Should contain the SQL structure
	if !strings.Contains(result, "SELECT * FROM users WHERE id") {
		t.Errorf("Expected result to contain SELECT query, got: %s", result)
	}
}

// TestDialectorDataTypeOf tests the DataTypeOf method for various field types
func TestDialectorDataTypeOf(t *testing.T) {
	dialector := New(Config{})

	tests := []struct {
		field    *schema.Field
		expected string
	}{
		{&schema.Field{DataType: schema.Bool}, "BOOLEAN"},
		{&schema.Field{DataType: schema.Int, Size: 8}, "SMALLINT"},
		{&schema.Field{DataType: schema.Int, Size: 16}, "INT"},
		{&schema.Field{DataType: schema.Int, Size: 64}, "BIGINT"},
		{&schema.Field{DataType: schema.Int, Size: 32, AutoIncrement: true}, "BIGINT IDENTITY(1,1)"},
		{&schema.Field{DataType: schema.Uint, Size: 8}, "SMALLINT"},
		{&schema.Field{DataType: schema.Float}, "FLOAT"},
		{&schema.Field{DataType: schema.String, Size: 100}, "VARCHAR(100)"},
		{&schema.Field{DataType: schema.String, Size: 5000}, "VARCHAR"},
		{&schema.Field{DataType: schema.String, PrimaryKey: true}, "VARCHAR(256)"},
		{&schema.Field{DataType: schema.String, TagSettings: map[string]string{"INDEX": "idx_name"}}, "VARCHAR(256)"},
		{&schema.Field{DataType: schema.Time}, "TIMESTAMP_NTZ"},
		{&schema.Field{DataType: schema.Bytes}, "VARBINARY"},
		{&schema.Field{DataType: "CUSTOM"}, "CUSTOM"},
	}

	for _, test := range tests {
		result := dialector.DataTypeOf(test.field)
		if result != test.expected {
			t.Errorf("For field %+v, expected %s, got %s", test.field, test.expected, result)
		}
	}
}

// TestDialectorSavePoint tests the SavePoint method
func TestDialectorSavePoint(t *testing.T) {
	dialector := New(Config{}).(*Dialector)
	db := setupMockDB(t)

	// SavePoint should return nil (no-op for Snowflake)
	err := dialector.SavePoint(db, "test_savepoint")
	if err != nil {
		t.Errorf("Expected SavePoint to return nil, got %v", err)
	}
}

// TestDialectorRollbackTo tests the RollbackTo method
func TestDialectorRollbackTo(t *testing.T) {
	dialector := New(Config{}).(*Dialector)
	db := setupMockDB(t)

	// RollbackTo should execute a ROLLBACK TRANSACTION command
	err := dialector.RollbackTo(db, "test_savepoint")
	if err != nil {
		t.Errorf("Expected RollbackTo to return nil, got %v", err)
	}
}

// TestDialectorMigrator tests the Migrator method
func TestDialectorMigrator(t *testing.T) {
	dialector := New(Config{})
	db := setupMockDB(t)

	migrator := dialector.Migrator(db)

	// Should return a Snowflake migrator
	_, ok := migrator.(Migrator)
	if !ok {
		t.Error("Expected migrator to be of type snowflake.Migrator")
	}
}

// TestNamingStrategy tests the NamingStrategy functionality
func TestNamingStrategy(t *testing.T) {
	ns := NewNamingStrategy()

	if ns == nil {
		t.Fatal("Expected NewNamingStrategy to return a non-nil instance")
	}

	if ns.defaultNS == nil {
		t.Error("Expected defaultNS to be initialized")
	}
}

func TestNamingStrategyColumnName(t *testing.T) {
	ns := NewNamingStrategy()

	tests := []struct {
		table    string
		column   string
		expected string
	}{
		{"users", "first_name", "first_name"},
		{"posts", "user_id", "user_id"},
		{"", "id", "id"},
	}

	for _, test := range tests {
		result := ns.ColumnName(test.table, test.column)
		if result != test.expected {
			t.Errorf("ColumnName(%s, %s): expected %s, got %s",
				test.table, test.column, test.expected, result)
		}
	}
}

func TestNamingStrategyTableName(t *testing.T) {
	ns := NewNamingStrategy()

	tests := []struct {
		table    string
		expected string
	}{
		{"User", "users"},
		{"UserPost", "user_posts"},
		{"", ""},
	}

	for _, test := range tests {
		result := ns.TableName(test.table)
		if result != test.expected {
			t.Errorf("TableName(%s): expected %s, got %s",
				test.table, test.expected, result)
		}
	}
}

func TestNamingStrategyJoinTableName(t *testing.T) {
	ns := NewNamingStrategy()

	tests := []struct {
		joinTable string
		expected  string
	}{
		{"UserRole", "user_roles"},
		{"PostTag", "post_tags"},
		{"", ""},
	}

	for _, test := range tests {
		result := ns.JoinTableName(test.joinTable)
		if result != test.expected {
			t.Errorf("JoinTableName(%s): expected %s, got %s",
				test.joinTable, test.expected, result)
		}
	}
}

func TestNamingStrategyRelationshipFKName(t *testing.T) {
	ns := NewNamingStrategy()

	// Create a test relationship with proper schema setup
	rel := schema.Relationship{
		Name:        "User",
		Field:       &schema.Field{Name: "UserID"},
		Schema:      &schema.Schema{Name: "Order"},
		FieldSchema: &schema.Schema{Name: "User"},
	}

	result := ns.RelationshipFKName(rel)
	if result == "" {
		t.Error("Expected RelationshipFKName to return a non-empty string")
	}
}

func TestNamingStrategyCheckerName(t *testing.T) {
	ns := NewNamingStrategy()

	tests := []struct {
		table    string
		column   string
		expected string
	}{
		{"users", "age", "chk_users_age"},
		{"posts", "status", "chk_posts_status"},
	}

	for _, test := range tests {
		result := ns.CheckerName(test.table, test.column)
		if result != test.expected {
			t.Errorf("CheckerName(%s, %s): expected %s, got %s",
				test.table, test.column, test.expected, result)
		}
	}
}

func TestNamingStrategyIndexName(t *testing.T) {
	ns := NewNamingStrategy()

	tests := []struct {
		table    string
		column   string
		expected string
	}{
		{"users", "email", "idx_users_email"},
		{"posts", "title", "idx_posts_title"},
	}

	for _, test := range tests {
		result := ns.IndexName(test.table, test.column)
		if result != test.expected {
			t.Errorf("IndexName(%s, %s): expected %s, got %s",
				test.table, test.column, test.expected, result)
		}
	}
}

// Helper types for testing
type mockClauseWriter struct {
	builder *strings.Builder
}

func (m *mockClauseWriter) WriteByte(b byte) error {
	return m.builder.WriteByte(b)
}

func (m *mockClauseWriter) WriteString(s string) (int, error) {
	return m.builder.WriteString(s)
}

func (m *mockClauseWriter) WriteQuoted(field interface{}) {
	switch v := field.(type) {
	case string:
		m.builder.WriteString(`"` + v + `"`)
	case clause.Column:
		m.builder.WriteString(`"` + v.Name + `"`)
	default:
		m.builder.WriteString(fmt.Sprintf("%v", v))
	}
}

func (m *mockClauseWriter) AddVar(writer clause.Writer, stmt *gorm.Statement, vars ...interface{}) {
	for range vars {
		m.builder.WriteString("?")
	}
}
