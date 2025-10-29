# GORM Snowflake Driver - Test Coverage Report

## Overview

This document provides a comprehensive overview of the test suite created for the GORM Snowflake driver. The test coverage achieves **64.3%** of statements across all functionality.

## Test Files Created

### 1. `snowflake_test.go` - Core Dialector Tests

Tests the main functionality of the Snowflake dialector and naming strategy.

#### Dialector Tests

- **TestDialectorName**: Verifies the dialector name is "snowflake"
- **TestDialectorOpen**: Tests database connection opening
- **TestDialectorNew**: Tests dialector creation
- **TestDialectorInitialize**: Tests database initialization
- **TestDialectorClauseBuilders**: Tests clause builders registration
- **TestDialectorDefaultValueOf**: Tests default value handling
- **TestDialectorBindVarTo**: Tests variable binding
- **TestDialectorExplain**: Tests SQL explanation functionality
- **TestDialectorDataTypeOf**: Tests data type mapping for various Go types
- **TestDialectorSavePoint**: Tests savepoint functionality (no-op in Snowflake)
- **TestDialectorRollbackTo**: Tests rollback functionality
- **TestDialectorMigrator**: Tests migrator creation

#### Batch Insert Tests

- **TestBatchInsert**: Tests UNION SELECT batch insertion
- **TestBatchInsertMethods**: Tests both VALUES and UNION SELECT methods
- **TestBatchInsertWithConflict**: Tests merge operations with conflicts

#### Quote Tests

- **TestQuoteToFunction**: Tests identifier quoting functionality
- Note: EXCLUDED transformation is handled in `prepareOnConflictForMerge()`, not `QuoteTo()`. See create_test.go for integration and unit tests.

#### Naming Strategy Tests

- **TestNamingStrategy**: Tests naming strategy creation
- **TestNamingStrategyColumnName**: Tests column name transformation
- **TestNamingStrategyTableName**: Tests table name transformation
- **TestNamingStrategyJoinTableName**: Tests join table naming
- **TestNamingStrategyRelationshipFKName**: Tests foreign key naming
- **TestNamingStrategyCheckerName**: Tests check constraint naming
- **TestNamingStrategyIndexName**: Tests index naming

### 2. `create_test.go` - Create Operations Tests

Tests the CREATE functionality and SQL generation utilities.

#### Core Create Tests

- **TestCreateEdgeCases**: Tests create operations with various edge cases
  - Existing SQL handling
  - Unscoped operations
  - Schema create clauses

#### SQL Generation Tests

- **TestShouldUseUnionSelect**: Tests logic for choosing UNION SELECT vs VALUES
- **TestBuildUnionSelectInsert**: Tests UNION SELECT SQL generation
- **TestBuildValuesInsert**: Tests VALUES SQL generation
- **TestMergeCreateComplex**: Tests complex merge operations
- **TestCreateWithFieldsWithDefaultDBValue**: Tests default value handling
- **TestCreateConflictDetection**: Tests conflict detection logic
- **TestVariablePreallocation**: Tests variable slice optimization

#### EXCLUDED Handling Tests

- **TestGORMSaveExcludedQuotingBug**: Tests EXCLUDED keyword handling in MERGE statements
  - Tests QuoteTo with various column inputs
  - Tests production pattern: WriteString("EXCLUDED.") + WriteQuoted(column)
  - Tests manual OnConflict with user-provided `excluded.column` syntax
  - Tests `prepareOnConflictForMerge()` transformation: `excluded.X` → `EXCLUDED."X"` (case-insensitive)
  - Verifies EXCLUDED references in DoUpdates clauses
  - Ensures EXCLUDED table name is never quoted
  - Validates end-to-end SQL generation with manual clause construction

### 3. `migrator_test.go` - Migration Operations Tests

Tests database migration functionality.

#### Basic Migration Tests

- **TestMigratorHasTable**: Tests table existence checking
- **TestMigratorHasColumn**: Tests column existence checking
- **TestMigratorRenameColumn**: Tests column renaming
- **TestMigratorIndexOperations**: Tests index operations (create, drop, rename)
- **TestMigratorHasConstraint**: Tests constraint checking
- **TestMigratorCurrentDatabase**: Tests current database retrieval

#### Data Type Tests

- **TestMigratorDialectorDataType**: Tests data type mapping through migrator

#### Advanced Migration Tests

- **TestMigratorSQL**: Tests migrator structure and type assertions
- **TestBuildConstraint**: Tests constraint SQL building
- **TestMigratorGuessConstraintAndTable**: Tests constraint name inference
- **TestMigratorCreateTableSQL**: Tests table creation SQL
- **TestMigratorRenameTable**: Tests table renaming
- **TestMigratorDropTable**: Tests table dropping
- **TestMigratorAlterColumn**: Tests column alteration
- **TestMigratorCreateConstraint**: Tests constraint creation
- **TestMigratorDropConstraint**: Tests constraint dropping

## Test Models and Utilities

### Test Models Created

- `TestUser`: Basic user model for general testing
- `TestModel`: Model with various field types
- `TestModelWithID`: Model with explicit ID field
- `AutoIncrementOnly`: Model with only auto-increment field
- `TestModelWithDefault`: Model with default values
- `TestUserRelation`: Model for relationship testing

### Mock Infrastructure

- `MockConnectionPool`: Custom connection pool for testing
- `MockDriver`: Driver implementation for isolated testing
- `setupMockDB()`: Helper function for test database setup
- `getMockDB()`: Helper for getting mock database instances

## Key Features Tested

### 1. Data Type Mapping

- String types (VARCHAR, TEXT)
- Numeric types (INT, BIGINT, SMALLINT, FLOAT)
- Boolean types
- Time types (TIMESTAMP_NTZ)
- Binary types (VARBINARY)
- Auto-increment fields with IDENTITY

### 2. SQL Generation

- INSERT with VALUES syntax
- INSERT with UNION SELECT syntax
- MERGE operations for upserts
- Conflict resolution
- Default value handling

### 3. Migration Operations

- Table management (create, drop, rename)
- Column management (add, alter, rename)
- Index management (create, drop, rename)
- Constraint management

### 4. Naming Strategies

- Table name transformation
- Column name transformation
- Index name generation
- Foreign key name generation
- Constraint name generation

### 5. EXCLUDED Handling in MERGE Statements

- EXCLUDED keyword handling in ON CONFLICT clauses
- Production pattern: WriteString("EXCLUDED.") + WriteQuoted(column)
- User-provided `excluded.column` transformation in `prepareOnConflictForMerge()`
- Pre-formatted EXCLUDED references via clause.Expr
- Verification that EXCLUDED table name is never quoted
- Case-insensitive transformation: all variants → `EXCLUDED."column"`

### 6. Edge Cases

- Empty value insertion
- Auto-increment only models
- Unscoped operations
- Default value fields
- Complex merge scenarios

## Coverage Analysis

The test suite achieves **64.3% statement coverage**, covering:

- ✅ Core dialector interface methods
- ✅ Data type mapping logic
- ✅ SQL clause building
- ✅ Create operations and merge logic
- ✅ EXCLUDED keyword handling in MERGE statements
- ✅ Migration operations
- ✅ Naming strategy transformations
- ✅ Quote handling and identifier escaping
- ✅ Batch insertion strategies
- ✅ Error handling and edge cases

## Recent Improvements

### EXCLUDED Handling Implementation

The codebase properly handles EXCLUDED keyword references in MERGE statements:

#### Production Usage (Normal Path)

In `create.go` within `MergeCreate()`, EXCLUDED is handled in two ways:

- **Table name written directly**: `WriteString(") AS EXCLUDED (")`
- **Column references**: `WriteString("EXCLUDED.")` + `WriteQuoted(columnName)`

These paths write EXCLUDED directly and never pass "excluded.column" to `QuoteTo()`.

#### Edge Case Handling (prepareOnConflictForMerge)

However, **users can manually construct** `clause.Column` with names like `"excluded.ACCESS_TOKEN"`:

```go
DoUpdates: []clause.Assignment{
    {
        Column: clause.Column{Name: "ACCESS_TOKEN"},
        Value:  clause.Column{Name: "excluded.ACCESS_TOKEN"}, // User-provided!
    },
}
```

For this edge case, `prepareOnConflictForMerge()` includes special handling:

- **Detection**: Case-insensitive check for `strings.HasPrefix(colNameLower, "excluded.")`
- **Extraction**: Extracts column name after `"excluded."` prefix
- **Transformation**: `"excluded.ACCESS_TOKEN"` → `clause.Expr{SQL: \`EXCLUDED."ACCESS_TOKEN"\`}`
- **Result**: All EXCLUDED transformation happens in one place during MERGE preparation

This centralized approach ensures backward compatibility and correct handling of user-provided EXCLUDED references without polluting the general-purpose `QuoteTo()` method.

## Running Tests

```bash
# Run all tests
go test -v

# Run tests with coverage
go test -cover

# Run specific test files
go test -v -run TestDialector
go test -v -run TestMigrator
go test -v -run TestCreate
```

## Test Quality Features

- **Table-driven tests**: Most tests use table-driven approach for comprehensive coverage
- **Subtests**: Complex scenarios broken down into focused subtests
- **Mock infrastructure**: Isolated testing without requiring actual database connections
- **Edge case coverage**: Tests handle error conditions and boundary cases
- **Performance testing**: Variable preallocation and optimization verification
- **SQL validation**: Generated SQL is validated for correctness

This comprehensive test suite ensures the GORM Snowflake driver functions correctly across all supported operations and provides confidence for future development and maintenance.
