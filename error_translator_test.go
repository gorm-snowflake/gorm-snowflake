package snowflake

import (
	"errors"
	"testing"

	"github.com/snowflakedb/gosnowflake"
	"gorm.io/gorm"
)

func TestTranslate(t *testing.T) {
	dialector := &Dialector{Config: &Config{}}

	t.Run("nil error returns nil", func(t *testing.T) {
		err := dialector.Translate(nil)
		if err != nil {
			t.Errorf("Expected nil, got %v", err)
		}
	})

	t.Run("non-snowflake error returns original error", func(t *testing.T) {
		originalErr := errors.New("some random error")
		err := dialector.Translate(originalErr)
		if err != originalErr {
			t.Errorf("Expected original error, got %v", err)
		}
	})

	t.Run("duplicate key error is translated to ErrDuplicatedKey", func(t *testing.T) {
		sfErr := &gosnowflake.SnowflakeError{
			Number:  1062,
			Message: "Duplicate entry 'test' for key 'PRIMARY'",
		}
		err := dialector.Translate(sfErr)
		if !errors.Is(err, gorm.ErrDuplicatedKey) {
			t.Errorf("Expected ErrDuplicatedKey, got %v", err)
		}
	})

	t.Run("unique constraint violation is translated to ErrDuplicatedKey", func(t *testing.T) {
		sfErr := &gosnowflake.SnowflakeError{
			Number:  1062,
			Message: "Unique constraint violation on column 'email'",
		}
		err := dialector.Translate(sfErr)
		if !errors.Is(err, gorm.ErrDuplicatedKey) {
			t.Errorf("Expected ErrDuplicatedKey, got %v", err)
		}
	})

	t.Run("foreign key violation is translated to ErrForeignKeyViolated", func(t *testing.T) {
		sfErr := &gosnowflake.SnowflakeError{
			Number:  1451,
			Message: "Cannot delete or update a parent row: a foreign key constraint fails",
		}
		err := dialector.Translate(sfErr)
		if !errors.Is(err, gorm.ErrForeignKeyViolated) {
			t.Errorf("Expected ErrForeignKeyViolated, got %v", err)
		}
	})

	t.Run("check constraint violation is translated to ErrCheckConstraintViolated", func(t *testing.T) {
		sfErr := &gosnowflake.SnowflakeError{
			Number:  3819,
			Message: "Check constraint 'age_check' is violated",
		}
		err := dialector.Translate(sfErr)
		if !errors.Is(err, gorm.ErrCheckConstraintViolated) {
			t.Errorf("Expected ErrCheckConstraintViolated, got %v", err)
		}
	})

	t.Run("invalid value error is translated to ErrInvalidData", func(t *testing.T) {
		sfErr := &gosnowflake.SnowflakeError{
			Number:  100038,
			Message: "Invalid value provided for column",
		}
		err := dialector.Translate(sfErr)
		if !errors.Is(err, gorm.ErrInvalidData) {
			t.Errorf("Expected ErrInvalidData, got %v", err)
		}
	})

	t.Run("invalid data error is translated to ErrInvalidData", func(t *testing.T) {
		sfErr := &gosnowflake.SnowflakeError{
			Number:  100040,
			Message: "Invalid data type in expression",
		}
		err := dialector.Translate(sfErr)
		if !errors.Is(err, gorm.ErrInvalidData) {
			t.Errorf("Expected ErrInvalidData, got %v", err)
		}
	})

	t.Run("unrecognized snowflake error returns original error", func(t *testing.T) {
		sfErr := &gosnowflake.SnowflakeError{
			Number:  999999,
			Message: "Some other snowflake error",
		}
		err := dialector.Translate(sfErr)
		if err != sfErr {
			t.Errorf("Expected original snowflake error, got %v", err)
		}
	})
}
