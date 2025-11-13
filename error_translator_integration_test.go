package snowflake

import (
	"testing"

	"gorm.io/gorm"
)

// TestErrorTranslatorInterface verifies that the Dialector implements the ErrorTranslator interface
// This ensures that when GORM's TranslateError option is enabled, no warning is produced
func TestErrorTranslatorInterface(t *testing.T) {
	dialector := &Dialector{Config: &Config{}}

	// Check that Dialector implements ErrorTranslator interface
	_, ok := interface{}(dialector).(gorm.ErrorTranslator)
	if !ok {
		t.Error("Dialector does not implement gorm.ErrorTranslator interface")
	}
}

// TestErrorTranslatorWithGormInstance tests that the error translator works when used with a GORM instance
func TestErrorTranslatorWithGormInstance(t *testing.T) {
	// Create a dialector with a mock connection
	dialector := &Dialector{
		Config: &Config{
			Conn: &mockConnPool{},
		},
	}

	// Open a GORM connection with TranslateError enabled (default)
	// This should not produce any warnings about missing ErrorTranslator
	db, err := gorm.Open(dialector, &gorm.Config{
		TranslateError: true, // Explicitly enable error translation
	})

	if err != nil {
		t.Fatalf("Failed to open GORM connection: %v", err)
	}

	if db == nil {
		t.Fatal("DB instance is nil")
	}

	// Verify that the dialector is properly recognized as implementing ErrorTranslator
	// by checking that it has the Translate method
	var _ gorm.ErrorTranslator = dialector // This will fail to compile if not implemented
}
