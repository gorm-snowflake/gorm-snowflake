package snowflake

import (
	"strings"
	"testing"
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

	dialector := New(Config{})

	const expected = "excluded.test"

	dialector.QuoteTo(c, expected)

	if out != expected {
		t.Errorf("Expected %s got %s", expected, out)
	}
}

func teardown() {
	out = ""
}
