package snowflake

import (
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
	t.Cleanup(teardown)
	c := clauseBuilder{}

	dialector := New(Config{})

	dialector.QuoteTo(c, "TEST_FUNCTION1(test)")

	const expected = `TEST_FUNCTION1("test")`
	if out != expected {
		t.Errorf("Expected %s got %s", expected, out)
	}
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
