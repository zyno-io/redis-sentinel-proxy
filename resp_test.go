package main

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestRESPReaderSimpleString(t *testing.T) {
	r := NewRESPReader(strings.NewReader("+OK\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeSimpleString {
		t.Fatalf("expected SimpleString, got %c", val.Type)
	}
	if val.Str != "OK" {
		t.Fatalf("expected OK, got %q", val.Str)
	}
}

func TestRESPReaderError(t *testing.T) {
	r := NewRESPReader(strings.NewReader("-ERR something wrong\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeError {
		t.Fatalf("expected Error, got %c", val.Type)
	}
	if val.Str != "ERR something wrong" {
		t.Fatalf("expected error message, got %q", val.Str)
	}
}

func TestRESPReaderInteger(t *testing.T) {
	r := NewRESPReader(strings.NewReader(":42\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeInteger {
		t.Fatalf("expected Integer, got %c", val.Type)
	}
	if val.Int != 42 {
		t.Fatalf("expected 42, got %d", val.Int)
	}
}

func TestRESPReaderNegativeInteger(t *testing.T) {
	r := NewRESPReader(strings.NewReader(":-1\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeInteger || val.Int != -1 {
		t.Fatalf("expected -1, got %d", val.Int)
	}
}

func TestRESPReaderBulkString(t *testing.T) {
	r := NewRESPReader(strings.NewReader("$5\r\nhello\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeBulkString {
		t.Fatalf("expected BulkString, got %c", val.Type)
	}
	if val.Str != "hello" {
		t.Fatalf("expected hello, got %q", val.Str)
	}
}

func TestRESPReaderNullBulkString(t *testing.T) {
	r := NewRESPReader(strings.NewReader("$-1\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeBulkString || !val.IsNil {
		t.Fatal("expected nil bulk string")
	}
}

func TestRESPReaderEmptyBulkString(t *testing.T) {
	r := NewRESPReader(strings.NewReader("$0\r\n\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeBulkString || val.Str != "" {
		t.Fatalf("expected empty string, got %q", val.Str)
	}
}

func TestRESPReaderArray(t *testing.T) {
	r := NewRESPReader(strings.NewReader("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || len(val.Array) != 2 {
		t.Fatalf("expected array of 2, got %d", len(val.Array))
	}
	if val.Array[0].Str != "foo" || val.Array[1].Str != "bar" {
		t.Fatalf("expected [foo, bar], got [%s, %s]", val.Array[0].Str, val.Array[1].Str)
	}
}

func TestRESPReaderNullArray(t *testing.T) {
	r := NewRESPReader(strings.NewReader("*-1\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || !val.IsNil {
		t.Fatal("expected nil array")
	}
}

func TestRESPReaderEmptyArray(t *testing.T) {
	r := NewRESPReader(strings.NewReader("*0\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || len(val.Array) != 0 {
		t.Fatalf("expected empty array, got %d elements", len(val.Array))
	}
}

func TestRESPReaderNestedArray(t *testing.T) {
	input := "*2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
	r := NewRESPReader(strings.NewReader(input))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || len(val.Array) != 2 {
		t.Fatal("expected outer array of 2")
	}
	inner := val.Array[0]
	if inner.Type != TypeArray || len(inner.Array) != 2 {
		t.Fatal("expected inner array of 2")
	}
	if inner.Array[0].Str != "a" || inner.Array[1].Str != "b" {
		t.Fatal("unexpected inner values")
	}
	if val.Array[1].Str != "c" {
		t.Fatal("unexpected second element")
	}
}

func TestRESPReaderInlineCommand(t *testing.T) {
	r := NewRESPReader(strings.NewReader("PING\r\n"))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Str != "PING" {
		t.Fatalf("expected PING, got %q", val.Str)
	}
}

func TestRESPReaderMultipleValues(t *testing.T) {
	input := "+OK\r\n:100\r\n"
	r := NewRESPReader(strings.NewReader(input))

	v1, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if v1.Str != "OK" {
		t.Fatalf("expected OK, got %q", v1.Str)
	}

	v2, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if v2.Int != 100 {
		t.Fatalf("expected 100, got %d", v2.Int)
	}
}

func TestRESPReaderEOF(t *testing.T) {
	r := NewRESPReader(strings.NewReader(""))
	_, err := r.ReadValue()
	if err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestRESPReaderInvalidInteger(t *testing.T) {
	r := NewRESPReader(strings.NewReader(":abc\r\n"))
	_, err := r.ReadValue()
	if err == nil {
		t.Fatal("expected error for invalid integer")
	}
}

func TestRESPWriterSimpleString(t *testing.T) {
	var buf bytes.Buffer
	w := NewRESPWriter(&buf)
	w.WriteSimpleString("OK")
	w.Flush()
	if buf.String() != "+OK\r\n" {
		t.Fatalf("expected +OK\\r\\n, got %q", buf.String())
	}
}

func TestRESPWriterError(t *testing.T) {
	var buf bytes.Buffer
	w := NewRESPWriter(&buf)
	w.WriteError("ERR bad")
	w.Flush()
	if buf.String() != "-ERR bad\r\n" {
		t.Fatalf("expected -ERR bad\\r\\n, got %q", buf.String())
	}
}

func TestRESPWriterInteger(t *testing.T) {
	var buf bytes.Buffer
	w := NewRESPWriter(&buf)
	w.WriteInteger(42)
	w.Flush()
	if buf.String() != ":42\r\n" {
		t.Fatalf("expected :42\\r\\n, got %q", buf.String())
	}
}

func TestRESPWriterBulkString(t *testing.T) {
	var buf bytes.Buffer
	w := NewRESPWriter(&buf)
	w.WriteBulkString("hello")
	w.Flush()
	if buf.String() != "$5\r\nhello\r\n" {
		t.Fatalf("expected $5\\r\\nhello\\r\\n, got %q", buf.String())
	}
}

func TestRESPWriterNullBulkString(t *testing.T) {
	var buf bytes.Buffer
	w := NewRESPWriter(&buf)
	w.WriteNullBulkString()
	w.Flush()
	if buf.String() != "$-1\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestRESPWriterArrayHeader(t *testing.T) {
	var buf bytes.Buffer
	w := NewRESPWriter(&buf)
	w.WriteArrayHeader(3)
	w.Flush()
	if buf.String() != "*3\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestRESPWriterNullArray(t *testing.T) {
	var buf bytes.Buffer
	w := NewRESPWriter(&buf)
	w.WriteNullArray()
	w.Flush()
	if buf.String() != "*-1\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestRESPWriterBulkStringArray(t *testing.T) {
	var buf bytes.Buffer
	w := NewRESPWriter(&buf)
	w.WriteBulkStringArray([]string{"foo", "bar"})
	w.Flush()
	expected := "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	if buf.String() != expected {
		t.Fatalf("expected %q, got %q", expected, buf.String())
	}
}

func TestRESPWriterEmptyBulkStringArray(t *testing.T) {
	var buf bytes.Buffer
	w := NewRESPWriter(&buf)
	w.WriteBulkStringArray([]string{})
	w.Flush()
	if buf.String() != "*0\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestRESPWriterRawMessage(t *testing.T) {
	var buf bytes.Buffer
	w := NewRESPWriter(&buf)
	w.WriteRawMessage([]byte("+PONG\r\n"))
	w.Flush()
	if buf.String() != "+PONG\r\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestRESPRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewRESPWriter(&buf)
	w.WriteArrayHeader(3)
	w.WriteBulkString("SET")
	w.WriteBulkString("key")
	w.WriteBulkString("value")
	w.Flush()

	r := NewRESPReader(&buf)
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || len(val.Array) != 3 {
		t.Fatal("expected array of 3")
	}
	args := val.ToArgs()
	if len(args) != 3 || args[0] != "SET" || args[1] != "key" || args[2] != "value" {
		t.Fatalf("unexpected args: %v", args)
	}
}

func TestToArgsArray(t *testing.T) {
	val := RESPValue{
		Type: TypeArray,
		Array: []RESPValue{
			{Type: TypeBulkString, Str: "SENTINEL"},
			{Type: TypeBulkString, Str: "get-master-addr-by-name"},
			{Type: TypeBulkString, Str: "mymaster"},
		},
	}
	args := val.ToArgs()
	if len(args) != 3 || args[0] != "SENTINEL" || args[2] != "mymaster" {
		t.Fatalf("unexpected args: %v", args)
	}
}

func TestToArgsInline(t *testing.T) {
	val := RESPValue{Type: TypeSimpleString, Str: "SENTINEL get-master-addr-by-name mymaster"}
	args := val.ToArgs()
	if len(args) != 3 || args[0] != "SENTINEL" || args[2] != "mymaster" {
		t.Fatalf("unexpected args: %v", args)
	}
}

func TestSplitInline(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"PING", []string{"PING"}},
		{"SET key value", []string{"SET", "key", "value"}},
		{`SET key "hello world"`, []string{"SET", "key", "hello world"}},
		{"SET key 'hello world'", []string{"SET", "key", "hello world"}},
		{"  spaces  between  ", []string{"spaces", "between"}},
		{"", nil},
		{"SET\tkey\tvalue", []string{"SET", "key", "value"}},
	}

	for _, tt := range tests {
		got := splitInline(tt.input)
		if len(got) != len(tt.expected) {
			t.Errorf("splitInline(%q): got %v, want %v", tt.input, got, tt.expected)
			continue
		}
		for i := range got {
			if got[i] != tt.expected[i] {
				t.Errorf("splitInline(%q)[%d]: got %q, want %q", tt.input, i, got[i], tt.expected[i])
			}
		}
	}
}

func TestRESPReaderBulkStringWithBinaryData(t *testing.T) {
	// Bulk string can contain \r\n within the data
	data := "he\r\nllo"
	input := "$7\r\nhe\r\nllo\r\n"
	r := NewRESPReader(strings.NewReader(input))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Str != data {
		t.Fatalf("expected %q, got %q", data, val.Str)
	}
}

// --- Issue #1: negative lengths must not panic ---

func TestRESPReaderNegativeBulkStringLength(t *testing.T) {
	tests := []string{"$-2\r\n", "$-3\r\n", "$-100\r\n"}
	for _, input := range tests {
		r := NewRESPReader(strings.NewReader(input))
		_, err := r.ReadValue()
		if err == nil {
			t.Fatalf("expected error for %q, got nil", input)
		}
	}
}

func TestRESPReaderNegativeArrayLength(t *testing.T) {
	tests := []string{"*-2\r\n", "*-3\r\n", "*-100\r\n"}
	for _, input := range tests {
		r := NewRESPReader(strings.NewReader(input))
		_, err := r.ReadValue()
		if err == nil {
			t.Fatalf("expected error for %q, got nil", input)
		}
	}
}

func TestRESPReaderHugeBulkStringLength(t *testing.T) {
	r := NewRESPReader(strings.NewReader("$999999999999\r\n"))
	_, err := r.ReadValue()
	if err == nil {
		t.Fatal("expected error for huge bulk string length")
	}
}

func TestRESPReaderHugeArrayLength(t *testing.T) {
	r := NewRESPReader(strings.NewReader("*999999999999\r\n"))
	_, err := r.ReadValue()
	if err == nil {
		t.Fatal("expected error for huge array length")
	}
}

// --- Issue #10: bulk string framing validation ---

func TestRESPReaderLineTooLong(t *testing.T) {
	// Build a simple string line that exceeds maxLineLength (64KB)
	long := "+" + strings.Repeat("x", 65*1024) + "\r\n"
	r := NewRESPReader(strings.NewReader(long))
	_, err := r.ReadValue()
	if err == nil {
		t.Fatal("expected error for oversized line")
	}
	if !strings.Contains(err.Error(), "line length exceeds maximum") {
		t.Fatalf("expected line length error, got: %v", err)
	}
}

func TestRESPReaderBulkStringExceedsLimit(t *testing.T) {
	// Bulk string exceeding 64 KiB limit should fail
	r := NewRESPReader(strings.NewReader("$65537\r\n"))
	_, err := r.ReadValue()
	if err == nil {
		t.Fatal("expected error for bulk string exceeding 64 KiB")
	}
	if !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected exceeds maximum error, got: %v", err)
	}
}

func TestRESPReaderBulkStringBadTerminator(t *testing.T) {
	// 5 bytes of data but wrong terminator
	r := NewRESPReader(strings.NewReader("$5\r\nhelloXX"))
	_, err := r.ReadValue()
	if err == nil {
		t.Fatal("expected error for bad bulk string terminator")
	}
}

func TestRESPReaderDeepNestingRejected(t *testing.T) {
	// Build a deeply nested array: *1\r\n repeated 33+ times then +OK\r\n
	var input string
	for i := 0; i < 34; i++ {
		input += "*1\r\n"
	}
	input += "+OK\r\n"

	r := NewRESPReader(strings.NewReader(input))
	_, err := r.ReadValue()
	if err == nil {
		t.Fatal("expected error for deep nesting")
	}
	if !strings.Contains(err.Error(), "nesting depth") {
		t.Fatalf("expected nesting depth error, got: %v", err)
	}
}

func TestRESPReaderNestingWithinLimit(t *testing.T) {
	// 3 levels of nesting should be fine
	input := "*1\r\n*1\r\n*1\r\n+OK\r\n"
	r := NewRESPReader(strings.NewReader(input))
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || len(val.Array) != 1 {
		t.Fatal("expected outer array of 1")
	}
}

func TestRESPReaderBareLFRejected(t *testing.T) {
	// Bare \n without \r should be rejected
	r := NewRESPReader(strings.NewReader("+OK\n"))
	_, err := r.ReadValue()
	if err == nil {
		t.Fatal("expected error for bare \\n")
	}
	if !strings.Contains(err.Error(), "bare \\n") {
		t.Fatalf("expected bare \\n error, got: %v", err)
	}
}

func TestRESPReaderBareLFInBulkStringLength(t *testing.T) {
	r := NewRESPReader(strings.NewReader("$5\nhello\r\n"))
	_, err := r.ReadValue()
	if err == nil {
		t.Fatal("expected error for bare \\n in bulk string length")
	}
}

func TestRESPReaderBareLFInInlineCommand(t *testing.T) {
	r := NewRESPReader(strings.NewReader("PING\n"))
	_, err := r.ReadValue()
	if err == nil {
		t.Fatal("expected error for bare \\n in inline command")
	}
}
