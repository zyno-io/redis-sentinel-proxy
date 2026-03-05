package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// RESP value types
type RESPValue struct {
	Type  byte
	Str   string
	Int   int64
	Array []RESPValue
	IsNil bool
}

const (
	TypeSimpleString = '+'
	TypeError        = '-'
	TypeInteger      = ':'
	TypeBulkString   = '$'
	TypeArray        = '*'

	// Sentinel/ioredis control-plane traffic only; keep limits tight.
	maxBulkStringLength = 64 * 1024 // 64 KiB
	maxArrayLength      = 1024      // plenty for ioredis command vectors
	maxLineLength       = 64 * 1024 // 64 KiB
	maxNestingDepth     = 32

	maxRESPValueBytes  = 1 * 1024 * 1024 // aggregate payload budget per parsed value
	maxInitialArrayCap = 16              // avoid trusting declared count for upfront alloc
)

// RESPReader reads RESP values from a buffered reader.
type RESPReader struct {
	r *bufio.Reader
}

func NewRESPReader(r io.Reader) *RESPReader {
	return &RESPReader{r: bufio.NewReader(r)}
}

func (r *RESPReader) ReadValue() (RESPValue, error) {
	remaining := int64(maxRESPValueBytes)
	return r.readValue(0, &remaining)
}

func (r *RESPReader) readValue(depth int, remaining *int64) (RESPValue, error) {
	if depth > maxNestingDepth {
		return RESPValue{}, fmt.Errorf("nesting depth exceeds maximum %d", maxNestingDepth)
	}

	typeByte, err := r.r.ReadByte()
	if err != nil {
		return RESPValue{}, err
	}

	switch typeByte {
	case TypeSimpleString, TypeError:
		line, err := r.readLine()
		if err != nil {
			return RESPValue{}, err
		}
		if err := consumeRESPBytes(remaining, len(line)); err != nil {
			return RESPValue{}, err
		}
		return RESPValue{Type: typeByte, Str: line}, nil

	case TypeInteger:
		line, err := r.readLine()
		if err != nil {
			return RESPValue{}, err
		}
		if err := consumeRESPBytes(remaining, len(line)); err != nil {
			return RESPValue{}, err
		}
		n, err := strconv.ParseInt(line, 10, 64)
		if err != nil {
			return RESPValue{}, fmt.Errorf("invalid integer: %s", line)
		}
		return RESPValue{Type: TypeInteger, Int: n}, nil

	case TypeBulkString:
		line, err := r.readLine()
		if err != nil {
			return RESPValue{}, err
		}
		length, err := strconv.Atoi(line)
		if err != nil {
			return RESPValue{}, fmt.Errorf("invalid bulk string length: %s", line)
		}
		if length == -1 {
			return RESPValue{Type: TypeBulkString, IsNil: true}, nil
		}
		if length < 0 {
			return RESPValue{}, fmt.Errorf("invalid bulk string length: %d", length)
		}
		if length > maxBulkStringLength {
			return RESPValue{}, fmt.Errorf("bulk string length %d exceeds maximum %d", length, maxBulkStringLength)
		}
		if err := consumeRESPBytes(remaining, length); err != nil {
			return RESPValue{}, err
		}
		data := make([]byte, length)
		if _, err = io.ReadFull(r.r, data); err != nil {
			return RESPValue{}, err
		}
		if err := r.readCRLF(); err != nil {
			return RESPValue{}, err
		}
		return RESPValue{Type: TypeBulkString, Str: string(data)}, nil

	case TypeArray:
		line, err := r.readLine()
		if err != nil {
			return RESPValue{}, err
		}
		count, err := strconv.Atoi(line)
		if err != nil {
			return RESPValue{}, fmt.Errorf("invalid array length: %s", line)
		}
		if count == -1 {
			return RESPValue{Type: TypeArray, IsNil: true}, nil
		}
		if count < 0 {
			return RESPValue{}, fmt.Errorf("invalid array length: %d", count)
		}
		if count > maxArrayLength {
			return RESPValue{}, fmt.Errorf("array length %d exceeds maximum %d", count, maxArrayLength)
		}
		initialCap := count
		if initialCap > maxInitialArrayCap {
			initialCap = maxInitialArrayCap
		}
		arr := make([]RESPValue, 0, initialCap)
		for i := 0; i < count; i++ {
			elem, err := r.readValue(depth+1, remaining)
			if err != nil {
				return RESPValue{}, err
			}
			arr = append(arr, elem)
		}
		return RESPValue{Type: TypeArray, Array: arr}, nil

	default:
		// Handle inline commands (no type prefix) - read rest of line
		rest, err := r.readLine()
		if err != nil {
			return RESPValue{}, err
		}
		if err := consumeRESPBytes(remaining, 1+len(rest)); err != nil {
			return RESPValue{}, err
		}
		return RESPValue{Type: TypeSimpleString, Str: string(typeByte) + rest}, nil
	}
}

func (r *RESPReader) readLine() (string, error) {
	var buf []byte
	for {
		b, err := r.r.ReadByte()
		if err != nil {
			return "", err
		}
		if b == '\n' {
			if len(buf) == 0 || buf[len(buf)-1] != '\r' {
				return "", fmt.Errorf("expected \\r\\n line terminator, got bare \\n")
			}
			return string(buf[:len(buf)-1]), nil
		}
		buf = append(buf, b)
		if len(buf) > maxLineLength {
			return "", fmt.Errorf("line length exceeds maximum %d", maxLineLength)
		}
	}
}

func (r *RESPReader) readCRLF() error {
	cr, err := r.r.ReadByte()
	if err != nil {
		return err
	}
	lf, err := r.r.ReadByte()
	if err != nil {
		return err
	}
	if cr != '\r' || lf != '\n' {
		return fmt.Errorf("invalid bulk string terminator")
	}
	return nil
}

func consumeRESPBytes(remaining *int64, n int) error {
	if n < 0 {
		return fmt.Errorf("invalid RESP payload size: %d", n)
	}
	if int64(n) > *remaining {
		return fmt.Errorf("RESP value exceeds maximum %d bytes", maxRESPValueBytes)
	}
	*remaining -= int64(n)
	return nil
}

// ToArgs converts an array RESP value (or inline command) into a slice of strings.
func (v RESPValue) ToArgs() []string {
	if v.Type == TypeArray {
		args := make([]string, len(v.Array))
		for i, a := range v.Array {
			args[i] = a.Str
		}
		return args
	}
	// Inline command - already in Str as space-separated
	return splitInline(v.Str)
}

func splitInline(s string) []string {
	var args []string
	var current []byte
	inQuote := false
	quoteChar := byte(0)

	for i := 0; i < len(s); i++ {
		c := s[i]
		if inQuote {
			if c == quoteChar {
				inQuote = false
			} else {
				current = append(current, c)
			}
		} else if c == '"' || c == '\'' {
			inQuote = true
			quoteChar = c
		} else if c == ' ' || c == '\t' {
			if len(current) > 0 {
				args = append(args, string(current))
				current = current[:0]
			}
		} else {
			current = append(current, c)
		}
	}
	if len(current) > 0 {
		args = append(args, string(current))
	}
	return args
}

// RESPWriter writes RESP values to a writer.
type RESPWriter struct {
	w *bufio.Writer
}

func NewRESPWriter(w io.Writer) *RESPWriter {
	return &RESPWriter{w: bufio.NewWriter(w)}
}

func (w *RESPWriter) Flush() error {
	return w.w.Flush()
}

func (w *RESPWriter) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.w, "+%s\r\n", s)
	return err
}

func (w *RESPWriter) WriteError(s string) error {
	_, err := fmt.Fprintf(w.w, "-%s\r\n", s)
	return err
}

func (w *RESPWriter) WriteInteger(n int64) error {
	_, err := fmt.Fprintf(w.w, ":%d\r\n", n)
	return err
}

func (w *RESPWriter) WriteBulkString(s string) error {
	_, err := fmt.Fprintf(w.w, "$%d\r\n%s\r\n", len(s), s)
	return err
}

func (w *RESPWriter) WriteNullBulkString() error {
	_, err := fmt.Fprintf(w.w, "$-1\r\n")
	return err
}

func (w *RESPWriter) WriteArrayHeader(n int) error {
	_, err := fmt.Fprintf(w.w, "*%d\r\n", n)
	return err
}

func (w *RESPWriter) WriteNullArray() error {
	_, err := fmt.Fprintf(w.w, "*-1\r\n")
	return err
}

func (w *RESPWriter) WriteBulkStringArray(items []string) error {
	if err := w.WriteArrayHeader(len(items)); err != nil {
		return err
	}
	for _, item := range items {
		if err := w.WriteBulkString(item); err != nil {
			return err
		}
	}
	return nil
}

// WriteRawMessage writes a pre-formatted RESP message.
func (w *RESPWriter) WriteRawMessage(msg []byte) error {
	_, err := w.w.Write(msg)
	return err
}
