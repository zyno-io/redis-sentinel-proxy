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
)

// RESPReader reads RESP values from a buffered reader.
type RESPReader struct {
	r *bufio.Reader
}

func NewRESPReader(r io.Reader) *RESPReader {
	return &RESPReader{r: bufio.NewReader(r)}
}

func (r *RESPReader) ReadValue() (RESPValue, error) {
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
		return RESPValue{Type: typeByte, Str: line}, nil

	case TypeInteger:
		line, err := r.readLine()
		if err != nil {
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
		buf := make([]byte, length+2) // +2 for \r\n
		_, err = io.ReadFull(r.r, buf)
		if err != nil {
			return RESPValue{}, err
		}
		return RESPValue{Type: TypeBulkString, Str: string(buf[:length])}, nil

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
		arr := make([]RESPValue, count)
		for i := 0; i < count; i++ {
			arr[i], err = r.ReadValue()
			if err != nil {
				return RESPValue{}, err
			}
		}
		return RESPValue{Type: TypeArray, Array: arr}, nil

	default:
		// Handle inline commands (no type prefix) - read rest of line
		rest, err := r.readLine()
		if err != nil {
			return RESPValue{}, err
		}
		return RESPValue{Type: TypeSimpleString, Str: string(typeByte) + rest}, nil
	}
}

func (r *RESPReader) readLine() (string, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	// Strip \r\n
	if len(line) >= 2 && line[len(line)-2] == '\r' {
		return line[:len(line)-2], nil
	}
	if len(line) >= 1 {
		return line[:len(line)-1], nil
	}
	return line, nil
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
