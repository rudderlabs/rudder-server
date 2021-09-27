package clickhouse

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

const (
	eof = rune(0)
)

type token struct {
	kind rune
	data string
}

func skipWhiteSpace(s io.RuneScanner) {
	for {
		r := read(s)
		switch r {
		case ' ', '\t', '\n':
			continue
		case eof:
			return
		}
		_ = s.UnreadRune()
		return
	}
}

func read(s io.RuneScanner) rune {
	r, _, err := s.ReadRune()
	if err != nil {
		return eof
	}
	return r
}

func readEscaped(s io.RuneScanner) (rune, error) {
	r := read(s)
	switch r {
	case eof:
		return 0, fmt.Errorf("unexpected eof in escaped char")
	case 'b':
		return '\b', nil
	case 'f':
		return '\f', nil
	case 'r':
		return '\r', nil
	case 'n':
		return '\n', nil
	case 't':
		return '\t', nil
	case '0':
		return '\x00', nil
	default:
		return r, nil
	}
}

func readRaw(s io.RuneScanner) *bytes.Buffer {
	var data bytes.Buffer

	for {
		r := read(s)

		if r == eof {
			break
		}

		data.WriteRune(r)
	}

	return &data
}

func readQuoted(s io.RuneScanner) (*token, error) {
	var data bytes.Buffer

loop:
	for {
		r := read(s)

		switch r {
		case eof:
			return nil, fmt.Errorf("unexpected eof inside quoted string")
		case '\\':
			escaped, err := readEscaped(s)
			if err != nil {
				return nil, fmt.Errorf("incorrect escaping in quoted string: %v", err)
			}
			r = escaped
		case '\'':
			break loop
		}

		data.WriteRune(r)
	}

	return &token{'q', data.String()}, nil
}

func readNumberOrID(s io.RuneScanner) *token {
	var data bytes.Buffer

loop:
	for {
		r := read(s)

		switch r {
		case eof, ' ', '\t', '\n':
			break loop
		case '(', ')', ',':
			_ = s.UnreadRune()
			break loop
		default:
			data.WriteRune(r)
		}
	}

	return &token{'s', data.String()}
}

func tokenize(s io.RuneScanner) ([]*token, error) {
	var tokens []*token

loop:
	for {
		var t *token
		var err error

		switch read(s) {
		case eof:
			break loop
		case ' ', '\t', '\n':
			skipWhiteSpace(s)
			continue
		case '(':
			t = &token{kind: '('}
		case ')':
			t = &token{kind: ')'}
		case ',':
			t = &token{kind: ','}
		case '\'':
			t, err = readQuoted(s)
			if err != nil {
				return nil, err
			}
		default:
			_ = s.UnreadRune()
			t = readNumberOrID(s)
		}

		tokens = append(tokens, t)
	}

	tokens = append(tokens, &token{kind: eof})
	return tokens, nil
}

// tokenizeString splits a string into tokens according to ClickHouse
// formatting rules as per https://clickhouse.yandex/docs/en/interfaces/formats/#data-formatting
func tokenizeString(s string) ([]*token, error) {
	return tokenize(strings.NewReader(s))
}
