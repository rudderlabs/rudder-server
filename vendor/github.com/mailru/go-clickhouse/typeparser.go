package clickhouse

import (
	"fmt"
)

// TypeDesc describes a (possibly nested) data type returned by ClickHouse.
type TypeDesc struct {
	Name string
	Args []*TypeDesc
}

func parseTypeDesc(tokens []*token) (*TypeDesc, []*token, error) {
	var name string
	if tokens[0].kind == 's' || tokens[0].kind == 'q' {
		name = tokens[0].data
		tokens = tokens[1:]
	} else {
		return nil, nil, fmt.Errorf("failed to parse type name: wrong token type '%c'", tokens[0].kind)
	}

	desc := TypeDesc{Name: name}
	if tokens[0].kind != '(' {
		return &desc, tokens, nil
	}

	tokens = tokens[1:]

	if tokens[0].kind == ')' {
		return &desc, tokens[1:], nil
	}

	if name == "Enum8" || name == "Enum16" {
		// TODO: an Enum's arguments get completely ignored
		for i := range tokens {
			if tokens[i].kind == ')' {
				return &desc, tokens[i+1:], nil
			}
		}
		return nil, nil, fmt.Errorf("unfinished enum type description")
	}

	for {
		var arg *TypeDesc
		var err error

		arg, tokens, err = parseTypeDesc(tokens)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse subtype: %v", err)
		}
		desc.Args = append(desc.Args, arg)

		switch tokens[0].kind {
		case ',':
			tokens = tokens[1:]
			continue
		case ')':
			return &desc, tokens[1:], nil
		}
	}
}

// ParseTypeDesc parses the type description that ClickHouse provides.
//
// The grammar is quite simple:
//     desc
//         name
//         name()
//         name(args)
//     args
//         desc
//         desc, args
//
// Examples:
//     String
//     Nullable(Nothing)
//     Array(Tuple(Tuple(String, String), Tuple(String, UInt64)))
func ParseTypeDesc(s string) (*TypeDesc, error) {
	tokens, err := tokenizeString(s)
	if err != nil {
		return nil, fmt.Errorf("failed to tokenize type description: %v", err)
	}

	desc, tail, err := parseTypeDesc(tokens)
	if err != nil {
		return nil, fmt.Errorf("failed to parse type description: %v", err)
	}

	if len(tail) != 1 || tail[0].kind != eof {
		return nil, fmt.Errorf("unexpected tail after type description")
	}

	return desc, nil
}
