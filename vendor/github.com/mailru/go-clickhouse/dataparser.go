package clickhouse

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"
)

var (
	reflectTypeString      = reflect.TypeOf("")
	reflectTypeTime        = reflect.TypeOf(time.Time{})
	reflectTypeEmptyStruct = reflect.TypeOf(struct{}{})
	reflectTypeInt8        = reflect.TypeOf(int8(0))
	reflectTypeInt16       = reflect.TypeOf(int16(0))
	reflectTypeInt32       = reflect.TypeOf(int32(0))
	reflectTypeInt64       = reflect.TypeOf(int64(0))
	reflectTypeUInt8       = reflect.TypeOf(uint8(0))
	reflectTypeUInt16      = reflect.TypeOf(uint16(0))
	reflectTypeUInt32      = reflect.TypeOf(uint32(0))
	reflectTypeUInt64      = reflect.TypeOf(uint64(0))
	reflectTypeFloat32     = reflect.TypeOf(float32(0))
	reflectTypeFloat64     = reflect.TypeOf(float64(0))
)

func readNumber(s io.RuneScanner) (string, error) {
	var builder bytes.Buffer

loop:
	for {
		r := read(s)

		switch r {
		case eof:
			break loop
		case ',', ']', ')':
			_ = s.UnreadRune()
			break loop
		}

		builder.WriteRune(r)
	}

	return builder.String(), nil
}

func readUnquoted(s io.RuneScanner, length int) (string, error) {
	var builder bytes.Buffer

	runesRead := 0
loop:
	for length == 0 || runesRead < length {
		r := read(s)

		switch r {
		case eof:
			break loop
		case '\\':
			escaped, err := readEscaped(s)
			if err != nil {
				return "", fmt.Errorf("incorrect escaping in string: %v", err)
			}
			r = escaped
		case '\'':
			_ = s.UnreadRune()
			break loop
		}

		builder.WriteRune(r)
		runesRead++
	}

	if length != 0 && runesRead != length {
		return "", fmt.Errorf("unexpected string length %d, expected %d", runesRead, length)
	}

	return builder.String(), nil
}

func readString(s io.RuneScanner, length int, unquote bool) (string, error) {
	if unquote {
		if r := read(s); r != '\'' {
			return "", fmt.Errorf("unexpected character instead of a quote")
		}
	}

	str, err := readUnquoted(s, length)
	if err != nil {
		return "", fmt.Errorf("failed to read string")
	}

	if unquote {
		if r := read(s); r != '\'' {
			return "", fmt.Errorf("unexpected character instead of a quote")
		}
	}

	return str, nil
}

// DataParser implements parsing of a driver value and reporting its type.
type DataParser interface {
	Parse(io.RuneScanner) (driver.Value, error)
	Type() reflect.Type
}

type nullableParser struct {
	DataParser
}

func (p *nullableParser) Parse(s io.RuneScanner) (driver.Value, error) {
	// Clickhouse returns `\N` string for `null` in tsv format.
	// For checking this value we need to check first two runes in `io.RuneScanner`, but we can't reset `io.RuneScanner` after it.
	// Copy io.RuneScanner to `bytes.Buffer` and use it twice (1st for casting to string and checking to null, 2nd for passing to original parser)
	var dB *bytes.Buffer

	dType := p.DataParser.Type()

	switch dType {
	case reflectTypeInt8, reflectTypeInt16, reflectTypeInt32, reflectTypeInt64,
		reflectTypeUInt8, reflectTypeUInt16, reflectTypeUInt32, reflectTypeUInt64,
		reflectTypeFloat32, reflectTypeFloat64:
		d, err := readNumber(s)
		if err != nil {
			return nil, fmt.Errorf("error: %v", err)
		}

		dB = bytes.NewBufferString(d)
	case reflectTypeString:
		runes := ""
		iter := 0

		isNotString := false
		for {
			r, size, err := s.ReadRune()
			if err == io.EOF && size == 0 { 
				break
			}
			
			if err != nil {
				return nil, fmt.Errorf("error: %v", err)
			}

			if r != '\'' && iter == 0 {
				err = s.UnreadRune()
				if err != nil {
					return nil, fmt.Errorf("error: %v", err)
				}
				d := readRaw(s)
				dB = d
				isNotString = true
				break
			}

			isEscaped := false
			if r == '\\' {
				escaped, err := readEscaped(s)
				if err != nil {
					return "", fmt.Errorf("incorrect escaping in string: %v", err)
				}

				isEscaped = true
				r = escaped
				if r == '\'' {
					runes += string('\\')
				}
			}

			runes += string(r)

			if r == '\'' && iter != 0 && !isEscaped {
				break
			}
			iter++
		}

		if bytes.Equal([]byte(runes), []byte(`'N'`)) {
			return nil, nil
		}

		if !isNotString {
			dB = bytes.NewBufferString(runes)
		}
	case reflectTypeTime:
		runes := ""

		iter := 0
		for {
			r, _, err := s.ReadRune()
			if err != nil {
				if err != io.EOF {
					return nil, fmt.Errorf("unexpected error on ReadRune: %v", err)
				}
				break
			}

			runes += string(r)

			if r == '\'' && iter != 0 {
				break
			}
			iter++
		}

		if runes == "0000-00-00" || runes == "0000-00-00 00:00:00" {
			return time.Time{}, nil
		}

		if bytes.Equal([]byte(runes), []byte(`'\N'`)) {
			return nil, nil
		}

		dB = bytes.NewBufferString(runes)
	case reflectTypeEmptyStruct:
		d := readRaw(s)
		dB = d
	default:
		d := readRaw(s)
		dB = d
	}

	if bytes.Equal(dB.Bytes(), []byte(`\N`)) {
		return nil, nil
	}

	return p.DataParser.Parse(dB)
}

type stringParser struct {
	unquote bool
	length  int
}

func (p *stringParser) Parse(s io.RuneScanner) (driver.Value, error) {
	return readString(s, p.length, p.unquote)
}

func (p *stringParser) Type() reflect.Type {
	return reflectTypeString
}

type dateTimeParser struct {
	unquote  bool
	format   string
	location *time.Location
}

func (p *dateTimeParser) Parse(s io.RuneScanner) (driver.Value, error) {
	str, err := readString(s, len(p.format), p.unquote)
	if err != nil {
		return nil, fmt.Errorf("failed to read the string representation of date or datetime: %v", err)
	}

	if str == "0000-00-00" || str == "0000-00-00 00:00:00" {
		return time.Time{}, nil
	}

	return time.ParseInLocation(p.format, str, p.location)
}

func (p *dateTimeParser) Type() reflect.Type {
	return reflectTypeTime
}

type arrayParser struct {
	arg DataParser
}

func (p *arrayParser) Type() reflect.Type {
	return reflect.SliceOf(p.arg.Type())
}

type tupleParser struct {
	args []DataParser
}

func (p *tupleParser) Type() reflect.Type {
	fields := make([]reflect.StructField, len(p.args))
	for i, arg := range p.args {
		fields[i].Name = "Field" + strconv.Itoa(i)
		fields[i].Type = arg.Type()
	}
	return reflect.StructOf(fields)
}

func (p *tupleParser) Parse(s io.RuneScanner) (driver.Value, error) {
	r := read(s)
	if r != '(' {
		return nil, fmt.Errorf("unexpected character '%c', expected '(' at the beginning of tuple", r)
	}

	struc := reflect.New(p.Type()).Elem()
	for i, arg := range p.args {
		if i > 0 {
			r := read(s)
			if r != ',' {
				return nil, fmt.Errorf("unexpected character '%c', expected ',' between tuple elements", r)
			}
		}

		v, err := arg.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse tuple element: %v", err)
		}

		struc.Field(i).Set(reflect.ValueOf(v))
	}

	r = read(s)
	if r != ')' {
		return nil, fmt.Errorf("unexpected character '%c', expected ')' at the end of tuple", r)
	}

	return struc.Interface(), nil
}

func (p *arrayParser) Parse(s io.RuneScanner) (driver.Value, error) {
	r := read(s)
	if r != '[' {
		return nil, fmt.Errorf("unexpected character '%c', expected '[' at the beginning of array", r)
	}

	slice := reflect.MakeSlice(p.Type(), 0, 0)
	for i := 0; ; i++ {
		r := read(s)
		_ = s.UnreadRune()
		if r == ']' {
			break
		}

		v, err := p.arg.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("failed to parse array element: %v", err)
		}

		if v == nil {
			if reflect.TypeOf(p.arg) != reflect.TypeOf(&nullableParser{}) {
				//need check if v is nil: panic otherwise
				return nil, fmt.Errorf("unexpected nil element")
			}
		} else {
			slice = reflect.Append(slice, reflect.ValueOf(v))
		}

		r = read(s)
		if r != ',' {
			_ = s.UnreadRune()
		}
	}

	r = read(s)
	if r != ']' {
		return nil, fmt.Errorf("unexpected character '%c', expected ']' at the end of array", r)
	}

	return slice.Interface(), nil
}

type lowCardinalityParser struct {
	arg DataParser
}

func (p *lowCardinalityParser) Type() reflect.Type {
	return p.arg.Type()
}

func (p *lowCardinalityParser) Parse(s io.RuneScanner) (driver.Value, error) {
	return p.arg.Parse(s)
}

type simpleAggregateFunctionParser struct {
	arg DataParser
}

func (p *simpleAggregateFunctionParser) Type() reflect.Type {
	return p.arg.Type()
}

func (p *simpleAggregateFunctionParser) Parse(s io.RuneScanner) (driver.Value, error) {
	return p.arg.Parse(s)
}

func newDateTimeParser(format string, loc *time.Location, unquote bool) (DataParser, error) {
	return &dateTimeParser{
		unquote:  unquote,
		format:   format,
		location: loc,
	}, nil
}

type intParser struct {
	signed  bool
	bitSize int
}

type floatParser struct {
	bitSize int
}

func (p *intParser) Parse(s io.RuneScanner) (driver.Value, error) {
	repr, err := readNumber(s)
	if err != nil {
		return nil, err
	}

	if p.signed {
		v, err := strconv.ParseInt(repr, 10, p.bitSize)
		switch p.bitSize {
		case 8:
			return int8(v), err
		case 16:
			return int16(v), err
		case 32:
			return int32(v), err
		case 64:
			return int64(v), err
		default:
			panic("unsupported bit size")
		}
	} else {
		v, err := strconv.ParseUint(repr, 10, p.bitSize)
		switch p.bitSize {
		case 8:
			return uint8(v), err
		case 16:
			return uint16(v), err
		case 32:
			return uint32(v), err
		case 64:
			return uint64(v), err
		default:
			panic("unsupported bit size")
		}
	}
}

func (p *intParser) Type() reflect.Type {
	if p.signed {
		switch p.bitSize {
		case 8:
			return reflectTypeInt8
		case 16:
			return reflectTypeInt16
		case 32:
			return reflectTypeInt32
		case 64:
			return reflectTypeInt64
		default:
			panic("unsupported bit size")
		}
	} else {
		switch p.bitSize {
		case 8:
			return reflectTypeUInt8
		case 16:
			return reflectTypeUInt16
		case 32:
			return reflectTypeUInt32
		case 64:
			return reflectTypeUInt64
		default:
			panic("unsupported bit size")
		}
	}
}

func (p *floatParser) Parse(s io.RuneScanner) (driver.Value, error) {
	repr, err := readNumber(s)
	if err != nil {
		return nil, err
	}

	v, err := strconv.ParseFloat(repr, p.bitSize)
	switch p.bitSize {
	case 32:
		return float32(v), err
	case 64:
		return float64(v), err
	default:
		panic("unsupported bit size")
	}
}

func (p *floatParser) Type() reflect.Type {
	switch p.bitSize {
	case 32:
		return reflectTypeFloat32
	case 64:
		return reflectTypeFloat64
	default:
		panic("unsupported bit size")
	}
}

type nothingParser struct{}

func (p *nothingParser) Parse(s io.RuneScanner) (driver.Value, error) {
	return nil, nil
}

func (p *nothingParser) Type() reflect.Type {
	return reflectTypeEmptyStruct
}

// DataParserOptions describes DataParser options.
// Ex.: Fields Location and UseDBLocation specify timezone options.
type DataParserOptions struct {
	// Location describes default location for DateTime and Date field without Timezone argument.
	Location *time.Location
	// UseDBLocation if false: always use Location, ignore DateTime argument.
	UseDBLocation bool
}

// NewDataParser creates a new DataParser based on the
// given TypeDesc.
func NewDataParser(t *TypeDesc, opt *DataParserOptions) (DataParser, error) {
	return newDataParser(t, false, opt)
}

func newDataParser(t *TypeDesc, unquote bool, opt *DataParserOptions) (DataParser, error) {
	switch t.Name {
	case "Nothing":
		return &nothingParser{}, nil
	case "Nullable":
		if len(t.Args) == 0 {
			return nil, fmt.Errorf("Nullable should pass original type")
		}
		p, err := newDataParser(t.Args[0], unquote, opt)
		if err != nil {
			return nil, err
		}
		return &nullableParser{p}, nil
	case "Date":
		loc := time.UTC
		if opt != nil && opt.Location != nil {
			loc = opt.Location
		}
		return newDateTimeParser(dateFormat, loc, unquote)
	case "DateTime":
		loc := time.UTC
		if (opt == nil || opt.Location == nil || opt.UseDBLocation) && len(t.Args) > 0 {
			var err error
			loc, err = time.LoadLocation(t.Args[0].Name)
			if err != nil {
				return nil, err
			}
		} else if opt != nil && opt.Location != nil {
			loc = opt.Location
		}
		return newDateTimeParser(timeFormat, loc, unquote)
	case "UInt8":
		return &intParser{false, 8}, nil
	case "UInt16":
		return &intParser{false, 16}, nil
	case "UInt32":
		return &intParser{false, 32}, nil
	case "UInt64":
		return &intParser{false, 64}, nil
	case "Int8":
		return &intParser{true, 8}, nil
	case "Int16":
		return &intParser{true, 16}, nil
	case "Int32":
		return &intParser{true, 32}, nil
	case "Int64":
		return &intParser{true, 64}, nil
	case "Float32":
		return &floatParser{32}, nil
	case "Float64":
		return &floatParser{64}, nil
	case "Decimal", "String", "Enum8", "Enum16", "UUID", "IPv4", "IPv6":
		return &stringParser{unquote: unquote}, nil
	case "FixedString":
		if len(t.Args) != 1 {
			return nil, fmt.Errorf("length not specified for FixedString")
		}
		length, err := strconv.Atoi(t.Args[0].Name)
		if err != nil {
			return nil, fmt.Errorf("malformed length specified for FixedString: %v", err)
		}
		return &stringParser{unquote: unquote, length: length}, nil
	case "Array":
		if len(t.Args) != 1 {
			return nil, fmt.Errorf("element type not specified for Array")
		}
		subParser, err := newDataParser(t.Args[0], true, opt)
		if err != nil {
			return nil, fmt.Errorf("failed to create parser for array elements: %v", err)
		}
		return &arrayParser{subParser}, nil
	case "Tuple":
		if len(t.Args) < 1 {
			return nil, fmt.Errorf("element types not specified for Tuple")
		}
		subParsers := make([]DataParser, len(t.Args))
		for i, arg := range t.Args {
			subParser, err := newDataParser(arg, true, opt)
			if err != nil {
				return nil, fmt.Errorf("failed to create parser for tuple element: %v", err)
			}
			subParsers[i] = subParser
		}
		return &tupleParser{subParsers}, nil
	case "LowCardinality":
		if len(t.Args) != 1 {
			return nil, fmt.Errorf("element type not specified for LowCardinality")
		}
		subParser, err := newDataParser(t.Args[0], unquote, opt)
		if err != nil {
			return nil, fmt.Errorf("failed to create parser for LowCardinality elements: %v", err)
		}
		return &lowCardinalityParser{subParser}, nil
	case "SimpleAggregateFunction":
		if len(t.Args) != 2 {
			return nil, fmt.Errorf("incorrect number of arguments for SimpleAggregateFunction")
		}
		subParser, err := newDataParser(t.Args[1], unquote, opt)
		if err != nil {
			return nil, fmt.Errorf("failed to create parser for SimpleAggregateFunction element: %v", err)
		}
		return &simpleAggregateFunctionParser{subParser}, nil
	default:
		return nil, fmt.Errorf("type %s is not supported", t.Name)
	}
}
