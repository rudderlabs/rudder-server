package tableprinter

import (
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// StructHeaders are being cached from the root-level structure to print out.
// They can be customized for custom head titles.
//
// Header can also contain the necessary information about its values, useful for its presentation
// such as alignment, alternative value if main is empty, if the row should print the number of elements inside a list or if the column should be formated as number.
var (
	StructHeaders    = make(map[reflect.Type][]StructHeader) // type is the root struct.
	structsHeadersMu sync.RWMutex
)

type structParser struct {
	TagsOnly bool
}

func (p *structParser) Parse(v reflect.Value, filters []RowFilter) ([]string, [][]string, []int) {
	if !CanAcceptRow(v, filters) {
		return nil, nil, nil
	}

	row, nums := p.ParseRow(v)

	return p.ParseHeaders(v), [][]string{row}, nums
}

func (p *structParser) ParseHeaders(v reflect.Value) []string {
	hs := extractHeadersFromStruct(v.Type(), true)
	if len(hs) == 0 {
		return nil
	}

	headers := make([]string, len(hs))
	for idx := range hs {
		headers[idx] = hs[idx].Name
	}

	return headers
}

func (p *structParser) ParseRow(v reflect.Value) ([]string, []int) {
	return getRowFromStruct(v, p.TagsOnly)
}

// TimestampHeaderTagValue the header's value of a "timestamp" header tag functionality.
type TimestampHeaderTagValue struct {
	FromMilliseconds bool

	UTC   bool
	Local bool

	Human bool

	Format string
}

// StructHeader contains the name of the header extracted from the struct's `HeaderTag` field tag.
type StructHeader struct {
	Name string
	// Position is the horizontal position (start from zero) of the header.
	Position int

	ValueAsNumber    bool
	ValueAsCountable bool
	ValueAsText      bool
	ValueAsTimestamp bool
	TimestampValue   TimestampHeaderTagValue
	ValueAsDate      bool
	ValueAsDuration  bool

	AlternativeValue string
}

func extractHeaderFromStructField(f reflect.StructField, pos int, tagsOnly bool) (header StructHeader, ok bool) {
	if f.PkgPath != "" {
		return // ignore unexported fields.
	}

	headerTag := f.Tag.Get(HeaderTag)
	if headerTag == "" && tagsOnly {
		return emptyHeader, false
	}

	// embedded structs are acting like headers appended to the existing(s).
	if f.Type.Kind() == reflect.Struct {
		return emptyHeader, false
	} else if headerTag != "" {
		if header, ok := extractHeaderFromTag(headerTag); ok {
			header.Position = pos
			return header, true
		}

	} else if !tagsOnly {
		return StructHeader{
			Position: pos,
			Name:     f.Name,
		}, true
	}

	return emptyHeader, false
}

func extractHeadersFromStruct(typ reflect.Type, tagsOnly bool) (headers []StructHeader) {
	typ = indirectType(typ)
	if typ.Kind() != reflect.Struct {
		return
	}

	// search cache.
	structsHeadersMu.RLock()
	if cached, has := StructHeaders[typ]; has {
		structsHeadersMu.RUnlock()
		return cached
	}
	structsHeadersMu.RUnlock()

	for i, n := 0, typ.NumField(); i < n; i++ {
		f := typ.Field(i)
		if f.Type.Kind() == reflect.Struct && f.Tag.Get(HeaderTag) == InlineHeaderTag {
			hs := extractHeadersFromStruct(f.Type, tagsOnly)
			headers = append(headers, hs...)
			continue
		}

		header, _ := extractHeaderFromStructField(f, i, tagsOnly)
		if header.Name != "" {
			headers = append(headers, header)
		}
	}

	if len(headers) > 0 {
		// insert to cache if it's valid table.
		structsHeadersMu.Lock()
		StructHeaders[typ] = headers
		structsHeadersMu.Unlock()
	}

	return headers
}

var (
	timeStdFormats = map[string]string{
		TimestampFormatANSICHeaderTag:        time.ANSIC,
		TimestampFormatUnixDateCHeaderTag:    time.UnixDate,
		TimestampFormatRubyDateHeaderTag:     time.RubyDate,
		TimestampFormatRFC822HeaderTag:       time.RFC822,
		TimestampFormatRFC822ZHeaderTag:      time.RFC822Z, // default one.
		TimestampFormatRFC850HeaderTag:       time.RFC850,
		TimestampFormatRFC1123HeaderTag:      time.RFC1123,
		TimestampFormatRFC1123ZHeaderTag:     time.RFC1123Z,
		TimestampFormatRFC3339HeaderTag:      time.RFC3339,
		TimestampFormatARFC3339NanoHeaderTag: time.RFC3339Nano,
	}

	emptyTimestampHeaderTagValue TimestampHeaderTagValue
)

func extractTimestampHeader(timestampHeaderTagValue string) (TimestampHeaderTagValue, bool) {
	if !strings.HasPrefix(timestampHeaderTagValue, TimestampHeaderTag) {
		return emptyTimestampHeaderTagValue, false // should never happen at this state.
	}

	if len(timestampHeaderTagValue) == len(TimestampHeaderTag) {
		// timestamp without args.
		return emptyTimestampHeaderTagValue, true
	}

	trail := timestampHeaderTagValue[len(TimestampHeaderTag):] // timestamp:<<(....)>>
	if !strings.HasPrefix(trail, "(") || !strings.HasSuffix(trail, ")") {
		// invalid format for args, but still a valid simple timestamp.
		return emptyTimestampHeaderTagValue, true
	}

	t := TimestampHeaderTagValue{}
	argsLine := trail[1 : len(trail)-1]
	args := strings.Split(argsLine, "|")
	for _, arg := range args {
		// arg = strings.ToLower(arg)
		switch arg {
		case TimestampFromMillisecondsHeaderTag:
			t.FromMilliseconds = true
		case TimestampAsUTCHeaderTag:
			t.UTC = true
		case TimestampAsLocalHeaderTag:
			t.Local = true
		case TimestampFormatHumanHeaderTag:
			t.Human = true
		case // formats are specific.
			TimestampFormatANSICHeaderTag,
			TimestampFormatUnixDateCHeaderTag,
			TimestampFormatRubyDateHeaderTag,
			TimestampFormatRFC822HeaderTag,
			TimestampFormatRFC822ZHeaderTag,
			TimestampFormatRFC850HeaderTag,
			TimestampFormatRFC1123HeaderTag,
			TimestampFormatRFC1123ZHeaderTag,
			TimestampFormatRFC3339HeaderTag,
			TimestampFormatARFC3339NanoHeaderTag:
			if expectedFormat, ok := timeStdFormats[arg]; ok {
				t.Format = expectedFormat
			}
		default:
			// custom format.
			t.Format = arg
		}
	}

	if t.Format == "" {
		t.Format = TimestampFormatRFC822ZHeaderTag
	}

	return t, true
}
func extractHeaderFromTag(headerTag string) (header StructHeader, ok bool) {
	if headerTag == "" {
		return
	}
	ok = true

	parts := strings.Split(headerTag, ",")

	// header name is the first part.
	header.Name = parts[0]

	if len(parts) > 1 {
		for _, hv := range parts[1:] /* except the first part ofc which should be the header value */ {
			switch hv {
			// any position.
			case NumberHeaderTag:
				header.ValueAsNumber = true
			case CountHeaderTag:
				header.ValueAsCountable = true
			case ForceTextHeaderTag:
				header.ValueAsText = true
			case DurationHeaderTag:
				header.ValueAsDuration = true
			case DateHeaderTag:
				header.ValueAsDate = true
			default:
				if strings.HasPrefix(hv, TimestampHeaderTag) {
					header.TimestampValue, header.ValueAsTimestamp = extractTimestampHeader(hv)
					continue
				}

				header.AlternativeValue = hv
			}
		}
	}

	return
}

// getRowFromStruct returns the positions of the cells that should be aligned to the right
// and the list of cells(= the values based on the cell's description) based on the "in" value.
func getRowFromStruct(v reflect.Value, tagsOnly bool) (cells []string, rightCells []int) {
	typ := v.Type()
	j := 0

	for i, n := 0, typ.NumField(); i < n; i++ {

		f := typ.Field(i)
		header, ok := extractHeaderFromStructField(f, j, tagsOnly)
		if !ok {
			if f.Type.Kind() == reflect.Struct && f.Tag.Get(HeaderTag) == InlineHeaderTag {
				fieldValue := indirectValue(v.Field(i))
				c, rc := getRowFromStruct(fieldValue, tagsOnly)
				for _, rcc := range rc {
					rightCells = append(rightCells, rcc+j)
				}
				cells = append(cells, c...)
				j++
			}

			continue
		}

		fieldValue := indirectValue(v.Field(i))
		c, r := extractCells(j, header, fieldValue, tagsOnly)
		rightCells = append(rightCells, c...)
		cells = append(cells, r...)
		j++
	}

	return
}

// RemoveStructHeader will dynamically remove a specific header tag from a struct's field
// based on the "fieldName" which must be a valid exported field of the struct.
// It returns the new, converted, struct value.
//
// If "original" is not a struct or
// the "fieldName" was unable to be found then the "item" is returned as it was before this call.
//
// See `SetStructHeader` too.
func RemoveStructHeader(original interface{}, fieldName string) interface{} {
	return SetStructHeader(original, fieldName, "")
}

// SetStructHeader dynamically sets the "newHeaderValue" to a specific struct's field's header tag's value
// based on the "fieldName" which must be a valid exported field of the struct.
//
// It returns the new, converted, struct value.
//
// If "original" is not a struct or
// the "fieldName" was unable to be found then the "item" is returned as it was before this call.
//
// If the "newValue" is empty then the whole header will be removed, see `RemoveStructHeader` too.
func SetStructHeader(original interface{}, fieldName string, newHeaderValue string) interface{} {
	if original == nil {
		return nil
	}

	typ := indirectType(reflect.TypeOf(original))
	if typ.Kind() != reflect.Struct {
		return original
	}

	// we will catch only exported fields in order to convert the type successfully, so dynamic length,
	// in any case, the unexported fields are not used inside a table at all.
	n := typ.NumField()
	fs := make([]reflect.StructField, 0)
	// 1. copy the struct's fields, we will make a new one based on the "original"
	// 2. if the "fieldName" found then we can continue and clear the header tag, otherwise return "original".
	found := false
	if len(newHeaderValue) > 0 && !strings.Contains(newHeaderValue, `"`) {
		newHeaderValue = strconv.Quote(newHeaderValue)
	}

	for i := 0; i < n; i++ {
		f := typ.Field(i)
		if f.PkgPath != "" {
			continue
		}

		if f.Name == fieldName {
			found = true
			// json:"value" xml:"value header:"value"
			// header:"value"
			// json:"value" header:"value"
			// header:"value" xml:"value" json:"value"
			// json:"value" header:"value,options" xml:"value", we need to change only the [start: header:]...[end: last" or space],
			// but let's do it without for loops here, search for old value and change it if exists, otherwise append the whole header tag,
			// remove that if "newValue" is empty.
			oldHeaderValue := f.Tag.Get(HeaderTag)

			if oldHeaderValue == "" {
				if newHeaderValue != "" {
					line := string(f.Tag)
					if line != "" {
						line += " "
					}
					// set the header tag, append to the existing tag value(not just the header one) (if exists a space is prepended before the header tag).
					f.Tag = reflect.StructTag(line + HeaderTag + ":" + newHeaderValue)
				} else {
					// do nothing, new header value is empty and old header tag does not exist.
				}
			} else {
				// quote it.
				oldHeaderValue = strconv.Quote(oldHeaderValue)
				// simple replace.
				tag := string(f.Tag)
				if newHeaderValue != "" {
					tag = strings.Replace(tag, HeaderTag+":"+oldHeaderValue, HeaderTag+":"+newHeaderValue, 1)
				} else {
					// should remove the `HeaderTag`(?[space]header:[?value][?space]) if it's there.
					//
					// note: strings.Join/Split usage, not searching through char looping, although it would be faster but we don't really care here,
					// keep it simpler to change.
					tagValues := strings.Split(tag, " ")
					for j, part := range tagValues { // the whole value.
						if strings.HasPrefix(part, HeaderTag+":") {
							tagValues = append(tagValues[:j], tagValues[j+1:]...)
							break
						}
					}

					tag = strings.Join(tagValues, " ")
				}

				f.Tag = reflect.StructTag(tag)
			}
		}

		fs = append(fs, f)
	}

	if !found {
		return original
	}

	withoutHeaderTyp := reflect.StructOf(fs)
	tmp := reflect.New(withoutHeaderTyp).Elem()

	// fill the fields.
	v := indirectValue(reflect.ValueOf(original))
	for i := 0; i < n; i++ {
		f := typ.Field(i)
		if f.PkgPath != "" {
			// "original" may have unexported fields, so we check by name, see below.
			continue
		}

		for j := 0; j < withoutHeaderTyp.NumField(); j++ {
			tmpF := withoutHeaderTyp.Field(j)
			if tmpF.Name == f.Name {
				tmp.Field(j).Set(v.Field(i))
			}
		}
	}

	return tmp.Interface()
	// return indirectValue(reflect.ValueOf(original)).Convert(withoutHeaderTyp).Interface()
}
