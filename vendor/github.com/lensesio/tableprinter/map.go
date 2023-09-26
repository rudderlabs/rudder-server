package tableprinter

import (
	"fmt"
	"reflect"
)

// Should we have a single parser value its specific types and give input arguments to the funcs, like "keys"
// or is better to initialize a new parser on each output, so it can be used as a cache?
type mapParser struct {
	TagsOnly bool
}

func (p *mapParser) Parse(v reflect.Value, filters []RowFilter) ([]string, [][]string, []int) {
	keys := p.Keys(v)
	if len(keys) == 0 {
		return nil, nil, nil
	}

	headers := p.ParseHeaders(v, keys)
	rows, numbers := p.ParseRows(v, keys, filters)

	return headers, rows, numbers
}

func (p *mapParser) Keys(v reflect.Value) []reflect.Value {
	return v.MapKeys()
}

func extendSlice(slice reflect.Value, typ reflect.Type, max int) reflect.Value {
	if slice.Len() == max {
		return slice
	}

	empty := reflect.New(typ).Elem()
	if slice.Len() == 0 {
		for max > 0 {
			slice = reflect.Append(slice, empty)
			max--
		}
		return slice
	}

	for max > slice.Len() {
		slice = reflect.Append(slice, empty)
	}

	return slice
}

func (p *mapParser) ParseRows(v reflect.Value, keys []reflect.Value, filters []RowFilter) ([][]string, []int) {
	// cursors := make(map[int]int) // key = map's key index(although maps don't keep order), value = current index of elements inside the map.
	maxLength := maxMapElemLength(v, keys)

	rows := make([][]string, maxLength)
	// depends on the header size, this is for the entire col aligment but
	// we can't do that on `GetHeaders` because its values depends on the rows[index] value's type to the table.
	numbers := make([]int, 0)

	for _, key := range keys {

		elem := v.MapIndex(key)
		if elem.Kind() != reflect.Slice {
			if !CanAcceptRow(elem, filters) {
				continue
			}

			a, row := extractCells(0, emptyHeader, elem, p.TagsOnly)
			if len(row) == 0 {
				continue
			}

			if cap(rows) == 0 {
				rows = [][]string{row}
			} else {
				rows[0] = append(rows[0], row...)
			}

			numbers = append(numbers, a...)
			continue
		}

		n := elem.Len()
		if n == 0 {
			continue
		}

		if elem.Len() < maxLength {
			elem = extendSlice(elem, elem.Index(0).Type(), maxLength)
		}

		for i, n := 0, elem.Len(); i < n; i++ {
			item := elem.Index(i)
			if !CanAcceptRow(item, filters) {
				continue
			}

			a, row := extractCells(i, emptyHeader, item, p.TagsOnly)

			if len(row) == 0 {
				continue
			}

			rows[i] = append(rows[i], row...)
			numbers = append(numbers, a...)
		}
	}

	return rows, numbers
}

func (p *mapParser) ParseHeaders(v reflect.Value, keys []reflect.Value) (headers []string) {
	if len(keys) == 0 {
		return nil
	}

	for _, key := range keys {
		// support any type, even if it's declared as "interface{}" or pointer to something, we care about this "something"'s value.
		key = indirectValue(key)
		if !key.CanInterface() {
			continue
		}

		if header := stringValue(key); header != "" {
			headers = append(headers, header)
		}
	}

	return
}

func maxMapElemLength(v reflect.Value, keys []reflect.Value) (max int) {
	for _, key := range keys {
		elem := v.MapIndex(key)
		if elem.Kind() != reflect.Slice {
			continue
		}
		if current := elem.Len(); current > max {
			max = current
		}
	}

	return
}

func stringValue(key reflect.Value) string {
	if !key.CanInterface() {
		return ""
	}

	switch keyV := key.Interface().(type) {
	case string:
		return keyV
	case fmt.Stringer:
		return keyV.String()
	default:
		return ""
	}
}
