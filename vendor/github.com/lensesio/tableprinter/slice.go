package tableprinter

import (
	"reflect"
)

type sliceParser struct {
	TagsOnly bool
}

var emptyStruct = struct{}{}

func (p *sliceParser) Parse(v reflect.Value, filters []RowFilter) ([]string, [][]string, []int) {
	headers := p.ParseHeaders(v)
	rows, nums := p.ParseRows(v, filters)
	return headers, rows, nums
}

func (p *sliceParser) ParseRows(v reflect.Value, filters []RowFilter) (rows [][]string, nums []int) {
	for i, n := 0, v.Len(); i < n; i++ {
		item := indirectValue(v.Index(i))
		if !CanAcceptRow(item, filters) {
			continue
		}

		if item.Kind() != reflect.Struct {
			// if not struct, don't search its fields, just put a row as it's.
			c, r := extractCells(i, emptyHeader, indirectValue(item), p.TagsOnly)
			rows = append(rows, r)
			nums = append(nums, c...)
			continue
		}
		r, c := getRowFromStruct(item, p.TagsOnly)

		nums = append(nums, c...)

		rows = append(rows, r)
	}

	return
}

func (p *sliceParser) ParseHeaders(v reflect.Value) (headers []string) {
	tmp := make(map[reflect.Type]struct{})

	for i, n := 0, v.Len(); i < n; i++ {
		item := indirectValue(v.Index(i))

		// no filters.
		itemTyp := item.Type()
		if _, ok := tmp[itemTyp]; !ok {
			// make headers once per type.
			tmp[itemTyp] = emptyStruct
			hs := extractHeadersFromStruct(itemTyp, p.TagsOnly)
			if len(hs) == 0 {
				continue
			}
			for _, h := range hs {
				headers = append(headers, h.Name)
			}
		}
	}

	return
}
