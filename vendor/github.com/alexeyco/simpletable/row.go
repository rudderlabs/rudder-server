package simpletable

import (
	"fmt"
	"strings"
)

// tblRow is a meta table row
type tblRow struct {
	Cells []cellInterface
	Table *Table
}

// len returns row length
func (r *tblRow) len() int {
	l := 0
	for _, c := range r.Cells {
		l += c.width()
	}

	len := len(r.Cells)
	return l + (3 * len) - 1
}

// toStringSlice returns row contents as a toString
func (r *tblRow) toStringSlice() []string {
	l := [][]string{}

	for _, c := range r.Cells {
		l = append(l, c.lines())
	}

	l = r.transpose(l)

	ret := []string{}
	for _, s := range l {
		row := strings.Join(s, fmt.Sprintf(" %s ", r.Table.style.Cell))
		if !r.isDivider() {
			row = fmt.Sprintf(" %s ", row)
		}

		ret = append(ret, row)
	}

	return ret
}

// transpose transposes slice of string slices
func (r *tblRow) transpose(s [][]string) [][]string {
	ret := [][]string{}
	l := len(s)

	for x := 0; x < len(s[0]); x++ {
		ret = append(ret, make([]string, l))
	}

	for x, row := range s {
		for y, c := range row {
			ret[y][x] = c
		}
	}

	return ret
}

// resize resets all cells height
func (r *tblRow) resize() {
	m := 0
	for _, c := range r.Cells {
		h := c.height()
		if h > m {
			m = h
		}
	}

	for _, c := range r.Cells {
		c.setHeight(m)
	}
}

// isDivider check if a row is divider (if first row cell is divider - row divider to)
func (r *tblRow) isDivider() bool {
	switch r.Cells[0].(type) {
	case *dividerCell:
		return true
	}

	return false
}
