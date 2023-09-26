package simpletable

import (
	"fmt"
	"math"
	"strings"
)

// Table main table object
type Table struct {
	Header   *Header
	Body     *Body
	Footer   *Footer
	style    *Style
	rows     []*tblRow
	columns  []*tblColumn
	spanned  []*Cell
	dividers []*dividerCell
}

//SetStyle sets table style
func (t *Table) SetStyle(style *Style) {
	t.style = style
}

// String returns table as a toString
func (t *Table) String() string {
	t.refresh()

	// TODO: protect against wrong spans

	t.initCellContent()
	t.prepareRows()
	t.prepareColumns()

	t.resizeRows()
	t.resizeColumns()

	s := []string{}

	b := t.borderTop()
	if b != "" {
		s = append(s, b)
	}

	for _, r := range t.rows {
		s = append(s, t.borderLeftRight(r.toStringSlice(), r.isDivider())...)
	}

	b = t.borderBottom()
	if b != "" {
		s = append(s, b)
	}

	return strings.Join(s, "\n")
}

// Print prints table
func (t *Table) Print() {
	fmt.Print(t.String())
}

// Println prints table with new line below
func (t *Table) Println() {
	fmt.Println(t.String())
}

// refresh resets table meta data (t.rows, t.columns, t.spanned, t.dividers)
func (t *Table) refresh() {
	t.rows = []*tblRow{}
	t.columns = []*tblColumn{}
	t.spanned = []*Cell{}
	t.dividers = []*dividerCell{}
}

// borderTop returns top table border
func (t *Table) borderTop() string {
	s := t.style.Border
	return t.line(s.TopLeft, s.Top, s.TopRight, s.TopIntersection)
}

// borderBottom returns bottom table border
func (t *Table) borderBottom() string {
	s := t.style.Border
	return t.line(s.BottomLeft, s.Bottom, s.BottomRight, s.BottomIntersection)
}

// borderLeftRight returns bordered row as a toString
func (t *Table) borderLeftRight(s []string, d bool) []string {
	if d {
		return s
	}

	for i, v := range s {
		s[i] = fmt.Sprintf("%s%s%s", t.style.Border.Left, v, t.style.Border.Right)
	}

	return s
}

// line returns line (border or divider) as a toString
func (t *Table) line(l, c, r, i string) string {
	b := []string{}
	for _, col := range t.columns {
		b = append(b, strings.Repeat(c, col.getWidth()+2))
	}

	return fmt.Sprintf("%s%s%s", l, strings.Join(b, i), r)
}

// textSlice2CellSlice casts []*Cell to []cellInterface cause it's not possible do this:
//     s := append([]cellInterface{}, t...)
func (t *Table) textSlice2CellSlice(c []*Cell) []cellInterface {
	r := []cellInterface{}

	for _, tc := range c {
		r = append(r, tc)
	}

	return r
}

// initCellContent loads content for all Cells of table (t.Header.Cells, t.Footer.Cells and t.Body.Cells)
func (t *Table) initCellContent() {
	t.initContent(t.Header.Cells...)

	for _, c := range t.Body.Cells {
		t.initContent(c...)
	}

	t.initContent(t.Footer.Cells...)
}

// initContent loads content in a slice of Cell
func (t *Table) initContent(s ...*Cell) {
	for _, c := range s {
		c.content = newContent(c.Text)
	}
}

// prepareRows fills t.rows slice from t.Header, t.Body and t.Footer
func (t *Table) prepareRows() {
	hlen := len(t.Header.Cells)
	if hlen > 0 {
		t.rows = append(t.rows, &tblRow{
			Cells: t.textSlice2CellSlice(t.Header.Cells),
			Table: t,
		})

		d := &dividerCell{
			span: hlen,
		}

		if t.style.Divider.Center != "" {
			t.rows = append(t.rows, &tblRow{
				Cells: []cellInterface{
					d,
				},
				Table: t,
			})
		}

		t.dividers = append(t.dividers, d)
	}

	for _, r := range t.Body.Cells {
		t.rows = append(t.rows, &tblRow{
			Cells: t.textSlice2CellSlice(r),
			Table: t,
		})
	}

	flen := len(t.Footer.Cells)
	if flen > 0 {
		d := &dividerCell{
			span: hlen,
		}

		if t.style.Divider.Center != "" {
			t.rows = append(t.rows, &tblRow{
				Cells: []cellInterface{
					d,
				},
				Table: t,
			})
		}

		t.dividers = append(t.dividers, d)

		t.rows = append(t.rows, &tblRow{
			Cells: t.textSlice2CellSlice(t.Footer.Cells),
			Table: t,
		})
	}
}

// prepareColumns fills t.columns slice from t.rows
func (t *Table) prepareColumns() {
	m := [][]cellInterface{}

	for _, r := range t.rows {
		row := []cellInterface{}

		for _, c := range r.Cells {
			row = append(row, c)
			span := 0
			var p cellInterface
			var tc *Cell

			switch v := c.(type) {
			case *Cell:
				span = v.Span
				p = v
				tc = v
			case *dividerCell:
				span = v.span
				p = v
			}

			if span > 1 {
				empty := []*emptyCell{}

				for i := 1; i < span; i++ {
					empty = append(empty, &emptyCell{
						parent: p,
					})
				}

				for _, c := range empty {
					row = append(row, c)
				}

				if tc != nil {
					t.spanned = append(t.spanned, tc)
				}

				switch v := c.(type) {
				case *Cell:
					v.children = empty
				case *dividerCell:
					v.children = empty
				}
			}
		}

		m = append(m, row)
	}

	m = t.transposeCells(m)
	for _, r := range m {
		c := &tblColumn{
			Cells: r,
			Table: t,
		}

		for _, cell := range c.Cells {
			cell.setColumn(c)
		}

		t.columns = append(t.columns, c)
	}
}

// transposeCells transposes cells matrix - needed for t.columns filling
func (t *Table) transposeCells(i [][]cellInterface) [][]cellInterface {
	r := [][]cellInterface{}

	for x := 0; x < len(i[0]); x++ {
		r = append(r, make([]cellInterface, len(i)))
	}

	for x, row := range i {
		for y, c := range row {
			r[y][x] = c
		}
	}

	return r
}

// resizeColumns calculate column height
func (t *Table) resizeRows() {
	for _, r := range t.rows {
		r.resize()
	}
}

// resizeColumns calculate column width (text cells and dividers)
func (t *Table) resizeColumns() {
	for _, c := range t.columns {
		c.resize()
	}

	for _, c := range t.spanned {
		c.resize()
	}

	for _, d := range t.dividers {
		s := t.size()
		d.setWidth(s)
	}
}

// incrementColumns bulk increment columns for specified length
func (t *Table) incrementColumns(c []*tblColumn, length int) {
	sizes := t.carve(length, len(c))

	for i, col := range c {
		col.incrementWidth(sizes[i])
	}
}

// carve splits length into a slice of integers, the sum of which is equal to length, and the count - equal to parts
func (t *Table) carve(length, parts int) []int {
	r := []int{}
	step := int(math.Floor(float64(length) / float64(parts)))
	if step*parts != length {
		step++
	}

	for i := 0; i < parts; i++ {
		var n int
		if length < step {
			n = length
		} else {
			n = step
		}

		length -= n
		r = append(r, n)
	}

	return r
}

// size returns table content size.
func (t *Table) size() int {
	return t.rows[0].len()
}

// New is a Table constructor. It loads struct data, ready to be manipulated.
func New() *Table {
	return &Table{
		style: StyleDefault,
		Header: &Header{
			Cells: []*Cell{},
		},
		Body: &Body{
			Cells: [][]*Cell{},
		},
		Footer: &Footer{
			Cells: []*Cell{},
		},
		rows:     []*tblRow{},
		columns:  []*tblColumn{},
		spanned:  []*Cell{},
		dividers: []*dividerCell{},
	}
}
