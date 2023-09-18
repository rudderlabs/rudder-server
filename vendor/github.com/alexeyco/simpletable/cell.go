package simpletable

const (
	// AlignLeft sets cell left alignment (default)
	AlignLeft = 0

	// AlignCenter sets cell center alignment
	AlignCenter = 1

	// AlignRight sets cell right alignment
	AlignRight = 2
)

// cellInterface is a basic cell interface
type cellInterface interface {
	len() int              // Returns cell text length
	isSpanned() bool       // Returns true if cell spanned
	width() int            // Returns cell content width
	setWidth(int)          // Sets cell content width
	height() int           // Returns cell content height
	setHeight(int)         // Sets cell content height
	getColumn() *tblColumn // Returns parent column
	setColumn(*tblColumn)  // Sets parent column
	lines() []string       // Returns cell as string slice
}

// Cell is a table cell
type Cell struct {
	Align    int          // Cell alignment
	Span     int          // span cell to right (1 - default)
	Text     string       // Cell raw text
	content  *content     // Cell content object instance
	children []*emptyCell // Nested empty cells
	column   *tblColumn   // Parent column
}

func (c *Cell) len() int {
	return c.width()
}

func (c *Cell) isSpanned() bool {
	return c.Span > 1
}

func (c *Cell) width() int {
	return c.content.width()
}

func (c *Cell) setWidth(width int) {
	c.content.setWidth(width)
}

func (c *Cell) height() int {
	return c.content.height()
}

func (c *Cell) setHeight(height int) {
	c.content.setHeight(height)
}

func (c *Cell) getColumn() *tblColumn {
	return c.column
}

func (c *Cell) setColumn(column *tblColumn) {
	c.column = column
}

func (c *Cell) resize() {
	if !c.isSpanned() {
		return
	}

	s := c.column.getWidth()
	for _, ch := range c.children {
		s += ch.getColumn().getWidth()
	}

	s += len(c.children) * 3

	if s > c.len() {
		c.setWidth(s)
	} else {
		cols := []*tblColumn{
			c.column,
		}

		for _, ch := range c.children {
			cols = append(cols, ch.column)
		}

		c.column.Table.incrementColumns(cols, c.len()-s)
	}
}

func (c *Cell) lines() []string {
	return c.content.lines(c.Align)
}

// dividerCell is table divider cell
type dividerCell struct {
	span     int          // Divider span
	children []*emptyCell // Nested empty meta cells
	column   *tblColumn   // Divider parent column
}

func (d *dividerCell) len() int {
	return 1
}

func (d *dividerCell) isSpanned() bool {
	return d.span > 1
}

func (d *dividerCell) width() int {
	return 1
}

func (d *dividerCell) setWidth(width int) {
}

func (d *dividerCell) height() int {
	return 1
}

func (d *dividerCell) setHeight(height int) {
}

func (d *dividerCell) getColumn() *tblColumn {
	return d.column
}

func (d *dividerCell) setColumn(column *tblColumn) {
	d.column = column
}

func (d *dividerCell) lines() []string {
	s := d.column.Table.style.Divider
	c := &content{
		c: []string{
			d.column.Table.line(s.Left, s.Center, s.Right, s.Intersection),
		},
	}

	return c.lines(AlignLeft)
}

// emptyCell is meta cell, used when cell is spanned
type emptyCell struct {
	parent  cellInterface // Parent cell
	w       int           // Cell width
	column  *tblColumn    // Parent cell column
	content *content      // Cell content
}

func (e *emptyCell) len() int {
	return 0
}

func (e *emptyCell) isSpanned() bool {
	return false
}

func (e *emptyCell) width() int {
	return e.w
}

func (e *emptyCell) setWidth(width int) {
	e.w = width
}

func (e *emptyCell) height() int {
	return e.content.height()
}

func (e *emptyCell) setHeight(height int) {
	e.content.setHeight(height)
}

func (e *emptyCell) getColumn() *tblColumn {
	return e.column
}

func (e *emptyCell) setColumn(column *tblColumn) {
	e.column = column
}

func (e *emptyCell) lines() []string {
	return e.content.lines(AlignLeft)
}
