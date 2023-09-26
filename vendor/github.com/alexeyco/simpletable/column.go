package simpletable

// tblColumn is a meta table column
type tblColumn struct {
	Cells []cellInterface
	Table *Table
	width int
}

// resize resizes column to maximum nested cell length
func (c *tblColumn) resize() {
	c.width = 0

	for _, cell := range c.Cells {
		if !cell.isSpanned() {
			w := cell.len()
			if w > c.width {
				c.width = w
			}
		}
	}

	c.incrementWidth(0)
}

// incrementWidth increment nested cells width to i
func (c *tblColumn) incrementWidth(i int) {
	c.width += i
	for _, cell := range c.Cells {
		cell.setWidth(c.width)
	}
}

// getWidth returns column width
func (c *tblColumn) getWidth() int {
	return c.width
}
