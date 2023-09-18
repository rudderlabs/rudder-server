package simpletable

// Header is table header
type Header struct {
	Cells []*Cell
}

// Body is table body
type Body struct {
	Cells [][]*Cell
}

// Footer is table footer
type Footer struct {
	Cells []*Cell
}
