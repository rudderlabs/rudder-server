package simpletable

import (
	"fmt"
	"github.com/mattn/go-runewidth"
	"regexp"
	"strings"
)

// stripAnsiEscapeRegexp is a regular expression to clean ANSI Control sequences
// feat https://stackoverflow.com/questions/14693701/how-can-i-remove-the-ansi-escape-sequences-from-a-string-in-python#33925425
var stripAnsiEscapeRegexp = regexp.MustCompile(`(\x9B|\x1B\[)[0-?]*[ -/]*[@-~]`)

// content is a cell content
type content struct {
	c  []string // content lines (trimmed)
	mw int      // maximal content width (f.e, for multilines columns)
	w  int      // meta content width
}

// width returns content width
func (c *content) width() int {
	m := c.mw
	if m > c.w {
		return m
	}

	return c.w
}

// setWidth sets content block width
func (c *content) setWidth(w int) {
	c.w = w
}

// height returns content height
func (c *content) height() int {
	return len(c.c)
}

// setHeigth sets content height
func (c *content) setHeight(h int) {
	l := c.height()
	if h <= l {
		return
	}

	for i := 0; i < h-l; i++ {
		c.c = append(c.c, "")
	}
}

// lines returns content as string slice
func (c *content) lines(a int) []string {
	r := []string{}

	for _, l := range c.c {
		r = append(r, c.line(l, a))
	}

	return r
}

// line formats content line
func (c *content) line(l string, a int) string {
	len := c.width() - realLength(l)
	if len <= 0 {
		return l
	}

	switch a {
	case AlignLeft:
		l = fmt.Sprintf("%s%s", l, strings.Repeat(" ", len))

	case AlignCenter:
		lft := len / 2
		rgt := len - lft

		left := strings.Repeat(" ", lft)
		right := strings.Repeat(" ", rgt)

		l = fmt.Sprintf("%s%s%s", left, l, right)

	case AlignRight:
		l = fmt.Sprintf("%s%s", strings.Repeat(" ", len), l)
	}

	return l
}

// newContent returns new content object
func newContent(s string) *content {
	c := strings.Split(s, "\n")
	mw := 0

	for i, v := range c {
		c[i] = strings.TrimSpace(v)
		w := realLength(c[i])

		if w > mw {
			mw = w
		}
	}

	return &content{
		c:  c,
		mw: mw,
	}
}

// stripAnsiEscape returns string without ANSI escape sequences (colors etc)
func stripAnsiEscape(s string) string {
	return stripAnsiEscapeRegexp.ReplaceAllString(s, "")
}

// realWidth returns real string length (without ANSI escape sequences)
func realLength(s string) int {
	return runewidth.StringWidth(stripAnsiEscape(s))
}
