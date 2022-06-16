package testhelper

import "fmt"

type Cleanup struct {
	fns []func()
}

func (*Cleanup) Log(a ...interface{}) {
	fmt.Println(a...)
}

func (c *Cleanup) Cleanup(fn func()) {
	c.fns = append(c.fns, fn)
}

func (c *Cleanup) Run() {
	l := len(c.fns) - 1
	for i := range c.fns {
		c.fns[l-i]()
	}
}
