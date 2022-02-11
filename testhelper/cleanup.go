package testhelper

import "log"

type Cleanup struct {
	fns []func() error
}

func (c *Cleanup) Defer(fn func() error) {
	c.fns = append(c.fns, fn)
}

func (c *Cleanup) Run() {
	l := len(c.fns) - 1
	for i := range c.fns {
		if err := c.fns[l-i](); err != nil {
			log.Println(err)
		}
	}
}
