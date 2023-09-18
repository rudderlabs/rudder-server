package compose

import (
	"bytes"
	"os/exec"
)

type File interface {
	apply(c *exec.Cmd)
}

type FilePaths []string

func (f FilePaths) apply(c *exec.Cmd) {
	for _, filePath := range f {
		c.Args = append(c.Args, "-f", filePath)
	}
}

type FileBytes []byte

func (f FileBytes) apply(c *exec.Cmd) {
	c.Args = append(c.Args, "-f", "/dev/stdin")
	c.Stdin = bytes.NewReader(f)
}
