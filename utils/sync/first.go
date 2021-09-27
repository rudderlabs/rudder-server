package sync

import (
	"sync/atomic"
)

type First struct {
	isFirst uint32
}

func (f *First) First() bool {
	return atomic.CompareAndSwapUint32(&f.isFirst, 0, 1)
}
