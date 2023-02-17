package misc

import (
	"golang.org/x/exp/constraints"
)

type Number interface {
	constraints.Integer | constraints.Float
}

// ExponentialNumber is a simple exponentially increasing number.
type ExponentialNumber[T Number] struct {
	value T
}

// Reset resets the number to zero.
func (expo *ExponentialNumber[T]) Reset() {
	expo.value = 0
}

// Next returns the next number, which is the previous one multiplied by 2, always abiding by the min and max provided.
func (expo *ExponentialNumber[T]) Next(min, max T) T {
	expo.value *= 2
	if expo.value > max {
		expo.value = max
	}
	if expo.value < min {
		expo.value = min
	}
	return expo.value
}
