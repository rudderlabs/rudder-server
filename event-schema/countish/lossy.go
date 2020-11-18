package countish

import (
	"math"
)

type FDeltaPair struct {
	F     float64
	Delta float64
}

type LossyCounter struct {
	Support        float64
	ErrorTolerance float64
	D              map[string]FDeltaPair
	N              uint64
	BucketWidth    uint64
}

func NewLossyCounter(support, errorTolerance float64) *LossyCounter {
	return &LossyCounter{
		Support:        support,
		ErrorTolerance: errorTolerance,
		D:              make(map[string]FDeltaPair),
		BucketWidth:    uint64(math.Ceil(1 / errorTolerance)),
		N:              0,
	}
}
func (lc *LossyCounter) prune(bucket uint64) {
	fbucket := float64(bucket)
	for key, value := range lc.D {
		if value.F+value.Delta <= fbucket {
			delete(lc.D, key)
		}
	}
}

// ItemsAboveThreshold returns a list of items that occur more than threshold, along
// with their frequencies. threshold is in the range [0,1]
func (lc *LossyCounter) ItemsAboveThreshold(threshold float64) []Entry {
	var results []Entry
	fN := float64(lc.N)
	for key, val := range lc.D {
		if val.F >= (threshold-float64(lc.ErrorTolerance))*fN {
			results = append(results, Entry{Key: key, Frequency: val.F/fN + lc.Support})
		}
	}
	return results
}

// Observe records a new sample
func (lc *LossyCounter) Observe(key string) {
	lc.N++
	bucket := lc.N / lc.BucketWidth
	val, exists := lc.D[key]
	if exists {
		val.F++
	} else {
		// reuse 0 val from lookup.
		val.F = 1
		val.Delta = float64(bucket - 1) // this doesn't make much sense
	}
	lc.D[key] = val
	if lc.N%lc.BucketWidth == 0 {
		lc.prune(bucket)
	}
}
