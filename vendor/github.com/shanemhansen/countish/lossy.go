package countish

import (
	"math"
)

type fDeltaPair struct {
	f     float64
	delta float64
}

type lossyCounter struct {
	support        float64
	errorTolerance float64
	D              map[string]fDeltaPair
	N              uint64
	bucketWidth    uint64
}

func NewLossyCounter(support, errorTolerance float64) *lossyCounter {
	return &lossyCounter{
		support:        support,
		errorTolerance: errorTolerance,
		D:              make(map[string]fDeltaPair),
		bucketWidth:    uint64(math.Ceil(1 / errorTolerance)),
		N:              0,
	}
}
func (lc *lossyCounter) prune(bucket uint64) {
	fbucket := float64(bucket)
	for key, value := range lc.D {
		if value.f+value.delta <= fbucket {
			delete(lc.D, key)
		}
	}
}

// ItemsAboveThreshold returns a list of items that occur more than threshold, along
// with their frequencies. threshold is in the range [0,1]
func (lc *lossyCounter) ItemsAboveThreshold(threshold float64) []Entry {
	var results []Entry
	fN := float64(lc.N)
	for key, val := range lc.D {
		if val.f >= (threshold-float64(lc.errorTolerance))*fN {
			results = append(results, Entry{Key: key, Frequency: val.f/fN + lc.support})
		}
	}
	return results
}

// Observe records a new sample
func (lc *lossyCounter) Observe(key string) {
	lc.N++
	bucket := lc.N / lc.bucketWidth
	val, exists := lc.D[key]
	if exists {
		val.f++
	} else {
		// reuse 0 val from lookup.
		val.f = 1
		val.delta = float64(bucket - 1) // this doesn't make much sense
	}
	lc.D[key] = val
	if lc.N%lc.bucketWidth == 0 {
		lc.prune(bucket)
	}
}
