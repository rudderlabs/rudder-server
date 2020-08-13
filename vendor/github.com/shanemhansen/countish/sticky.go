package countish

import (
	"math"
	"math/rand"
)

var Rand = rand.Float64
var RandCoin = rand.Int31n

type StickySampler struct {
	errorTolerance  float64
	support         float64
	S               map[string]float64
	R               float64
	failureProb     float64
	N               float64
	t               float64
	RequiredSamples int
}

func NewSampler(support, errorTolerance, failureProb float64) *StickySampler {
	twoT := 2 / errorTolerance * math.Log(1/(support*failureProb))
	return &StickySampler{
		errorTolerance:  errorTolerance,
		support:         support,
		R:               1,
		failureProb:     failureProb,
		t:               twoT,
		RequiredSamples: int(twoT),
		S:               make(map[string]float64),
	}
}

const sucessful = 0

func (s *StickySampler) prune() {
	for key, val := range s.S {
		// repeatedly toss coin
		// until coin toss is successful.
		// todo this can probably be derived
		// by looking at how close to 0
		// a number in [0, 1) is.
		for {
			if RandCoin(2) == sucessful {
				break
			}
			// diminish by one for every
			// unsucessful outcome
			val--
			// delete if needed
			if val <= 0 {
				delete(s.S, key)
			} else {
				s.S[key] = val
			}

		}
	}
}

// ItemsAboveThreshold returns a list of items that occur more than threshold, along
// with their frequencies. threshold is in the range [0,1]
func (s *StickySampler) ItemsAboveThreshold(threshold float64) []Entry {
	var results []Entry
	for key, f := range s.S {
		if f >= (threshold-s.errorTolerance)*s.N {
			results = append(results, Entry{Key: key, Frequency: f/s.N + s.support})
		}
	}
	return results
}

// Observe records a new sample
func (s *StickySampler) Observe(key string) {
	s.N++
	count := s.N
	if count > s.t {
		s.t *= 2
		s.R *= 2
		s.prune()
	}
	if _, exists := s.S[key]; !exists {
		// determine if value should be sampled
		shouldSample := Rand() <= 1/s.R
		if !shouldSample {
			return
		}
	}
	s.S[key]++
	return
}
