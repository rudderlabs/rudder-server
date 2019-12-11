package restrictor

//go:generate protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogoslick_out=. limiter.proto

import (
	"math"
	"time"
)

// NewLimiter creates a limiter
func NewLimiter() *Limiter {
	return &Limiter{FullUntil: 0, Buckets: make(map[uint32]uint32)}
}

// LimitReached checks whether limits reached
// side effect: the limiter itself is modified if limit is not reached yet
func (lmt *Limiter) LimitReached(window, upperLimit, interval uint32,
	now time.Time) (reached bool, lmtChanged bool, expireChanged bool) {
	if upperLimit == 0 {
		return true, false, false
	}

	ts := uint32(now.Unix())
	if ts < lmt.FullUntil { // total == upperLimit
		return true, false, false
	}

	boundary := ts - window

	total := uint32(0)
	oldest := uint32(math.MaxUint32)
	// remove old useless buckets outside window
	for t, count := range lmt.Buckets {
		if t <= boundary {
			delete(lmt.Buckets, t)
		} else {
			if oldest > t {
				oldest = t
			}
			total += count
		}
	}

	// limit not reached yet
	if total < upperLimit {
		// reset 'FullUntil'
		lmt.FullUntil = 0
		// normalized timestamp, because we only use a limited number of buckets
		normalizedTS := ts - (ts % interval)
		// update bucket count
		lmt.Buckets[normalizedTS]++
		return false, true, true
	}

	// blcoked until 'FullUntil'
	lmt.FullUntil = oldest + window
	return true, true, false
}
