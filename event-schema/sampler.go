package event_schema

import (
	"math/rand"
	"sync"
	"time"
)

type ReservoirSample struct {
	reservoirSize int
	currSize      int
	totalCount    int64
	rand          *rand.Rand
	lock          sync.RWMutex
	sampleEvents  []interface{}
}

func NewReservoirSampler(reservoirSize int, currSize int, totalCount int64) (rs *ReservoirSample) {
	reservoirSampler := new(ReservoirSample)
	reservoirSampler.currSize = currSize
	reservoirSampler.reservoirSize = reservoirSize
	reservoirSampler.totalCount = totalCount
	reservoirSampler.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	reservoirSampler.sampleEvents = make([]interface{}, reservoirSize)

	return reservoirSampler
}

func (rs *ReservoirSample) add(item interface{}) {
	if item == nil {
		pkgLogger.Info("Debug : Trying to add an empty event in reservoir sample")
		return
	}
	rs.lock.Lock()
	defer rs.lock.Unlock()

	rs.totalCount++

	if rs.currSize < rs.reservoirSize {
		rs.sampleEvents[rs.currSize] = item
		rs.currSize++
	} else {
		if i := rs.rand.Int63n(rs.totalCount); i < int64(rs.reservoirSize) {
			rs.sampleEvents[i] = item
		}
	}
}

func (rs *ReservoirSample) getSamples() []interface{} {
	rs.lock.RLock()
	defer rs.lock.RUnlock()

	dst := make([]interface{}, rs.currSize)
	_ = copy(dst, rs.sampleEvents[:rs.currSize])
	return dst
}

func (rs *ReservoirSample) getTotalCount() int64 {
	rs.lock.RLock()
	defer rs.lock.RUnlock()

	return rs.totalCount
}
