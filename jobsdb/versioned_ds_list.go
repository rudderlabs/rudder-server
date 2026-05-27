package jobsdb

import (
	"errors"
	"fmt"
	"sync"
)

var errInvalidDSListDrainVersion = errors.New("drain version must be older than current version")

func newVersionedDSList(list dataSetTList, rangeList dataSetRangeTList) *versionedDSList {
	return &versionedDSList{
		list:      list,
		rangeList: rangeList,
		readers:   make(map[uint64]int),
	}
}

type versionedDSList struct {
	mu sync.Mutex

	// version is the version assigned to readers reading the currently published snapshots.
	version uint64

	// list and rangeList are the currently published immutable snapshots.
	// They are owned by versionedDSList so read and update cannot drift apart.
	list      dataSetTList
	rangeList dataSetRangeTList

	// readers counts readers per version. A missing key means no reader is using that version.
	readers map[uint64]int

	// waiters are operations waiting for all readers at or before a version to drain.
	waiters []dsListDrainWaiter
}

type dsListDrainWaiter struct {
	through uint64        // through is inclusive: a drop for version N must wait for readers in versions 0..N.
	drained chan struct{} // closed when there are no more readers at or before through
}

// get returns the currently published snapshots and a release function that must be called when the reader is done with the snapshots.
func (v *versionedDSList) get() (list dataSetTList, ranges dataSetRangeTList, version uint64, release func()) {
	v.mu.Lock()
	version = v.version
	v.readers[version]++
	list = v.list
	ranges = v.rangeList
	v.mu.Unlock()

	var once sync.Once
	return list, ranges, version, func() {
		once.Do(func() {
			v.mu.Lock()
			defer v.mu.Unlock()

			if v.readers[version] <= 1 {
				delete(v.readers, version)
			} else {
				v.readers[version]--
			}
			v.closeDrainedLocked()
		})
	}
}

func (v *versionedDSList) snapshot() (dataSetTList, dataSetRangeTList) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.list, v.rangeList
}

func (v *versionedDSList) currentVersion() uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.version
}

func (v *versionedDSList) set(list dataSetTList, ranges dataSetRangeTList) {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.list = list
	v.rangeList = ranges
	v.version++
	v.closeDrainedLocked()
}

// wait returns a channel that will be closed when there are no more readers at or before through. through must be less than the current version.
func (v *versionedDSList) wait(through uint64) (<-chan struct{}, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if through >= v.version {
		return nil, fmt.Errorf("%w: through=%d current=%d", errInvalidDSListDrainVersion, through, v.version)
	}

	drained := make(chan struct{})
	v.waiters = append(v.waiters, dsListDrainWaiter{through: through, drained: drained})
	v.closeDrainedLocked()
	return drained, nil
}

// closeDrainedLocked closes the drained channels of all waiters that are waiting for versions that have no more readers. It must be called with v.mu held.
func (v *versionedDSList) closeDrainedLocked() {
	hasReaders := func(through uint64) bool {
		for version, count := range v.readers {
			if version <= through && count > 0 {
				return true
			}
		}
		return false
	}
	kept := v.waiters[:0]
	for _, w := range v.waiters {
		if hasReaders(w.through) {
			kept = append(kept, w)
			continue
		}
		close(w.drained)
	}
	v.waiters = kept
}
