package jobsdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionedDSList(t *testing.T) {
	t.Run("read pins snapshot until release", func(t *testing.T) {
		initialList := dataSetTList{testVersionedDS("1")}
		initialRanges := dataSetRangeTList{testVersionedRange("1", 1, 10)}
		nextList := dataSetTList{testVersionedDS("2")}
		nextRanges := dataSetRangeTList{testVersionedRange("2", 11, 20)}

		v := newVersionedDSList(initialList, initialRanges)

		list, ranges, version, release := v.get()
		require.Equal(t, uint64(0), version)
		require.Equal(t, initialList, list)
		require.Equal(t, initialRanges, ranges)

		v.set(nextList, nextRanges)

		drained, err := v.wait(version)
		require.NoError(t, err)
		requireNotClosed(t, drained)

		nextReadList, nextReadRanges, nextVersion, nextRelease := v.get()
		require.Equal(t, uint64(1), nextVersion)
		require.Equal(t, nextList, nextReadList)
		require.Equal(t, nextRanges, nextReadRanges)
		nextRelease()

		require.Equal(t, initialList, list)
		require.Equal(t, initialRanges, ranges)
		requireNotClosed(t, drained)

		release()
		requireClosed(t, drained)
	})

	t.Run("wait until drained includes older versions", func(t *testing.T) {
		v := newVersionedDSList(dataSetTList{testVersionedDS("1")}, nil)

		_, _, oldVersion, releaseOld := v.get()
		require.Equal(t, uint64(0), oldVersion)

		v.set(dataSetTList{testVersionedDS("2")}, nil)

		_, _, removalVersion, releaseRemoval := v.get()
		require.Equal(t, uint64(1), removalVersion)

		v.set(dataSetTList{testVersionedDS("3")}, nil)

		drained, err := v.wait(removalVersion)
		require.NoError(t, err)
		requireNotClosed(t, drained)

		releaseRemoval()
		requireNotClosed(t, drained)

		releaseOld()
		requireClosed(t, drained)
	})

	t.Run("wait until drained ignores newer versions", func(t *testing.T) {
		v := newVersionedDSList(dataSetTList{testVersionedDS("1")}, nil)
		v.set(dataSetTList{testVersionedDS("2")}, nil)

		_, _, version, release := v.get()
		require.Equal(t, uint64(1), version)
		defer release()

		drained, err := v.wait(0)
		require.NoError(t, err)
		requireClosed(t, drained)
	})

	t.Run("release is idempotent", func(t *testing.T) {
		v := newVersionedDSList(dataSetTList{testVersionedDS("1")}, nil)

		_, _, version, release := v.get()
		v.set(dataSetTList{testVersionedDS("2")}, nil)

		drained, err := v.wait(version)
		require.NoError(t, err)
		requireNotClosed(t, drained)

		release()
		release()

		requireClosed(t, drained)
	})

	t.Run("wait until drained rejects current and future version", func(t *testing.T) {
		v := newVersionedDSList(dataSetTList{testVersionedDS("1")}, nil)

		drained, err := v.wait(0)
		require.ErrorIs(t, err, errInvalidDSListDrainVersion)
		require.Nil(t, drained)

		drained, err = v.wait(1)
		require.ErrorIs(t, err, errInvalidDSListDrainVersion)
		require.Nil(t, drained)

		v.set(dataSetTList{testVersionedDS("2")}, nil)

		drained, err = v.wait(v.currentVersion())
		require.ErrorIs(t, err, errInvalidDSListDrainVersion)
		require.Nil(t, drained)
	})
}

func testVersionedDS(index string) dataSetT {
	return dataSetT{
		JobTable:       "jobs_" + index,
		JobStatusTable: "job_status_" + index,
		Index:          index,
	}
}

func testVersionedRange(index string, minID, maxID int64) dataSetRangeT {
	return dataSetRangeT{
		minJobID: minID,
		maxJobID: maxID,
		ds:       testVersionedDS(index),
	}
}

func requireClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	default:
		require.Fail(t, "expected channel to be closed")
	}
}

func requireNotClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
		require.Fail(t, "expected channel to remain open")
	default:
	}
}
