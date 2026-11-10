package batchrouter

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func TestAsyncBatch(t *testing.T) {
	misc.Init()
	newBatch := func(t *testing.T, limits asyncBatchLimits) *asyncBatch {
		b := &asyncBatch{destinationID: "dest-1", limits: limits}
		t.Cleanup(b.removeFile)
		return b
	}
	job := func(id int64) *jobsdb.JobT {
		return &jobsdb.JobT{JobID: id}
	}
	add := func(t *testing.T, b *asyncBatch, id int64, line string) bool {
		t.Helper()
		ok, err := b.add(job(id), line)
		require.NoError(t, err)
		return ok
	}

	t.Run("it is full at its jobs limit", func(t *testing.T) {
		b := newBatch(t, asyncBatchLimits{jobs: 2, bytes: 100})
		require.True(t, add(t, b, 1, "a\n"))
		require.False(t, b.full)
		b.closeFile() // the next job is appended
		require.True(t, add(t, b, 2, "b\n"))
		require.True(t, b.full)
		require.False(t, add(t, b, 3, "c\n"))
		require.Equal(t, []int64{1, 2}, jobIDs(b.jobs))
		b.closeFile()
		content, err := os.ReadFile(b.fileName)
		require.NoError(t, err)
		require.Equal(t, "a\nb\n", string(content))
	})

	t.Run("it is full at its bytes limit", func(t *testing.T) {
		b := newBatch(t, asyncBatchLimits{jobs: 10, bytes: 4})
		require.True(t, add(t, b, 1, "a\n"))
		require.True(t, add(t, b, 2, "b\n"))
		require.True(t, b.full)
	})

	t.Run("a job that doesn't fit leaves it full, so that no later job is taken before it", func(t *testing.T) {
		b := newBatch(t, asyncBatchLimits{jobs: 10, bytes: 5})
		require.True(t, add(t, b, 1, "a\n"))
		require.False(t, add(t, b, 2, "large\n"))
		require.True(t, b.full)
		require.False(t, add(t, b, 3, "c\n"))
		require.Equal(t, []int64{1}, jobIDs(b.jobs))
	})

	t.Run("it takes its first job, even if the job is larger than the limit", func(t *testing.T) {
		b := newBatch(t, asyncBatchLimits{jobs: 10, bytes: 2})
		require.True(t, add(t, b, 1, "large\n"))
		require.True(t, b.full)
	})

	t.Run("without a jobs limit, only its bytes are limited", func(t *testing.T) {
		b := newBatch(t, asyncBatchLimits{bytes: 100})
		for id := range int64(20) {
			require.True(t, add(t, b, id, "a\n"))
		}
		require.False(t, b.full)
	})

	t.Run("it is ready when it is full or old enough, but never while empty", func(t *testing.T) {
		b := newBatch(t, asyncBatchLimits{jobs: 2, bytes: 100})
		require.False(t, b.ready(0))
		require.True(t, add(t, b, 1, "a\n"))
		require.False(t, b.ready(time.Hour))
		require.True(t, b.ready(0))
		require.True(t, add(t, b, 2, "b\n"))
		require.True(t, b.ready(time.Hour))
	})
}
