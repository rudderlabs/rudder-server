package workerpool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

func TestInternalWorker(t *testing.T) {
	t.Run("ping and stop a productive worker", func(t *testing.T) {
		mw := &internalmockWorker{
			work: true,
		}
		w := newInternalWorker("1", logger.NOP, mw)

		w.Ping()
		require.Eventually(t, func() bool { return mw.worked }, 200*time.Millisecond, 1*time.Millisecond, "worker's work should have been called")
		require.False(t, mw.stopped, "worker should not be stopped")

		w.Stop()
		require.True(t, mw.stopped, "worker should have been stopped")
	})

	t.Run("ping and stop a non-productive worker", func(t *testing.T) {
		mw := &internalmockWorker{
			minSleep: 2 * time.Second,
			maxSleep: 2 * time.Second,
		}
		w := newInternalWorker("1", logger.NOP, mw)

		w.Ping()
		require.Eventually(t, func() bool { return mw.worked }, 200*time.Millisecond, 1*time.Millisecond, "worker's work should have been called")
		require.False(t, mw.stopped, "worker should not be stopped")

		w.Stop()
		require.True(t, mw.stopped, "worker should have been stopped")
	})

	t.Run("try to ping a stopped worker", func(t *testing.T) {
		mw := &internalmockWorker{}
		w := newInternalWorker("1", logger.NOP, mw)
		w.Stop()
		require.False(t, mw.worked, "worker should not have worked")
		require.True(t, mw.stopped, "worker should have been stopped")

		w.Ping()
		require.Never(t, func() bool { return mw.worked }, 100*time.Millisecond, 1*time.Millisecond, "worker should not have worked if ping was called after stop")
		require.True(t, mw.stopped, "worker should still be stopped")
	})

	t.Run("try to stop a stopped worker", func(t *testing.T) {
		mw := &internalmockWorker{}
		w := newInternalWorker("1", logger.NOP, mw)
		w.Stop()
		require.False(t, mw.worked, "worker should not have worked")
		require.True(t, mw.stopped, "worker should have been stopped")

		w.Stop()
		require.False(t, mw.worked, "worker should not have worked")
		require.True(t, mw.stopped, "worker should have been stopped")

		w.Ping()
		require.Never(t, func() bool { return mw.worked }, 100*time.Millisecond, 1*time.Millisecond, "worker should not have worked if ping was called after stop")
		require.True(t, mw.stopped, "worker should still be stopped")
	})
}

type internalmockWorker struct {
	worked   bool
	stopped  bool
	work     bool
	minSleep time.Duration
	maxSleep time.Duration
}

func (mw *internalmockWorker) Work() bool {
	mw.worked = true
	return mw.work
}

func (mw *internalmockWorker) SleepDurations() (min, max time.Duration) {
	return mw.minSleep, mw.maxSleep
}

func (mw *internalmockWorker) Stop() {
	mw.stopped = true
}
