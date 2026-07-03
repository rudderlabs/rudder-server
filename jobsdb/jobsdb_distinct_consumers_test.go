package jobsdb

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/utils/tx"
)

// TestGetDistinctConsumers covers the consumer-enumeration API across handle modes and datasets.
func TestGetDistinctConsumers(t *testing.T) {
	_ = startPostgres(t)
	ctx := context.Background()

	newJobDB := func(t *testing.T, multiConsumer bool) *Handle {
		t.Helper()
		c := config.New()
		c.Set("JobsDB.maxDSSize", 100000)
		jd := &Handle{
			TriggerAddNewDS:   func() <-chan time.Time { return make(chan time.Time) },
			TriggerCompaction: func() <-chan time.Time { return make(chan time.Time) },
			config:            c,
		}
		jd.conf.multiConsumer = multiConsumer
		require.NoError(t, jd.Setup(ReadWrite, true, strings.ToLower(rand.String(5))))
		t.Cleanup(jd.TearDown)
		return jd
	}

	createDS := func(t *testing.T, jd *Handle, idx string) {
		t.Helper()
		require.NoError(t, jd.dsListLock.WithLockInCtx(ctx, func(l lock.LockToken) error {
			dsList, _ := jd.dsList.snapshot()
			if err := jd.WithTx(ctx, func(txn *tx.Tx) error {
				return jd.addNewDSInTx(ctx, txn, l, dsList, newDataSet(jd.tablePrefix, idx))
			}); err != nil {
				return err
			}
			return jd.doRefreshDSRangeList(l)
		}))
	}

	t.Run("single-consumer handle returns the legacy '' consumer", func(t *testing.T) {
		jd := newJobDB(t, false)
		consumers, err := jd.GetDistinctConsumers(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{""}, consumers)
	})

	t.Run("multi-consumer handle unions registries across datasets", func(t *testing.T) {
		jd := newJobDB(t, true)

		// DS1: consumers {A,B,C}.
		jobs1 := genJobs(defaultWorkspaceID, "mc", 2, 1)
		jobs1[0].Consumers = []string{"A", "B"}
		jobs1[1].Consumers = []string{"C"}
		require.NoError(t, jd.Store(ctx, jobs1))

		// Rotate, then DS2: consumers {B,D} — B overlaps DS1, D is new.
		createDS(t, jd, "2")
		jobs2 := genJobs(defaultWorkspaceID, "mc", 1, 1)
		jobs2[0].Consumers = []string{"B", "D"}
		require.NoError(t, jd.Store(ctx, jobs2))

		consumers, err := jd.GetDistinctConsumers(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"A", "B", "C", "D"}, consumers)
	})

	t.Run("legacy jobs (no Consumers) register the '' consumer", func(t *testing.T) {
		jd := newJobDB(t, true)
		jobs := genJobs(defaultWorkspaceID, "mc", 2, 1) // no explicit Consumers → default '{""}'
		require.NoError(t, jd.Store(ctx, jobs))

		consumers, err := jd.GetDistinctConsumers(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{""}, consumers)
	})

	t.Run("cached older dataset + live latest dataset both reflected", func(t *testing.T) {
		jd := newJobDB(t, true)

		jobs1 := genJobs(defaultWorkspaceID, "mc", 1, 1)
		jobs1[0].Consumers = []string{"A"}
		require.NoError(t, jd.Store(ctx, jobs1))

		// Prime the cache: DS1 ("A") is now cached as a non-latest dataset.
		createDS(t, jd, "2")
		first, err := jd.GetDistinctConsumers(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"A"}, first)

		// A new consumer written to the live latest dataset must still appear, since the
		// latest dataset is never cached.
		jobs2 := genJobs(defaultWorkspaceID, "mc", 1, 1)
		jobs2[0].Consumers = []string{"B"}
		require.NoError(t, jd.Store(ctx, jobs2))

		second, err := jd.GetDistinctConsumers(ctx)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"A", "B"}, second)
	})
}
