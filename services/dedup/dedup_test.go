package dedup_test

import (
	"context"
	"os"
	"os/exec"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/services/dedup"
	"github.com/rudderlabs/rudder-server/services/dedup/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func Test_Dedup(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := t.TempDir()
	conf := config.New()
	t.Setenv("RUDDER_TMPDIR", dbPath)

	d, err := dedup.New(conf, stats.Default)
	require.Nil(t, err)
	defer d.Close()

	t.Run("key a not present in cache and badger db", func(t *testing.T) {
		key := dedup.SingleKey("a")
		found, err := d.Allowed(key)
		require.NoError(t, err)
		require.Equal(t, true, found[key])

		// Checking it again should give us the previous value from the cache
		found, err = d.Allowed(key)
		require.Nil(t, err)
		require.Equal(t, false, found[key])
	})

	t.Run("key a gets committed", func(t *testing.T) {
		key := dedup.SingleKey("b")
		found, err := d.Allowed(key)
		require.Nil(t, err)
		require.Equal(t, true, found[key])

		err = d.Commit([]string{"a"})
		require.NoError(t, err)

		found, err = d.Allowed(key)
		require.Nil(t, err)
		require.Equal(t, false, found[key])
	})

	t.Run("committing a key not present in committed list", func(t *testing.T) {
		key := dedup.SingleKey("c")
		found, err := d.Allowed(key)
		require.Nil(t, err)
		require.Equal(t, true, found[key])

		err = d.Commit([]string{"d"})
		require.NotNil(t, err)
	})

	t.Run("unique keys", func(t *testing.T) {
		kvs := []types.BatchKey{
			{Index: 0, Key: "e"},
			{Index: 1, Key: "f"},
			{Index: 2, Key: "g"},
		}
		found, err := d.Allowed(kvs...)
		require.Nil(t, err)
		for _, kv := range kvs {
			require.Equal(t, true, found[kv])
		}
		err = d.Commit([]string{"e", "f", "g"})
		require.NoError(t, err)
	})

	t.Run("non-unique keys", func(t *testing.T) {
		kvs := []types.BatchKey{
			{Index: 0, Key: "g"},
			{Index: 1, Key: "h"},
			{Index: 2, Key: "h"},
		}
		expected := map[types.BatchKey]bool{
			kvs[0]: false,
			kvs[1]: true,
			kvs[2]: false,
		}
		found, err := d.Allowed(kvs...)
		require.Nil(t, err)
		for _, kv := range kvs {
			require.Equal(t, expected[kv], found[kv])
		}
		err = d.Commit([]string{"h"})
		require.NoError(t, err)
	})
}

func Test_Dedup_Window(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := os.TempDir() + "/dedup_test"
	conf := config.New()
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)
	conf.Set("Dedup.dedupWindow", "1s")
	t.Setenv("RUDDER_TMPDIR", dbPath)
	d, err := dedup.New(conf, stats.Default)
	require.Nil(t, err)
	defer d.Close()

	k := dedup.SingleKey("to be deleted")
	found, err := d.Allowed(k)
	require.Nil(t, err)
	require.Equal(t, true, found[k])

	err = d.Commit([]string{k.Key})
	require.NoError(t, err)

	found, err = d.Allowed(k)
	require.Nil(t, err)
	require.Equal(t, false, found[k])

	require.Eventually(t, func() bool {
		found, err = d.Allowed(k)
		require.Nil(t, err)
		return found[k]
	}, 2*time.Second, 100*time.Millisecond)
}

func Test_Dedup_ErrTxnTooBig(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := os.TempDir() + "/dedup_test_errtxntoobig"
	defer func() { _ = os.RemoveAll(dbPath) }()
	conf := config.New()
	t.Setenv("RUDDER_TMPDIR", dbPath)
	d, err := dedup.New(conf, stats.Default)
	require.Nil(t, err)
	defer d.Close()

	size := 105_000
	messages := make([]string, size)
	for i := 0; i < size; i++ {
		key := uuid.New().String()
		messages[i] = key
		_, _ = d.Allowed(dedup.SingleKey(key))
	}
	err = d.Commit(messages)
	require.NoError(t, err)
}

func Test_Dedup_Race(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()

	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := t.TempDir()
	conf := config.New()
	t.Setenv("RUDDER_TMPDIR", dbPath)

	d, err := dedup.New(conf, stats.Default)
	require.Nil(t, err)
	defer d.Close()

	// warm up by committing some keys
	keys := lo.RepeatBy(10000, func(i int) string { return "warmup" + strconv.Itoa(i) })
	allowed, err := d.Allowed(lo.Map(keys, func(k string, i int) dedup.BatchKey { return dedup.BatchKey{Index: i, Key: k} })...)
	require.NoError(t, err)
	require.NoError(t, d.Commit(lo.Map(lo.Keys(allowed), func(bk dedup.BatchKey, _ int) string { return bk.Key })))
	for range 10000 {
		key := uuid.New().String()
		k := dedup.SingleKey(key)
		concurrency := 20
		g, _ := errgroup.WithContext(context.Background())
		for range concurrency {
			g.Go(func() error {
				allowed, err := d.Allowed(dedup.SingleKey(key))
				if err != nil {
					return err
				}
				if allowed[k] {
					return d.Commit([]string{key})
				}
				return nil
			})
		}
		require.NoError(t, g.Wait())
	}
}

func Benchmark_Dedup(b *testing.B) {
	config.Reset()
	logger.Reset()
	misc.Init()

	dbPath := path.Join("./testdata", "tmp", rand.String(10), "/DB_Benchmark_Dedup")
	b.Logf("using path %s, since tmpDir has issues in macOS\n", dbPath)
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.MkdirAll(dbPath, 0o750)
	conf := config.New()
	b.Setenv("RUDDER_TMPDIR", dbPath)
	d, err := dedup.New(conf, stats.Default)
	require.NoError(b, err)
	b.ResetTimer()
	b.Run("no duplicates", func(b *testing.B) {
		run := func(b *testing.B, loopEvents int, batch bool) {
			name := "single"
			if batch {
				name = "batch"
			}
			name = name + "_" + strconv.Itoa(loopEvents) + "_events_per_batch"
			b.Run(name, func(b *testing.B) {
				rand := uuid.New().String()
				var events int
				var bytes int
				batchKeys := make([]dedup.BatchKey, 0, loopEvents)
				keys := make([]string, 0, loopEvents)
				for range b.N {
					events++
					key := rand + strconv.Itoa(events)
					bytes += len(key)
					keys = append(keys, key)
					batchKeys = append(batchKeys, dedup.BatchKey{Index: len(keys), Key: key})
					if !batch {
						if _, err = d.Allowed(dedup.SingleKey(key)); err != nil {
							b.Errorf("error allowing key %s: %v", key, err)
							b.FailNow()
						}
					}
					if len(keys) == loopEvents || events == b.N { // need to commit
						if batch {
							if _, err = d.Allowed(batchKeys...); err != nil {
								b.Errorf("error allowing keys: %v", err)
								b.FailNow()
							}
						}
						if len(keys) > 0 {
							if err := d.Commit(keys); err != nil {
								b.Errorf("error committing keys: %v", err)
								b.FailNow()
							}
						}

						batchKeys = make([]dedup.BatchKey, 0, loopEvents)
						keys = make([]string, 0, loopEvents)

					}
				}
				b.ReportMetric(float64(events), "events")
				b.ReportMetric(float64(bytes), "bytes")
			})
		}

		for _, events := range []int{1, 10, 100, 1000, 10000} {
			run(b, events, false)
			run(b, events, true)
		}
	})
	d.Close()

	cmd := exec.Command("du", "-sh", dbPath)
	out, err := cmd.Output()
	if err != nil {
		b.Log(err)
	}

	b.Log("db size:", string(out))
}
