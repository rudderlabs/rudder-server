package badger

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	svcMetric "github.com/rudderlabs/rudder-go-kit/stats/metric"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

type cacheFixture struct {
	cache *Cache[[]byte]
}

func newCacheFixture(t *testing.T) *cacheFixture {
	t.Helper()
	misc.Init()
	t.Cleanup(config.Reset)
	config.Set("RUDDER_TMPDIR", t.TempDir())
	config.Set("LiveEvent.cache.ttl", "1s")
	s := stats.NewStats(config.Default, logger.Default, svcMetric.Instance)
	e, err := New[[]byte]("test", logger.NewLogger(), s)
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Stop() })
	return &cacheFixture{cache: e}
}

func TestBadgerCache(t *testing.T) {
	t.Run("cache testing", func(t *testing.T) {
		testKey := "test_key"
		testValue1 := []byte("test_value1")
		testValue2 := []byte("test_value2")
		testValue3 := []byte("test_value3")
		testValue4 := []byte("test_value4")

		t.Run("Cache Init", func(t *testing.T) {
			e := newCacheFixture(t).cache
			require.NotNil(t, e.db)
			require.NotZero(t, e.ttl.Load())
		})

		t.Run("Cache update", func(t *testing.T) {
			e := newCacheFixture(t).cache
			require.NoError(t, e.Update(testKey, testValue1))

			val, err := e.Read(testKey)
			require.Len(t, val, 1)
			require.NoError(t, err)
			require.Equal(t, [][]byte{testValue1}, val)

			val, err = e.Read(testKey)
			require.NoError(t, err)
			require.Len(t, val, 0)
		})

		t.Run("Cache readAndPopData", func(t *testing.T) {
			e := newCacheFixture(t).cache
			require.NoError(t, e.Update(testKey, testValue1))
			require.NoError(t, e.Update(testKey, testValue2))
			require.NoError(t, e.Update(testKey, testValue3))
			require.NoError(t, e.Update(testKey, testValue4))
			v, err := e.Read(testKey)
			require.NoError(t, err)
			require.Len(t, v, 4)
			require.Equal(t, [][]byte{testValue1, testValue2, testValue3, testValue4}, v)

			v, err = e.Read(testKey)
			require.NoError(t, err)
			require.Len(t, v, 0)
		})

		t.Run("Cache read when multiple keys exist", func(t *testing.T) {
			e := newCacheFixture(t).cache
			testKey2 := testKey + "_2"
			require.NoError(t, e.Update(testKey2, testValue1))
			require.NoError(t, e.Update(testKey, testValue1))

			require.NoError(t, e.Update(testKey2, testValue2))
			require.NoError(t, e.Update(testKey, testValue2))

			require.NoError(t, e.Update(testKey2, testValue3))
			require.NoError(t, e.Update(testKey, testValue3))

			require.NoError(t, e.Update(testKey2, testValue4))
			require.NoError(t, e.Update(testKey, testValue4))

			require.NoError(t, e.Update(testKey2, testValue1))

			v, err := e.Read(testKey)
			require.NoError(t, err)
			require.Len(t, v, 4)
			require.Equal(t, [][]byte{testValue1, testValue2, testValue3, testValue4}, v)

			v, err = e.Read(testKey)
			require.NoError(t, err)
			require.Len(t, v, 0)

			v, err = e.Read(testKey2)
			require.NoError(t, err)
			require.Len(t, v, 5)
			require.Equal(t, [][]byte{testValue1, testValue2, testValue3, testValue4, testValue1}, v)

			v, err = e.Read(testKey2)
			require.NoError(t, err)
			require.Len(t, v, 0)
		})

		t.Run("should only keep the last x values for the same key", func(t *testing.T) {
			e := newCacheFixture(t).cache
			var values [][]byte
			for i := 0; i <= e.limiter.Load(); i++ {
				value := fmt.Appendf(nil, "test_value_%d", i)
				require.NoError(t, e.Update(testKey, value))
				values = append(values, value)
			}

			v, err := e.Read(testKey)
			require.NoError(t, err)
			require.Len(t, v, e.limiter.Load())

			require.Equal(t, values[1:], v)
		})

		t.Run("Cache expiry", func(t *testing.T) {
			e := newCacheFixture(t).cache
			require.NoError(t, e.Update(testKey, testValue1))
			time.Sleep(e.ttl.Load())
			require.NoError(t, e.Update(testKey, testValue2))
			v, err := e.Read(testKey)
			require.NoError(t, err)
			require.Len(t, v, 1)
			require.ElementsMatch(t, v, [][]byte{testValue2})
		})
	})
}
