package memory

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

func TestMemoryCache(t *testing.T) {
	t.Run("cache testing", func(t *testing.T) {
		testKey := "test_key"
		testValue := []byte("test_value")

		t.Run("Cache init", func(t *testing.T) {
			var c *Cache[[]byte]
			c, err := New[[]byte]()
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Stop() })
			require.NotZero(t, c.size.Load())
			require.NotZero(t, c.keyTTL.Load())
			require.NotNil(t, c.cacheMap)
		})

		t.Run("Cache update", func(t *testing.T) {
			var c *Cache[[]byte]
			c, err := New[[]byte]()
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Stop() })

			require.NoError(t, c.Update(testKey, testValue))
			require.Len(t, c.cacheMap, 1)
			require.Len(t, c.cacheMap[testKey].data, 1)
			require.Equal(t, testValue, c.cacheMap[testKey].data[0])
		})

		t.Run("Cache timeout", func(t *testing.T) {
			var c *Cache[[]byte]
			config.Set("LiveEvent.cache.ttl", 10*time.Millisecond)
			config.Set("LiveEvent.cache.clearFreq", 10*time.Millisecond)
			t.Cleanup(config.Reset)
			c, err := New[[]byte]()
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Stop() })
			require.NoError(t, c.Update(testKey, testValue))
			// the cleanup goroutine mutates cacheMap concurrently
			c.lock.RLock()
			size := len(c.cacheMap)
			item := c.cacheMap[testKey]
			c.lock.RUnlock()
			require.Equal(t, 1, size)
			require.NotNil(t, item)
			require.Equal(t, [][]byte{testValue}, item.data)
			require.Eventually(t, func() bool {
				c.lock.RLock()
				defer c.lock.RUnlock()
				return len(c.cacheMap) == 0
			}, time.Second, 10*time.Millisecond)
		})

		t.Run("Cache readAndPopData", func(t *testing.T) {
			var c *Cache[[]byte]
			c, err := New[[]byte]()
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Stop() })
			require.NoError(t, c.Update(testKey, testValue))
			v, err := c.Read(testKey)
			require.Equal(t, [][]byte{testValue}, v)
			require.NoError(t, err)
			require.Len(t, c.cacheMap, 0)
		})

		t.Run("Cache data store limit", func(t *testing.T) {
			var c *Cache[[]byte]
			c, err := New[[]byte]()
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Stop() })
			testValue2 := []byte("test_value2")
			testValue3 := []byte("test_value3")
			require.NoError(t, c.Update(testKey, testValue))
			require.NoError(t, c.Update(testKey, testValue2))
			require.NoError(t, c.Update(testKey, testValue3))
			require.NoError(t, c.Update(testKey, testValue3))
			require.Len(t, c.cacheMap, 1)
			require.Len(t, c.cacheMap[testKey].data, 3)
			require.Equal(t, testValue2, c.cacheMap[testKey].data[0])
			require.Equal(t, testValue3, c.cacheMap[testKey].data[1])
		})
	})
}
