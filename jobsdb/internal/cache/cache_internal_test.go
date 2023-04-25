package cache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheEntry(t *testing.T) {
	t.Run("AddToken", func(t *testing.T) {
		e := cacheEntry{}
		for i := 0; i < 10; i++ {
			e.AddToken(fmt.Sprintf("token-%d", i))
			require.Len(t, e.tokens, i+1)
		}
		e.AddToken("token1")
		require.Len(t, e.tokens, 10)
	})

	t.Run("SetNoJobs", func(t *testing.T) {
		e := cacheEntry{}
		token := "token"
		e.AddToken(token)
		require.True(t, e.SetNoJobs(token))
		require.True(t, e.noJobs)
		require.Len(t, e.tokens, 0)
		require.False(t, e.SetNoJobs(token), "it shouldn't be able to set no jobs twice with the same token")
	})
}
