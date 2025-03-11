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

	t.Run("Tokens limit reached", func(t *testing.T) {
		e := cacheEntry{}
		for i := 0; i < 14; i++ { // add 14 tokens
			e.AddToken(fmt.Sprintf("token-%d", i))
		}

		// Should not be able to set no jobs for the first 4 tokens
		require.False(t, e.SetNoJobs("token-0"))
		require.False(t, e.SetNoJobs("token-1"))
		require.False(t, e.SetNoJobs("token-2"))
		require.False(t, e.SetNoJobs("token-3"))

		// Should be able to set no jobs for the last 4 tokens
		require.True(t, e.SetNoJobs("token-13"))
		e.AddToken("token-14")
		require.True(t, e.SetNoJobs("token-4"), "after setting no jobs, token gets removed from the list freeing up space")
		require.True(t, e.SetNoJobs("token-12"))
		require.True(t, e.SetNoJobs("token-11"))
		require.True(t, e.SetNoJobs("token-10"))
	})
}
