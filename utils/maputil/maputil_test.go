package maputil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompare(t *testing.T) {
	t.Run("strings", func(t *testing.T) {
		require.True(t, Compare(map[string]string{"a": "b"}, map[string]string{"a": "b"}))
		require.False(t, Compare(map[string]string{"a": "b"}, map[string]string{"a": "d"}))
		require.False(t, Compare(map[string]string{"a": "b"}, map[string]string{}))
		require.False(t, Compare(map[string]string{}, map[string]string{"a": "b"}))
	})
	t.Run("int", func(t *testing.T) {
		require.True(t, Compare(map[int]int{0: 1}, map[int]int{0: 1}))
		require.False(t, Compare(map[int]int{0: 1}, map[int]int{0: 2}))
		require.False(t, Compare(map[int]int{0: 1}, map[int]int{}))
		require.False(t, Compare(map[int]int{}, map[int]int{0: 1}))
	})
}
