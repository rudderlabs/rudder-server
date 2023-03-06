package stats

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTagsType(t *testing.T) {
	tags := Tags{
		"b": "value1",
		"a": "value2",
	}

	t.Run("strings method", func(t *testing.T) {
		for i := 0; i < 100; i++ { // just making sure we are not just lucky with the order
			require.Equal(t, []string{"a", "value2", "b", "value1"}, tags.Strings())
		}
	})

	t.Run("string method", func(t *testing.T) {
		require.Equal(t, "a,value2,b,value1", tags.String())
	})

	t.Run("special character replacement", func(t *testing.T) {
		specialTags := Tags{
			"b:1": "value1:1",
			"a:1": "value2:2",
		}
		require.Equal(t, []string{"a-1", "value2-2", "b-1", "value1-1"}, specialTags.Strings())
	})

	t.Run("empty tags", func(t *testing.T) {
		emptyTags := Tags{}
		require.Nil(t, emptyTags.Strings())
		require.Equal(t, "", emptyTags.String())
	})
}
