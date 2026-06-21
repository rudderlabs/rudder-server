package bqstreamallevents

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestTableSchemaCache(t *testing.T) {
	now := time.Now()
	c := NewTableSchemaCache(time.Minute)

	_, ok := c.Get("pages", now)
	require.False(t, ok)
	require.False(t, c.Has("pages", now))
	_, ok = c.Peek("pages")
	require.False(t, ok)
	require.Equal(t, 0, c.Len())

	schema := whutils.ModelTableSchema{"a": "string"}
	c.Set("pages", schema, now)
	require.True(t, c.Has("pages", now))
	require.Equal(t, 1, c.Len())

	got, ok := c.Get("pages", now)
	require.True(t, ok)
	require.Equal(t, schema, got)

	// mutating the returned schema must not affect the cached copy
	got["b"] = "int"
	again, ok := c.Get("pages", now)
	require.True(t, ok)
	require.NotContains(t, again, "b")

	// expired entries are invisible to Get/Has but still returned by Peek
	expired := now.Add(2 * time.Minute)
	_, ok = c.Get("pages", expired)
	require.False(t, ok)
	require.False(t, c.Has("pages", expired))
	peeked, ok := c.Peek("pages")
	require.True(t, ok)
	require.Equal(t, schema, peeked)

	c.Invalidate("pages")
	require.Equal(t, 0, c.Len())
	_, ok = c.Peek("pages")
	require.False(t, ok)
}
