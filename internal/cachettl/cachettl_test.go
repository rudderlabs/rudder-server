package cachettl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCacheTTL(t *testing.T) {
	now := time.Now()

	c := New()
	c.now = func() time.Time { return now }

	// nothing done so far, we expect the cache to be empty
	require.Nil(t, c.Get("one"))
	require.Nil(t, c.slice())

	// insert the very first value
	c.Put("two", "222", 2)
	require.Equal(t, "222", c.Get("two"))
	require.Equal(t, []interface{}{"222"}, c.slice())

	// insert the second value with an expiration higher than the first one
	c.Put("three", "333", 3)
	require.Equal(t, "333", c.Get("three"))
	require.Equal(t, []interface{}{"222", "333"}, c.slice())

	// insert the third value with an expiration lower than all other values
	c.Put("one", "111", 1)
	require.Equal(t, "111", c.Get("one"))
	require.Equal(t, []interface{}{"111", "222", "333"}, c.slice())

	// update "111" to have a higher expiration than all values
	c.Put("one", "111", 4)
	require.Equal(t, "111", c.Get("one"))
	require.Equal(t, []interface{}{"222", "333", "111"}, c.slice())

	// update "333" to have a higher expiration than all values
	c.Put("three", "333", 5)
	require.Equal(t, "333", c.Get("three"))
	require.Equal(t, []interface{}{"222", "111", "333"}, c.slice())

	// move time forward to expire "222"
	c.now = func() time.Time { return now.Add(1) } // "222" should still be there
	require.Nil(t, c.Get("whatever"))              // trigger the cleanup
	require.Equal(t, []interface{}{"222", "111", "333"}, c.slice())

	c.now = func() time.Time { return now.Add(2) } // "222" should still be there
	require.Nil(t, c.Get("whatever"))              // trigger the cleanup
	require.Equal(t, []interface{}{"222", "111", "333"}, c.slice())

	c.now = func() time.Time { return now.Add(3) } // "222" should be expired!
	require.Nil(t, c.Get("whatever"))              // trigger the cleanup
	require.Equal(t, []interface{}{"111", "333"}, c.slice())

	// let's move a lot forward to expire everything
	c.now = func() time.Time { return now.Add(6) }
	require.Nil(t, c.Get("whatever")) // trigger the cleanup
	require.Nil(t, c.slice())
	require.Len(t, c.m, 0)

	// now let's set a key, then move forward and get it directly without triggering with a different key
	c.now = func() time.Time { return now }
	c.Put("last", "999", 1)
	require.Equal(t, "999", c.Get("last"))
	require.Equal(t, []interface{}{"999"}, c.slice())
	c.now = func() time.Time { return now.Add(2) }
	require.Nil(t, c.Get("last")) // trigger the cleanup
	require.Nil(t, c.slice())
	require.Len(t, c.m, 0)
}
