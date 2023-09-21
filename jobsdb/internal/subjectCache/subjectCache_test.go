package subjectcache_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	subjectcache "github.com/rudderlabs/rudder-server/jobsdb/internal/subjectCache"
)

func Test_subjectCache(t *testing.T) {
	t.Run("check for non-existent subject", func(t *testing.T) {
		c := subjectcache.New()
		require.False(t, c.Check("foo.bar"))
	})

	t.Run("check for existing subject", func(t *testing.T) {
		c := subjectcache.New()
		c.SetPreRead("foo.bar")
		require.True(t, c.Check("foo.bar"))
	})

	t.Run("check for existing subject with wildcard", func(t *testing.T) {
		c := subjectcache.New()
		c.SetPreRead("foo.bar")
		require.False(t, c.Check("foo.*"))
	})

	t.Run("check for key is wildcard subject exists", func(t *testing.T) {
		c := subjectcache.New()
		c.SetPreRead("foo.*")
		require.True(t, c.Check("foo.bar"))
	})

	t.Run("Remove only allows subject literals, no wildcards - panics", func(t *testing.T) {
		c := subjectcache.New()
		require.Panics(t, func() {
			c.Remove("foo.*")
		})
		require.Panics(t, func() {
			c.Remove("foo.")
		})
		require.Panics(t, func() {
			c.Remove(".foo")
		})
		require.Panics(t, func() {
			c.Remove("")
		})
		require.Panics(t, func() {
			c.Remove("foo..bar")
		})
	})

	t.Run("should remove a literal subject", func(t *testing.T) {
		c := subjectcache.New()
		// c.SetPreRead("foo.bar")
		c.Remove("foo.bar")
		require.False(t, c.Check("foo.bar"))
	})

	t.Run("a write during read(no jobs) removes the preRead subject", func(t *testing.T) {
		c := subjectcache.New()
		c.SetPreRead("foo.*")              // getJobs will call SetPreRead
		c.Remove("foo.bar")                // write will call Remove
		c.CommitPreRead("foo.*")           // getJobs will call CommitPreRead(no jobs found)
		require.False(t, c.Check("foo.*")) // next iteration of getJobs
	})

	t.Run("a write during read(jobs found) removes the preRead subject", func(t *testing.T) {
		c := subjectcache.New()
		c.SetPreRead("foo.*")              // getJobs will call SetPreRead
		c.Remove("foo.bar")                // write will call Remove
		c.RemovePreRead("foo.*")           // getJobs will call RemovePreRead(jobs found)
		require.False(t, c.Check("foo.*")) // next iteration of getJobs
	})
}
