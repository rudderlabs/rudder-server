package cache_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/cache"
)

type paramFilter struct {
	name  string
	value string
}

func (p paramFilter) GetName() string {
	return p.name
}

func (p paramFilter) GetValue() string {
	return p.value
}

func TestNoResultsCache(t *testing.T) {
	const (
		dataset   = "dataset"
		workspace = "workspace"
		customVal = "customVal"
		state     = "state"
	)
	supportedParamFilters := []string{"param1", "param2"}
	ttl := 10 * time.Millisecond
	ttlFunc := func() time.Duration {
		return ttl
	}

	t.Run("Prepare, Set, Get", func(t *testing.T) {
		c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, ttlFunc)

		t.Run("calling Get without having set anything", func(t *testing.T) {
			require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}), "it should return false if noresult is not set")
		})

		c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
		require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}), "it should return true if no result is set for valid token")

		t.Run("expiration", func(t *testing.T) {
			require.Eventually(t, func() bool {
				return !c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})
			}, 2*ttl, 1*time.Millisecond, "it should eventually expire after ttl and return false")
		})

		t.Run("A Prepare doesn't affect the existing state", func(t *testing.T) {
			c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
			require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}), "it should return true if no result is set for valid token")

			_ = c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})
			require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}), "it should return true if no result is set for valid token")
		})
	})

	t.Run("Invalidation", func(t *testing.T) {
		t.Run("between StartNoResultTx and SetNoResult", func(t *testing.T) {
			c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, func() time.Duration { return time.Hour })
			tx := c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})
			c.Invalidate(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})
			tx.Commit()
			require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}), "it should return false if invalidation is called before SetNoResult")
		})

		t.Run("after SetNoResult", func(t *testing.T) {
			c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, func() time.Duration { return time.Hour })
			c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
			require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
			c.Invalidate(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})
			require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}), "it should return false after invalidation")
		})

		t.Run("InvalidateDataset", func(t *testing.T) {
			c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, func() time.Duration { return time.Hour })

			// set for 2 workspaces
			c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
			require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))

			c.StartNoResultTx(dataset, "other", []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
			require.True(t, c.Get(dataset, "other", []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))

			// set for another dataset
			c.StartNoResultTx("other", workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
			require.True(t, c.Get("other", workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))

			c.InvalidateDataset(dataset)
			require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
			require.False(t, c.Get(dataset, "other", []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
			require.True(t, c.Get("other", workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
		})

		t.Run("Wildcard", func(t *testing.T) {
			c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, func() time.Duration { return time.Hour })
			c.StartNoResultTx(dataset, "", []string{}, []string{state}, []paramFilter{}).Commit()
			require.True(t, c.Get(dataset, "", []string{}, []string{state}, []paramFilter{}))

			c.Invalidate(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})
			require.False(t, c.Get(dataset, "", []string{}, []string{state}, []paramFilter{}))
		})

		t.Run("Missing information during invalidate causes to invalidate parents", func(t *testing.T) {
			t.Run("no workspace provided", func(t *testing.T) {
				c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, func() time.Duration { return time.Hour })

				// set for 2 workspaces
				c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
				require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))

				c.StartNoResultTx(dataset, "other", []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
				require.True(t, c.Get(dataset, "other", []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))

				// set for another dataset
				c.StartNoResultTx("other", workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
				require.True(t, c.Get("other", workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))

				c.Invalidate(dataset, "", []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})
				require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
				require.False(t, c.Get(dataset, "other", []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
				require.True(t, c.Get("other", workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
			})

			t.Run("no customVal provided", func(t *testing.T) {
				c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, func() time.Duration { return time.Hour })

				// set for 2 customVals
				c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
				require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))

				c.StartNoResultTx(dataset, workspace, []string{"other"}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
				require.True(t, c.Get(dataset, workspace, []string{"other"}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))

				// set for another workspace
				c.StartNoResultTx(dataset, "other", []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
				require.True(t, c.Get(dataset, "other", []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))

				c.Invalidate(dataset, workspace, []string{}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})
				require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
				require.False(t, c.Get(dataset, workspace, []string{"other"}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
				require.True(t, c.Get(dataset, "other", []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
			})

			t.Run("no state provided", func(t *testing.T) {
				c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, func() time.Duration { return time.Hour })

				// set for 2 states
				c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
				require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))

				c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{"other"}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
				require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{"other"}, []paramFilter{{name: "param1", value: "value1"}}))

				// set for another customVal
				c.StartNoResultTx(dataset, workspace, []string{"other"}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
				require.True(t, c.Get(dataset, workspace, []string{"other"}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))

				c.Invalidate(dataset, workspace, []string{customVal}, []string{}, []paramFilter{{name: "param1", value: "value1"}})
				require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
				require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{"other"}, []paramFilter{{name: "param1", value: "value1"}}))
				require.True(t, c.Get(dataset, workspace, []string{"other"}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
			})

			t.Run("no param provided", func(t *testing.T) {
				c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, func() time.Duration { return time.Hour })

				// set for 2 params
				c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
				require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))

				c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param2", value: "value1"}}).Commit()
				require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param2", value: "value1"}}))

				// set for another state
				c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{"other"}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
				require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{"other"}, []paramFilter{{name: "param1", value: "value1"}}))

				c.Invalidate(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{})
				require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
				require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param2", value: "value1"}}))
				require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{"other"}, []paramFilter{{name: "param1", value: "value1"}}))
			})
		})

		t.Run("Missing noResult keys", func(t *testing.T) {
			c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, func() time.Duration { return time.Hour })
			c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
			require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))

			t.Run("different dataset", func(t *testing.T) {
				c.Invalidate("other", workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})
				require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
			})

			t.Run("different workspace", func(t *testing.T) {
				c.Invalidate(dataset, "other", []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})
				require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
			})

			t.Run("different customVal", func(t *testing.T) {
				c.Invalidate(dataset, workspace, []string{"other"}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})
				require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
			})

			t.Run("different state", func(t *testing.T) {
				c.Invalidate(dataset, workspace, []string{customVal}, []string{"other"}, []paramFilter{{name: "param1", value: "value1"}})
				require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
			})

			t.Run("different param", func(t *testing.T) {
				c.Invalidate(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param2", value: "value1"}})
				require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
			})
		})
	})

	t.Run("Ignore caching", func(t *testing.T) {
		c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, ttlFunc)

		t.Run("no states are used", func(t *testing.T) {
			c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
			require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{}, []paramFilter{{name: "param1", value: "value1"}}))
		})

		t.Run("invalid parameters are used", func(t *testing.T) {
			c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}, {name: "param3", value: "value3"}}).Commit()
			require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}, {name: "param3", value: "value3"}}))
		})
	})

	t.Run("Get exceptions", func(t *testing.T) {
		c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, ttlFunc)
		c.StartNoResultTx(dataset, "", []string{}, []string{state}, []paramFilter{}).Commit()

		t.Run("using an non-existent workspace", func(t *testing.T) {
			require.False(t, c.Get(dataset, "other", []string{}, []string{state}, []paramFilter{}))
		})

		t.Run("using an non-existent customVal", func(t *testing.T) {
			require.False(t, c.Get(dataset, "", []string{"other"}, []string{state}, []paramFilter{}))
		})

		t.Run("using an non-existent state", func(t *testing.T) {
			require.False(t, c.Get(dataset, "", []string{}, []string{"other"}, []paramFilter{}))
		})

		t.Run("using a valid but non-existent param", func(t *testing.T) {
			require.False(t, c.Get(dataset, "", []string{}, []string{state}, []paramFilter{{name: "param2", value: "value1"}}))
		})
	})

	t.Run("Set exceptions", func(t *testing.T) {
		c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, ttlFunc)
		c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})

		t.Run("using an non-existent workspace", func(t *testing.T) {
			tx := c.StartNoResultTx(dataset, "other", []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})
			c.Invalidate(dataset, "other", nil, nil, nil)
			tx.Commit()
			require.False(t, c.Get(dataset, "other", []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
		})

		t.Run("using an non-existent customVal", func(t *testing.T) {
			tx := c.StartNoResultTx(dataset, workspace, []string{"other"}, []string{state}, []paramFilter{{name: "param1", value: "value1"}})
			c.Invalidate(dataset, workspace, []string{"other"}, nil, nil)
			tx.Commit()
			require.False(t, c.Get(dataset, workspace, []string{"other"}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
		})

		t.Run("using an non-existent state", func(t *testing.T) {
			tx := c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{"other"}, []paramFilter{{name: "param1", value: "value1"}})
			c.Invalidate(dataset, workspace, []string{customVal}, []string{"other"}, nil)
			tx.Commit()
			require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{"other"}, []paramFilter{{name: "param1", value: "value1"}}))
		})

		t.Run("using a valid but non-existent param", func(t *testing.T) {
			tx := c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param2", value: "value1"}})
			c.Invalidate(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param2", value: "value1"}})
			tx.Commit()
			require.False(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param2", value: "value1"}}))
		})
	})

	t.Run("String", func(t *testing.T) {
		c := cache.NewNoResultsCache[paramFilter](supportedParamFilters, ttlFunc)
		c.StartNoResultTx(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}).Commit()
		require.True(t, c.Get(dataset, workspace, []string{customVal}, []string{state}, []paramFilter{{name: "param1", value: "value1"}}))
		require.Contains(t, c.String(), "map[dataset:map[workspace:map[customVal:map[state:map[param1:value1:{noJobs:true tokens:[] t:")
	})
}
