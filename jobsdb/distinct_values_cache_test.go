package jobsdb

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestGetDistinctValues(t *testing.T) {
	c := NewDistinctValuesCache()
	valuesMap := map[string][]string{
		"ds-1": {"a"},
		"ds-2": {"b"},
	}
	loadFunc := func(datasets []string) (map[string][]string, error) {
		return lo.PickByKeys(valuesMap, datasets), nil
	}
	res, err := c.GetDistinctValues("key", []string{"ds-1", "ds-2"}, loadFunc)
	require.NoError(t, err)
	require.ElementsMatch(t, res, []string{"a", "b"})

	// if the actual value were to change in the source of the information, we would lose it
	// the above would be true for all but the last dataset in the call provided to `GetDistinctValues`
	valuesMap["ds-1"] = []string{"a", "c"}
	res, err = c.GetDistinctValues("key", []string{"ds-1", "ds-2"}, loadFunc)
	require.NoError(t, err)
	require.ElementsMatch(t, res, []string{"a", "b"})
	// if the value for last dataset changes, this would always reflect in the result
	valuesMap["ds-2"] = []string{"b", "d"}
	res, err = c.GetDistinctValues("key", []string{"ds-1", "ds-2"}, loadFunc)
	require.NoError(t, err)
	require.ElementsMatch(t, res, []string{"a", "b", "d"})

	// only one dataset, eventhough it's already cached - it'll be looked up from source again
	res, err = c.GetDistinctValues("key", []string{"ds-1"}, loadFunc)
	require.NoError(t, err)
	require.ElementsMatch(t, res, []string{"a", "c"})
}

func TestConcurrentGetDistinctValues(t *testing.T) {
	type sourceStore struct {
		lookups          *atomic.Int32
		perDSLookupCount *atomic.Int32
		info             *sync.Map
	}

	source := &sourceStore{
		lookups:          new(atomic.Int32),
		perDSLookupCount: new(atomic.Int32),
		info:             new(sync.Map),
	}
	for i := 0; i < 100; i++ {
		source.info.Store(strconv.Itoa(i), []string{"a-" + strconv.Itoa(i)})
	}

	c := NewDistinctValuesCache()
	loadFunc := func(datasets []string) (map[string][]string, error) {
		_ = source.lookups.Add(1)
		res := make(map[string][]string)
		for _, dataset := range datasets {
			val, ok := source.info.Load(dataset)
			_ = source.perDSLookupCount.Add(1)
			if !ok {
				return nil, fmt.Errorf("%s", "not present")
			}
			res[dataset] = val.([]string)
		}
		return res, nil
	}
	wg := new(sync.WaitGroup)
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			_, err := c.GetDistinctValues("paramKey", []string{"1", "2"}, loadFunc)
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	// lookup is always called for the last dataset
	require.Equal(t, int32(1000), source.lookups.Load())
	// first DS is only looked up once - the first time
	// actually all except first DS - here 1 considering we lookup only 2 datasets
	require.Equal(t, int32(1001), source.perDSLookupCount.Load())

	source.lookups.Store(0)
	source.perDSLookupCount.Store(0)
	c.RemoveDataset("1")
	c.RemoveDataset("2")

	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			_, err := c.GetDistinctValues("paramKey", []string{"1", "2", "3"}, loadFunc)
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1000), source.lookups.Load())
	require.Equal(t, int32(1002), source.perDSLookupCount.Load())
}
