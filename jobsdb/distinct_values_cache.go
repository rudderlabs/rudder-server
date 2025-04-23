package jobsdb

import (
	"sync"

	"github.com/samber/lo"

	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
)

func NewDistinctValuesCache() *distinctValuesCache {
	return &distinctValuesCache{
		cache: make(map[string]map[string][]string),
		klock: kitsync.NewPartitionLocker(),
	}
}

type distinctValuesCache struct {
	cacheMu sync.RWMutex
	// key, dataset
	cache map[string]map[string][]string
	klock *kitsync.PartitionLocker
}

// GetDistinctValues returns the distinct values for the given key and datasets. If the values are
// already cached, it returns the cached values. If not, it loads the values from the given load
// function and caches them. The last dataset is never cached, so it is always loaded from the
// load function. The load function is called with the missing datasets and the last dataset.
func (dpc *distinctValuesCache) GetDistinctValues(key string, datasets []string, load func(datasets []string) (map[string][]string, error)) ([]string, error) {
	// First check if we are missing any datasets from the cache.
	// If we are, we need to load them along with the last dataset
	// The last dataset is never cached, so we need to load it every time
	dpc.cacheMu.RLock()
	missing := dpc.missing(key, datasets[:len(datasets)-1])
	dpc.cacheMu.RUnlock()

	// If we are missing any datasets, we need to lock the key, so that
	// we don't load the same datasets multiple times for the same key.
	// This lock needs to be retained until the datasets are loaded into the cache.
	if len(missing) > 0 {
		dpc.klock.Lock(key)
		// Check again if we are missing any datasets, to deal with race conditions
		dpc.cacheMu.Lock()
		missing = dpc.missing(key, datasets[:len(datasets)-1])
		if _, ok := dpc.cache[key]; !ok {
			dpc.cache[key] = make(map[string][]string)
		}
		dpc.cacheMu.Unlock()
		if len(missing) == 0 {
			// If we are not missing any datasets, we need to unlock the key
			dpc.klock.Unlock(key)
		}
	}

	// Load all the missing datasets along with the last dataset
	results, err := load(append(missing, datasets[len(datasets)-1]))
	if err != nil {
		return nil, err
	}
	// if we were missing any datasets, we need to add them to the cache and unlock the key
	if len(missing) > 0 {
		dpc.cacheMu.Lock()
		for _, ds := range missing {
			dpc.cache[key][ds] = results[ds]
		}
		dpc.cacheMu.Unlock()
		dpc.klock.Unlock(key)
	}

	// Now we need to get values for all the datasets requested so that we can calculate
	// the distinct values.
	// We already have some values in the results map (last dataset & missing), so we only need to fill in
	// the rest of the datasets from the cache.
	dpc.cacheMu.RLock()
	for _, ds := range datasets {
		if _, ok := results[ds]; !ok {
			results[ds] = dpc.cache[key][ds]
		}
	}
	dpc.cacheMu.RUnlock()

	// Calculating distinct values is easy, we just need to
	// iterate over all the datasets and add them to a map
	// and then return the keys of the map.
	distinctValues := make(map[string]struct{})
	for _, ds := range results {
		for _, v := range ds {
			distinctValues[v] = struct{}{}
		}
	}
	return lo.Keys(distinctValues), nil
}

// RemoveDataset removes the dataset from the cache for all keys.
func (dpc *distinctValuesCache) RemoveDataset(dataset string) {
	dpc.cacheMu.Lock()
	defer dpc.cacheMu.Unlock()
	for key := range dpc.cache {
		delete(dpc.cache[key], dataset)
	}
}

func (dpc *distinctValuesCache) missing(key string, datasets []string) []string {
	var missing []string
	dscache, ok := dpc.cache[key]
	if !ok {
		return datasets
	}
	for _, dataset := range datasets {
		if _, ok := dscache[dataset]; !ok {
			missing = append(missing, dataset)
		}
	}
	return missing
}
