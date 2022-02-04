package jobsdb

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type cacheValue string

const (
	hasJobs         cacheValue = "Has Jobs"
	noJobs          cacheValue = "No Jobs"
	dropDSFromCache cacheValue = "Drop DS From Cache"
	/*
	* willTryToSet value is used to prevent wrongly setting empty result when
	* a db update (new jobs or job status updates) happens during get(Un)Processed db query is in progress.
	*
	* getUnprocessedJobs() {  # OR getProcessedJobsDS
	* 0. Sets cache value to willTryToSet
	* 1. out = queryDB()
	* 2. check and set cache to (len(out) == 0) only if cache value is willTryToSet
	* }
	 */
	willTryToSet cacheValue = "Query in progress"
)

type cacheEntry struct {
	Value cacheValue `json:"value"`
	T     time.Time  `json:"set_at"`
}

type CacheEmptyDS struct {
	queryFilterKeys    QueryFiltersT
	lock               sync.Mutex
	dsEmptyResultCache map[dataSetT]map[string]map[string]map[string]map[string]cacheEntry //DS -> customer -> customVal -> params -> state -> cacheEntry
	dsCacheLock        sync.Mutex
}

/*
* If a query returns empty result for a specific dataset, we cache that so that
* future queries don't have to hit the DB.
* MarkEmpty() when mark=True marks dataset,customVal,state as empty.
* MarkEmpty() when mark=False clears a previous empty mark
 */

func (jd *CacheEmptyDS) MarkEmpty(ds dataSetT, customer string, stateFilters []string, customValFilters []string, parameterFilters []ParameterFilterT, value cacheValue, checkAndSet *cacheValue) {

	jd.dsCacheLock.Lock()
	defer jd.dsCacheLock.Unlock()

	//This means we want to mark/clear all customVals and stateFilters
	//When clearing, we remove the entire dataset entry. Not a big issue
	//We process ALL only during internal migration and caching empty
	//results is not important
	if len(stateFilters) == 0 || len(customValFilters) == 0 || customer == `` {
		if value == hasJobs || value == dropDSFromCache {
			delete(jd.dsEmptyResultCache, ds)
		}
		return
	}

	_, ok := jd.dsEmptyResultCache[ds]
	if !ok {
		jd.dsEmptyResultCache[ds] = map[string]map[string]map[string]map[string]cacheEntry{}
	}

	if _, ok := jd.dsEmptyResultCache[ds][customer]; !ok {
		jd.dsEmptyResultCache[ds][customer] = map[string]map[string]map[string]cacheEntry{}
	}
	for _, cVal := range customValFilters {
		_, ok := jd.dsEmptyResultCache[ds][customer][cVal]
		if !ok {
			jd.dsEmptyResultCache[ds][customer][cVal] = map[string]map[string]cacheEntry{}
		}

		pVals := []string{}
		for _, parameterFilter := range parameterFilters {
			pVals = append(pVals, fmt.Sprintf(`%s_%s`, parameterFilter.Name, parameterFilter.Value))
		}
		sort.Strings(pVals)
		pVal := strings.Join(pVals, "_")

		_, ok = jd.dsEmptyResultCache[ds][customer][cVal][pVal]
		if !ok {
			jd.dsEmptyResultCache[ds][customer][cVal][pVal] = map[string]cacheEntry{}
		}

		for _, st := range stateFilters {
			previous := jd.dsEmptyResultCache[ds][customer][cVal][pVal][st]
			if checkAndSet == nil || *checkAndSet == previous.Value {
				jd.dsEmptyResultCache[ds][customer][cVal][pVal][st] = cacheEntry{
					Value: value,
					T:     time.Now(),
				}
			}
		}
	}
}

func (jd *CacheEmptyDS) InvalidateKey(ds dataSetT, CVPMap map[string]map[string]map[string]struct{}) {
	for customer, customerCVPMap := range CVPMap {
		if jd.queryFilterKeys.CustomVal && len(jd.queryFilterKeys.ParameterFilters) > 0 {
			for cv, cVal := range customerCVPMap {
				for pv := range cVal {
					parameterFilters := []ParameterFilterT{}
					tokens := strings.Split(pv, "::")
					for _, token := range tokens {
						p := strings.Split(token, "##")
						param := ParameterFilterT{
							Name:  p[0],
							Value: p[1],
						}
						parameterFilters = append(parameterFilters, param)
					}
					jd.MarkEmpty(ds, customer, []string{NotProcessed.State}, []string{cv}, parameterFilters, hasJobs, nil)
				}
			}
		} else if jd.queryFilterKeys.CustomVal {
			for cv := range customerCVPMap {
				jd.MarkEmpty(ds, customer, []string{NotProcessed.State}, []string{cv}, nil, hasJobs, nil)
			}
		} else {
			jd.MarkEmpty(ds, customer, []string{}, []string{}, nil, hasJobs, nil)
		}
	}
}

func (jd *CacheEmptyDS) Invalidate(ds dataSetT) {
	//Trimming pre_drop from the table name
	if strings.HasPrefix(ds.JobTable, "pre_drop_") {
		parentDS := dataSetT{
			JobTable:       strings.ReplaceAll(ds.JobTable, "pre_drop_", ""),
			JobStatusTable: strings.ReplaceAll(ds.JobStatusTable, "pre_drop_", ""),
			Index:          ds.Index,
		}
		jd.MarkEmpty(parentDS, ``, []string{}, []string{}, nil, dropDSFromCache, nil)
	} else {
		jd.MarkEmpty(ds, ``, []string{}, []string{}, nil, dropDSFromCache, nil)
	}
}

func (jd *CacheEmptyDS) String() string {
	return fmt.Sprint(jd.dsEmptyResultCache)
}

func (jd *CacheEmptyDS) Status() interface{} {
	emptyResults := make(map[string]interface{})
	for ds, entry := range jd.dsEmptyResultCache {
		emptyResults[ds.JobTable] = entry
	}
	return emptyResults
}

// IsEmpty will return true if:
// 	For all the combinations of stateFilters, customValFilters, parameterFilters.
//  All of the condition above apply:
// 	* There is a cache entry for this dataset, customVal, parameterFilter, stateFilter
//  * The entry is noJobs
//  * The entry is not expired (entry time + cache expiration > now)
func (jd *CacheEmptyDS) IsEmpty(ds dataSetT, customer string, stateFilters []string, customValFilters []string, parameterFilters []ParameterFilterT) bool {
	// queryStat := stats.NewTaggedStat("isEmptyCheck", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	// queryStat.Start()
	// defer queryStat.End()

	jd.dsCacheLock.Lock()
	defer jd.dsCacheLock.Unlock()

	_, ok := jd.dsEmptyResultCache[ds]
	if !ok {
		return false
	}

	_, ok = jd.dsEmptyResultCache[ds][customer]
	if !ok {
		return false
	}
	//We want to check for all states and customFilters. Cannot
	//assert that from cache
	if len(stateFilters) == 0 || len(customValFilters) == 0 {
		return false
	}

	for _, cVal := range customValFilters {
		_, ok := jd.dsEmptyResultCache[ds][customer][cVal]
		if !ok {
			return false
		}

		pVals := []string{}
		for _, parameterFilter := range parameterFilters {
			pVals = append(pVals, fmt.Sprintf(`%s_%s`, parameterFilter.Name, parameterFilter.Value))
		}
		sort.Strings(pVals)
		pVal := strings.Join(pVals, "_")

		_, ok = jd.dsEmptyResultCache[ds][customer][cVal][pVal]
		if !ok {
			return false
		}

		for _, st := range stateFilters {
			mark, ok := jd.dsEmptyResultCache[ds][customer][cVal][pVal][st]
			if !ok || mark.Value != noJobs || time.Now().After(mark.T.Add(cacheExpiration)) {
				return false
			}
		}
	}
	//Every state and every customVal in the DS is empty
	//so can return
	return true
}
