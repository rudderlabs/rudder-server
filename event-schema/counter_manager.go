package event_schema

// schemaHash -> Key -> FrequencyCounter
var countersCache map[string]map[string]*FrequencyCounter

type CounterItem struct {
	Value     string
	Frequency float64
}

func init() {
	if countersCache == nil {
		countersCache = make(map[string]map[string]*FrequencyCounter)
	}
}

// populateFrequencyCountersBounded is responsible for capturing the frequency counters which
// are available in the db and store them in memory but in a bounded manner.
func populateFrequencyCounters(schemaHash string, frequencyCounters []*FrequencyCounter, bound int) {
	frequencyCountersMap := make(map[string]*FrequencyCounter)
	for idx, fc := range frequencyCounters {
		// If count exceeds for a particular schema hash, break
		// the loop
		if idx >= bound {
			break
		}

		frequencyCountersMap[fc.Name] = NewPeristedFrequencyCounter(fc)
	}
	countersCache[schemaHash] = frequencyCountersMap
}

func getAllFrequencyCounters(schemaHash string) []*FrequencyCounter {
	schemaVersionCounters, ok := countersCache[schemaHash]
	if !ok {
		return []*FrequencyCounter{}
	}

	frequencyCounters := make([]*FrequencyCounter, 0, len(schemaVersionCounters))

	for _, v := range schemaVersionCounters {
		frequencyCounters = append(frequencyCounters, v)
	}
	return frequencyCounters
}

// pruneFrequencyCounters brings the frequency counters back to desired bound.
func pruneFrequencyCounters(schemaHash string, bound int) {
	countersMap := countersCache[schemaHash]
	diff := bound - len(countersMap)

	if diff >= 0 {
		return
	}

	toDelete := -1 * diff
	for k := range countersMap {
		if toDelete > 0 {
			delete(countersMap, k)
			toDelete--

			continue
		}

		break
	}
}

// getFrequencyCounter simply returns frequency counter for flattened
// event key. It creates a new fc in case the key doesn't exist in map.
func getFrequencyCounter(schemaHash, key string, bound int) *FrequencyCounter {
	schemaVersionCounters, ok := countersCache[schemaHash]
	if !ok {
		schemaVersionCounters = make(map[string]*FrequencyCounter)
		countersCache[schemaHash] = schemaVersionCounters
	}

	// Here we add a new frequency counter for schemaVersionCounter
	frequencyCounter, ok := schemaVersionCounters[key]
	if !ok {
		if len(schemaVersionCounters) >= bound {
			return nil
		}

		frequencyCounter = NewFrequencyCounter(key)
		schemaVersionCounters[key] = frequencyCounter
	}

	return frequencyCounter
}

func getSchemaVersionCounters(schemaHash string) map[string][]*CounterItem {
	schemaVersionCounters := countersCache[schemaHash]
	counters := make(map[string][]*CounterItem)

	for key, fc := range schemaVersionCounters {

		entries := fc.ItemsAboveThreshold()
		counterItems := make([]*CounterItem, 0, len(entries))
		for _, entry := range entries {

			freq := entry.Frequency
			// Capping the freq to 1
			if freq > 1 {
				freq = 1.0
			}
			counterItems = append(counterItems, &CounterItem{Value: entry.Key, Frequency: freq})
		}

		if len(counterItems) > 0 {
			counters[key] = counterItems
		}
	}
	return counters
}
