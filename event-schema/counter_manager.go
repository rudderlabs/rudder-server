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

func populateFrequencyCounters(schemaHash string, frequencyCounters []*FrequencyCounter) {
	frequencyCountersMap := make(map[string]*FrequencyCounter)
	for _, fc := range frequencyCounters {
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

func getFrequencyCounter(schemaHash string, key string) *FrequencyCounter {
	schemaVersionCounters, ok := countersCache[schemaHash]
	if !ok {
		schemaVersionCounters = make(map[string]*FrequencyCounter)
		countersCache[schemaHash] = schemaVersionCounters
	}

	frequencyCounter, ok := schemaVersionCounters[key]
	if !ok {
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
			//Capping the freq to 1
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
