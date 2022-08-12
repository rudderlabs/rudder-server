package event_schema

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPopulateFrequencyCounterBounded(t *testing.T) {
	t.Parallel()

	hash := "my-unique-schema-hash"
	counters := []*FrequencyCounter{
		{Name: "prop1"},
		{Name: "prop2"},
	}
	populateFrequencyCounters(hash, counters, 1)

	// only one frequency counter should be loaded in the cache.
	loadedCounters := countersCache[hash]
	require.Equal(t, 1, len(loadedCounters), "loaded frequency counters into memory should be equal to 1")
}

func TestGetFreqCounterWhenLimitBreached(t *testing.T) {
	t.Parallel()

	hash := uuid.New()
	countersCache[hash.String()] = map[string]*FrequencyCounter{
		"key1": {Name: "key1"},
		"key2": {Name: "key2"},
	}

	// Updated frequencyCounterLimit bound
	frequencyCounterLimit := 2
	// This should ideally result in a new key being added to the countersCache
	// but since the bound is already reached, it simply returns nil for the freq counter
	fq := getFrequencyCounter(hash.String(), "new-key", frequencyCounterLimit)
	require.Nil(t, fq)

	// FrequencyCounter against the original key,
	// should still be returned.
	fq = getFrequencyCounter(hash.String(), "key1", frequencyCounterLimit)
	require.NotNil(t, fq)
	require.Equal(t, "key1", fq.Name)

	// Create room for the new key
	fq = getFrequencyCounter(hash.String(), "new-key-1", 3)
	require.NotNil(t, fq)
	require.Equal(t, "new-key-1", fq.Name)
}
