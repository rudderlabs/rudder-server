package reporting

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/event_sampler"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func floorFactor(intervalMs int64) int64 {
	factors := []int64{1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60}

	// Find the smallest index where factors[i] >= intervalMs
	index := sort.Search(len(factors), func(i int) bool {
		return factors[i] >= intervalMs
	})

	// If index is 0, intervalMs is smaller than the smallest factor
	if index == 0 {
		return factors[0]
	}

	// If factors[index] == intervalMs, return it directly
	if index < len(factors) && factors[index] == intervalMs {
		return factors[index]
	}

	// Otherwise, return the previous factor
	return factors[index-1]
}

func GetAggregationBucketMinute(timeMs, intervalMs int64) (int64, int64) {
	// If interval is not a factor of 60, then the bucket start will not be aligned to hour start
	// For example, if intervalMs is 7, and timeMs is 28891085 (6:05) then the bucket start will be 28891079 (5:59)
	// and current bucket will contain the data of 2 different hourly buckets, which is should not have happened.
	// To avoid this, we round the intervalMs to the nearest factor of 60.
	if intervalMs <= 0 || 60%intervalMs != 0 {
		intervalMs = floorFactor(intervalMs)
	}
	bucketStart := timeMs - (timeMs % intervalMs)
	bucketEnd := bucketStart + intervalMs
	return bucketStart, bucketEnd
}

func getStringifiedSampleEvent(rawSampleEvent json.RawMessage) string {
	if rawSampleEvent == nil {
		return `{}`
	}
	return string(rawSampleEvent)
}

// getSampleWithEventSamplingCore contains the common event sampling logic
func getSampleWithEventSamplingCore(sampleEvent json.RawMessage, sampleResponse string, eventSampler event_sampler.EventSampler, eventSamplingEnabled bool, hashGenerator func() string) (json.RawMessage, string, error) {
	if !eventSamplingEnabled || eventSampler == nil {
		return sampleEvent, sampleResponse, nil
	}

	isValidSample := (sampleEvent != nil || sampleResponse != "")

	if isValidSample {
		hash := hashGenerator()

		var found bool
		var err error
		found, err = eventSampler.Get(hash)
		if err != nil {
			return sampleEvent, sampleResponse, err
		}

		if found {
			sampleEvent = nil
			sampleResponse = ""
		} else {
			err = eventSampler.Put(hash)
			if err != nil {
				return sampleEvent, sampleResponse, err
			}
		}
	}

	return sampleEvent, sampleResponse, nil
}

func getSampleWithEventSampling(metric types.PUReportedMetric, reportedAt int64, eventSampler event_sampler.EventSampler, eventSamplingEnabled bool, eventSamplingDuration int64) (sampleEvent json.RawMessage, sampleResponse string, err error) {
	sampleEvent = metric.StatusDetail.SampleEvent
	sampleResponse = metric.StatusDetail.SampleResponse

	hashGenerator := func() string {
		sampleEventBucket, _ := GetAggregationBucketMinute(reportedAt, eventSamplingDuration)
		return NewLabelSet(metric, sampleEventBucket).generateHash()
	}

	return getSampleWithEventSamplingCore(sampleEvent, sampleResponse, eventSampler, eventSamplingEnabled, hashGenerator)
}

// getSampleWithEventSamplingForEDReportsDB applies event sampling to EDReportsDB metrics
// It reuses the common logic from getSampleWithEventSampling but works with EDReportsDB
func getSampleWithEventSamplingForEDReportsDB(metric types.EDReportsDB, reportedAt int64, eventSampler event_sampler.EventSampler, eventSamplingEnabled bool, eventSamplingDuration int64) (sampleEvent json.RawMessage, sampleResponse string, err error) {
	sampleEvent = metric.SampleEvent
	sampleResponse = metric.SampleResponse

	hashGenerator := func() string {
		sampleEventBucket, _ := GetAggregationBucketMinute(reportedAt, eventSamplingDuration)
		return NewLabelSetFromEDReportsDB(metric, sampleEventBucket).generateHash()
	}

	return getSampleWithEventSamplingCore(sampleEvent, sampleResponse, eventSampler, eventSamplingEnabled, hashGenerator)
}

func transformMetricForPII(metric types.PUReportedMetric, piiColumns []string) types.PUReportedMetric {
	for _, col := range piiColumns {
		switch col {
		case "sample_event":
			metric.StatusDetail.SampleEvent = nil
		case "sample_response":
			metric.StatusDetail.SampleResponse = ""
		case "event_name":
			metric.StatusDetail.EventName = ""
		case "event_type":
			metric.StatusDetail.EventType = ""
		}
	}

	return metric
}

func getPIIColumnsToExclude() []string {
	piiColumnsToExclude := strings.Split(config.GetString("REPORTING_PII_COLUMNS_TO_EXCLUDE", "sample_event,sample_response"), ",")
	for i := range piiColumnsToExclude {
		piiColumnsToExclude[i] = strings.Trim(piiColumnsToExclude[i], " ")
	}
	return piiColumnsToExclude
}
