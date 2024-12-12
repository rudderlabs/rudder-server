package reporting

import (
	"encoding/json"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/event_sampler"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func getAggregationBucketMinute(timeMs, intervalMs int64) (int64, int64) {
	// If interval is not a factor of 60, then the bucket start will not be aligned to hour start
	// For example, if intervalMs is 7, and timeMs is 28891085 (6:05) then the bucket start will be 28891079 (5:59)
	// and current bucket will contain the data of 2 different hourly buckets, which is should not have happened.
	// To avoid this, we round the intervalMs to the nearest factor of 60.
	if intervalMs <= 0 || 60%intervalMs != 0 {
		factors := []int64{1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30, 60}
		closestFactor := factors[0]
		for _, factor := range factors {
			if factor < intervalMs {
				closestFactor = factor
			} else {
				break
			}
		}
		intervalMs = closestFactor
	}

	bucketStart := timeMs - (timeMs % intervalMs)
	bucketEnd := bucketStart + intervalMs

	return bucketStart, bucketEnd
}

func transformMetricWithEventSampling(metric types.PUReportedMetric, reportedAt int64, eventSampler event_sampler.EventSampler, eventSamplingDuration int64) (types.PUReportedMetric, error) {
	if eventSampler == nil {
		return metric, nil
	}

	isValidSampleEvent := metric.StatusDetail.SampleEvent != nil && string(metric.StatusDetail.SampleEvent) != "{}"

	if isValidSampleEvent {
		sampleEventBucket, _ := getAggregationBucketMinute(reportedAt, eventSamplingDuration)
		hash := NewLabelSet(metric, sampleEventBucket).generateHash()
		found, err := eventSampler.Get(hash)
		if err != nil {
			return metric, err
		}

		if found {
			metric.StatusDetail.SampleEvent = json.RawMessage(`{}`)
			metric.StatusDetail.SampleResponse = ""
		} else {
			err := eventSampler.Put(hash)
			if err != nil {
				return metric, err
			}
		}
	}
	return metric, nil
}

func transformMetricForPII(metric types.PUReportedMetric, piiColumns []string) types.PUReportedMetric {
	for _, col := range piiColumns {
		switch col {
		case "sample_event":
			metric.StatusDetail.SampleEvent = []byte(`{}`)
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

func isMetricPosted(status int) bool {
	return status >= 200 && status < 300
}

func getPIIColumnsToExclude() []string {
	piiColumnsToExclude := strings.Split(config.GetString("REPORTING_PII_COLUMNS_TO_EXCLUDE", "sample_event,sample_response"), ",")
	for i := range piiColumnsToExclude {
		piiColumnsToExclude[i] = strings.Trim(piiColumnsToExclude[i], " ")
	}
	return piiColumnsToExclude
}
