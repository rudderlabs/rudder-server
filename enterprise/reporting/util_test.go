package reporting

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mockEventSampler "github.com/rudderlabs/rudder-server/mocks/enterprise/reporting/event_sampler"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestGetAggregationBucket(t *testing.T) {
	t.Run("should return the correct aggregation bucket with default interval of 1 mintue", func(t *testing.T) {
		cases := []struct {
			reportedAt  int64
			bucketStart int64
			bucketEnd   int64
		}{
			{
				reportedAt:  time.Date(2022, 1, 1, 10, 5, 10, 40, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 1, 1, 10, 5, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 1, 1, 10, 6, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 2, 4, 11, 5, 59, 10, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 2, 4, 11, 5, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 2, 4, 11, 6, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 3, 5, 12, 59, 59, 59, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 3, 5, 12, 59, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 3, 5, 13, 0, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 4, 6, 13, 0, 0, 0, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 4, 6, 13, 0, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 4, 6, 13, 1, 0, 0, time.UTC).Unix() / 60,
			},
		}

		for _, c := range cases {
			bs, be := getAggregationBucketMinute(c.reportedAt, 1)
			require.Equal(t, c.bucketStart, bs)
			require.Equal(t, c.bucketEnd, be)
		}
	})

	t.Run("should return the correct aggregation bucket with aggregation interval of 5 mintue", func(t *testing.T) {
		cases := []struct {
			reportedAt  int64
			bucketStart int64
			bucketEnd   int64
		}{
			{
				reportedAt:  time.Date(2022, 1, 1, 10, 5, 10, 40, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 1, 1, 10, 5, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 1, 1, 10, 10, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 2, 4, 11, 5, 59, 10, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 2, 4, 11, 5, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 2, 4, 11, 10, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 4, 6, 13, 7, 30, 11, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 4, 6, 13, 5, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 4, 6, 13, 10, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 4, 6, 13, 8, 50, 30, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 4, 6, 13, 5, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 4, 6, 13, 10, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 4, 6, 13, 9, 5, 15, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 4, 6, 13, 5, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 4, 6, 13, 10, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 3, 5, 12, 55, 53, 1, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 3, 5, 12, 55, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 3, 5, 13, 0, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 3, 5, 12, 57, 53, 1, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 3, 5, 12, 55, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 3, 5, 13, 0, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 3, 5, 12, 59, 59, 59, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 3, 5, 12, 55, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 3, 5, 13, 0, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 4, 6, 13, 0, 0, 0, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 4, 6, 13, 0, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 4, 6, 13, 5, 0, 0, time.UTC).Unix() / 60,
			},
		}

		for _, c := range cases {
			bs, be := getAggregationBucketMinute(c.reportedAt, 5)
			require.Equal(t, c.bucketStart, bs)
			require.Equal(t, c.bucketEnd, be)
		}
	})

	t.Run("should return the correct aggregation bucket with aggregation interval of 15 mintue", func(t *testing.T) {
		cases := []struct {
			reportedAt  int64
			bucketStart int64
			bucketEnd   int64
		}{
			{
				reportedAt:  time.Date(2022, 1, 1, 10, 5, 10, 40, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 1, 1, 10, 0, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 1, 1, 10, 15, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 2, 4, 11, 17, 59, 10, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 2, 4, 11, 15, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 2, 4, 11, 30, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 4, 6, 13, 39, 10, 59, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 4, 6, 13, 30, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 4, 6, 13, 45, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 4, 6, 13, 59, 50, 30, time.UTC).Unix() / 60,
				bucketStart: time.Date(2022, 4, 6, 13, 45, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 4, 6, 14, 0, 0, 0, time.UTC).Unix() / 60,
			},
		}

		for _, c := range cases {
			bs, be := getAggregationBucketMinute(c.reportedAt, 15)
			require.Equal(t, c.bucketStart, bs)
			require.Equal(t, c.bucketEnd, be)
		}
	})

	t.Run("should choose closest factor of 60 if interval is non positive and return the correct aggregation bucket", func(t *testing.T) {
		cases := []struct {
			reportedAt  int64
			interval    int64
			bucketStart int64
			bucketEnd   int64
		}{
			{
				reportedAt:  time.Date(2022, 1, 1, 12, 5, 10, 40, time.UTC).Unix() / 60,
				interval:    -1, // it should round to 1
				bucketStart: time.Date(2022, 1, 1, 12, 5, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 1, 1, 12, 6, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 2, 29, 10, 0, 2, 59, time.UTC).Unix() / 60,
				interval:    -1, // it should round to 1
				bucketStart: time.Date(2022, 2, 29, 10, 0, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 2, 29, 10, 1, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 2, 10, 0, 0, 0, 40, time.UTC).Unix() / 60,
				interval:    0, // it should round to 1
				bucketStart: time.Date(2022, 2, 10, 0, 0, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 2, 10, 0, 1, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 11, 27, 23, 59, 59, 40, time.UTC).Unix() / 60,
				interval:    0, // it should round to 1
				bucketStart: time.Date(2022, 11, 27, 23, 59, 59, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 11, 28, 0, 0, 0, 0, time.UTC).Unix() / 60,
			},
		}

		for _, c := range cases {
			bs, be := getAggregationBucketMinute(c.reportedAt, c.interval)
			require.Equal(t, c.bucketStart, bs)
			require.Equal(t, c.bucketEnd, be)
		}
	})

	t.Run("should choose closest factor of 60 if interval is not a factor of 60 and return the correct aggregation bucket", func(t *testing.T) {
		cases := []struct {
			reportedAt  int64
			interval    int64
			bucketStart int64
			bucketEnd   int64
		}{
			{
				reportedAt:  time.Date(2022, 1, 1, 10, 23, 10, 40, time.UTC).Unix() / 60,
				interval:    7, // it should round to 6
				bucketStart: time.Date(2022, 1, 1, 10, 18, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 1, 1, 10, 24, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 1, 1, 10, 5, 10, 40, time.UTC).Unix() / 60,
				interval:    14, // it should round to 12
				bucketStart: time.Date(2022, 1, 1, 10, 0, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 1, 1, 10, 12, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 1, 1, 10, 39, 10, 40, time.UTC).Unix() / 60,
				interval:    59, // it should round to 30
				bucketStart: time.Date(2022, 1, 1, 10, 30, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 1, 1, 11, 0, 0, 0, time.UTC).Unix() / 60,
			},
			{
				reportedAt:  time.Date(2022, 1, 1, 10, 5, 10, 40, time.UTC).Unix() / 60,
				interval:    63, // it should round to 60
				bucketStart: time.Date(2022, 1, 1, 10, 0, 0, 0, time.UTC).Unix() / 60,
				bucketEnd:   time.Date(2022, 1, 1, 11, 0, 0, 0, time.UTC).Unix() / 60,
			},
		}

		for _, c := range cases {
			bs, be := getAggregationBucketMinute(c.reportedAt, c.interval)
			require.Equal(t, c.bucketStart, bs)
			require.Equal(t, c.bucketEnd, be)
		}
	})
}

func TestGetSampleWithEventSamplingWithNilEventSampler(t *testing.T) {
	inputSampleEvent := json.RawMessage(`{"event": "1"}`)
	inputSampleResponse := "response"
	metric := types.PUReportedMetric{
		StatusDetail: &types.StatusDetail{
			SampleEvent:    inputSampleEvent,
			SampleResponse: inputSampleResponse,
		},
	}
	outputSampleEvent, outputSampleResponse, err := getSampleWithEventSampling(metric, 1234567890, nil, false, 60)
	require.NoError(t, err)
	require.Equal(t, inputSampleEvent, outputSampleEvent)
	require.Equal(t, inputSampleResponse, outputSampleResponse)
}

func TestFloorFactor(t *testing.T) {
	tests := []struct {
		name       string
		intervalMs int64
		expected   int64
	}{
		// Edge cases
		{name: "Smaller than smallest factor", intervalMs: 0, expected: 1},
		{name: "Exact match for smallest factor", intervalMs: 1, expected: 1},
		{name: "Exact match for largest factor", intervalMs: 60, expected: 60},
		{name: "Larger than largest factor", intervalMs: 100, expected: 60},

		// Typical cases
		{name: "Between 10 and 12", intervalMs: 11, expected: 10},
		{name: "Between 4 and 6", intervalMs: 5, expected: 5},
		{name: "Between 20 and 30", intervalMs: 25, expected: 20},
		{name: "Exact match in the middle", intervalMs: 30, expected: 30},
		{name: "Exact match at a non-boundary point", intervalMs: 12, expected: 12},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := floorFactor(test.intervalMs)
			require.Equal(t, test.expected, result)
		})
	}
}

func TestGetSampleWithEventSampling(t *testing.T) {
	sampleEvent := json.RawMessage(`{"event": "2"}`)
	sampleResponse := "sample response"
	emptySampleEvent := json.RawMessage(`{}`)
	emptySampleResponse := ""

	tests := []struct {
		name       string
		metric     types.PUReportedMetric
		wantMetric types.PUReportedMetric
		shouldGet  bool
		getError   error
		found      bool
		shouldPut  bool
		putError   error
	}{
		{
			name: "Nil sample event",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					SampleEvent: nil,
				},
			},
			wantMetric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					SampleEvent: nil,
				},
			},
		},
		{
			name: "Empty sample event",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					SampleEvent: emptySampleEvent,
				},
			},
			wantMetric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					SampleEvent: emptySampleEvent,
				},
			},
		},
		{
			name: "Event Sampler returns get error",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					SampleEvent:    sampleEvent,
					SampleResponse: sampleResponse,
				},
			},
			wantMetric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					SampleEvent:    sampleEvent,
					SampleResponse: sampleResponse,
				},
			},
			shouldGet: true,
			getError:  errors.New("get error"),
		},
		{
			name: "Event Sampler returns put error",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					SampleEvent:    sampleEvent,
					SampleResponse: sampleResponse,
				},
			},
			wantMetric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					SampleEvent:    sampleEvent,
					SampleResponse: sampleResponse,
				},
			},
			shouldGet: true,
			shouldPut: true,
			putError:  errors.New("put error"),
		},
		{
			name: "Sample is not found",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					SampleEvent:    sampleEvent,
					SampleResponse: sampleResponse,
				},
			},
			wantMetric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					SampleEvent:    sampleEvent,
					SampleResponse: sampleResponse,
				},
			},
			shouldGet: true,
			shouldPut: true,
		},
		{
			name: "Sample is found",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					SampleEvent:    sampleEvent,
					SampleResponse: sampleResponse,
				},
			},
			wantMetric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					SampleEvent:    emptySampleEvent,
					SampleResponse: emptySampleResponse,
				},
			},
			shouldGet: true,
			found:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockEventSampler := mockEventSampler.NewMockEventSampler(ctrl)
			if tt.shouldGet {
				mockEventSampler.EXPECT().Get(gomock.Any()).Return(tt.found, tt.getError)
			}
			if tt.shouldPut {
				mockEventSampler.EXPECT().Put(gomock.Any()).Return(tt.putError)
			}

			sampleEvent, sampleResponse, err := getSampleWithEventSampling(tt.metric, 1234567890, mockEventSampler, true, 60)
			if tt.getError != nil || tt.putError != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantMetric.StatusDetail.SampleEvent, sampleEvent)
			require.Equal(t, tt.wantMetric.StatusDetail.SampleResponse, sampleResponse)
		})
	}
}
