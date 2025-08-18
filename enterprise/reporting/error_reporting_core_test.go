package reporting

import (
	"net/http"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestShouldReport(t *testing.T) {
	RegisterTestingT(t)

	testCases := []struct {
		name     string
		metric   types.PUReportedMetric
		expected bool
	}{
		{
			name: "event failure case",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					StatusCode: http.StatusBadRequest,
				},
			},
			expected: true,
		},
		{
			name: "filter event case",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					StatusCode: types.FilterEventCode,
				},
			},
			expected: true,
		},
		{
			name: "suppress event case",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					StatusCode: types.SuppressEventCode,
				},
			},
			expected: true,
		},
		{
			name: "success case",
			metric: types.PUReportedMetric{
				StatusDetail: &types.StatusDetail{
					StatusCode: http.StatusOK,
				},
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := shouldReport(tc.metric)
			Expect(result).To(Equal(tc.expected))
		})
	}
}

func TestCleanUpErrorMessage(t *testing.T) {
	ext := NewErrorDetailExtractor(logger.NOP, config.New())

	testCases := []struct {
		name     string
		inputStr string
		expected string
	}{
		{
			name:     "object ID cleanup",
			inputStr: "Object with ID '123983489734' is not a valid object",
			expected: "Object with ID is not a valid object",
		},
		{
			name:     "URL cleanup with context deadline",
			inputStr: "http://xyz-rudder.com/v1/endpoint not reachable: context deadline exceeded",
			expected: "not reachable context deadline exceeded",
		},
		{
			name:     "URL cleanup with EOF",
			inputStr: "http://xyz-rudder.com/v1/endpoint not reachable 172.22.22.10: EOF",
			expected: "not reachable EOF",
		},
		{
			name:     "timestamp cleanup",
			inputStr: "Request failed to process from 16-12-2022:19:30:23T+05:30 due to internal server error",
			expected: "Request failed to process from due to internal server error",
		},
		{
			name:     "email cleanup",
			inputStr: "User with email 'vagor12@bing.com' is not valid",
			expected: "User with email is not valid",
		},
		{
			name:     "time duration cleanup",
			inputStr: "Allowed timestamp is [15 minutes] into the future",
			expected: "Allowed timestamp is minutes into the future",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			actual := ext.CleanUpErrorMessage(tc.inputStr)
			require.Equal(t, tc.expected, actual)
		})
	}
}
