package warehouse

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRouter_PrevScheduledTime(t *testing.T) {
	testCases := []struct {
		name                      string
		syncFrequency             string
		syncStartAt               string
		currTime                  time.Time
		expectedPrevScheduledTime time.Time
	}{
		{
			name:                      "should return prev scheduled time",
			syncFrequency:             "30",
			syncStartAt:               "14:00",
			currTime:                  time.Date(2020, 4, 27, 20, 23, 54, 3424534, time.UTC),
			expectedPrevScheduledTime: time.Date(2020, 4, 27, 20, 0, 0, 0, time.UTC),
		},
		{
			name:                      "should return prev scheduled time",
			syncFrequency:             "30",
			syncStartAt:               "14:00",
			currTime:                  time.Date(2020, 4, 27, 20, 30, 0, 0, time.UTC),
			expectedPrevScheduledTime: time.Date(2020, 4, 27, 20, 30, 0, 0, time.UTC),
		},
		{
			name:                      "should return prev day's last scheduled time if less than all of today's scheduled time",
			syncFrequency:             "360",
			syncStartAt:               "05:00",
			currTime:                  time.Date(2020, 4, 27, 4, 23, 54, 3424534, time.UTC),
			expectedPrevScheduledTime: time.Date(2020, 4, 26, 23, 0, 0, 0, time.UTC),
		},
		{
			name:                      "should return today's last scheduled time if current time is greater than all of today's scheduled time",
			syncFrequency:             "180",
			syncStartAt:               "22:00",
			currTime:                  time.Date(2020, 4, 27, 22, 23, 54, 3424534, time.UTC),
			expectedPrevScheduledTime: time.Date(2020, 4, 27, 22, 0, 0, 0, time.UTC),
		},
		{
			name:                      "should return appropriate scheduled time when current time is start of day",
			syncFrequency:             "180",
			syncStartAt:               "22:00",
			currTime:                  time.Date(2020, 4, 27, 0, 0, 0, 0, time.UTC),
			expectedPrevScheduledTime: time.Date(2020, 4, 26, 22, 0, 0, 0, time.UTC),
		},
		{
			name:                      "should return appropriate scheduled time when current time is start of day",
			syncFrequency:             "180",
			syncStartAt:               "00:00",
			currTime:                  time.Date(2020, 4, 27, 0, 0, 0, 0, time.UTC),
			expectedPrevScheduledTime: time.Date(2020, 4, 27, 0, 0, 0, 0, time.UTC),
		},
		{
			name:                      "should return appropriate scheduled time when current time is end of day",
			syncFrequency:             "180",
			syncStartAt:               "00:00",
			currTime:                  time.Date(2020, 4, 27, 23, 59, 59, 999999, time.UTC),
			expectedPrevScheduledTime: time.Date(2020, 4, 27, 21, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expectedPrevScheduledTime, prevScheduledTime(tc.syncFrequency, tc.syncStartAt, tc.currTime))
		})
	}
}

func TestRouter_CheckCurrentTimeExistsInExcludeWindow(t *testing.T) {
	testCases := []struct {
		currentTime   time.Time
		windowStart   string
		windowEnd     string
		expectedValue bool
	}{
		{
			currentTime:   time.Date(2009, time.November, 10, 5, 30, 0, 0, time.UTC),
			windowStart:   "05:00",
			windowEnd:     "06:00",
			expectedValue: true,
		},
		{
			currentTime:   time.Date(2009, time.November, 10, 5, 30, 0, 0, time.UTC),
			windowStart:   "22:00",
			windowEnd:     "06:00",
			expectedValue: true,
		},
		{
			currentTime:   time.Date(2009, time.November, 10, 23, 30, 0, 0, time.UTC),
			windowStart:   "22:00",
			windowEnd:     "06:00",
			expectedValue: true,
		},
		{
			currentTime: time.Date(2009, time.November, 10, 7, 30, 0, 0, time.UTC),
			windowStart: "05:00",
			windowEnd:   "06:00",
		},
		{
			currentTime: time.Date(2009, time.November, 10, 7, 30, 0, 0, time.UTC),
			windowStart: "22:00",
			windowEnd:   "06:00",
		},
		{
			currentTime: time.Date(2009, time.November, 10, 21, 30, 0, 0, time.UTC),
			windowStart: "22:00",
			windowEnd:   "06:00",
		},
		{
			currentTime: time.Date(2009, time.November, 10, 21, 30, 0, 0, time.UTC),
			windowStart: "",
			windowEnd:   "",
		},
		{
			currentTime: time.Date(2009, time.November, 10, 21, 30, 0, 0, time.UTC),
			windowStart: "22:00",
			windowEnd:   "",
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("checkCurrentTimeExistsInExcludeWindow %d", i), func(t *testing.T) {
			require.Equal(t, checkCurrentTimeExistsInExcludeWindow(tc.currentTime, tc.windowStart, tc.windowEnd), tc.expectedValue)
		})
	}
}

func TestRouter_ExcludeWindowStartEndTimes(t *testing.T) {
	testCases := []struct {
		name          string
		excludeWindow map[string]interface{}
		expectedStart string
		expectedEnd   string
	}{
		{
			name: "nil",
		},
		{
			name: "excludeWindowStartTime and excludeWindowEndTime",
			excludeWindow: map[string]interface{}{
				"excludeWindowStartTime": "2006-01-02 15:04:05.999999 Z",
				"excludeWindowEndTime":   "2006-01-02 15:05:05.999999 Z",
			},
			expectedStart: "2006-01-02 15:04:05.999999 Z",
			expectedEnd:   "2006-01-02 15:05:05.999999 Z",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			start, end := excludeWindowStartEndTimes(tc.excludeWindow)
			require.Equal(t, tc.expectedStart, start)
			require.Equal(t, tc.expectedEnd, end)
		})
	}
}
