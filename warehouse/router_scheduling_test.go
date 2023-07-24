package warehouse_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/admin"
	. "github.com/rudderlabs/rudder-server/warehouse"
)

func initWarehouse() {
	config.Reset()
	admin.Init()
	logger.Reset()
	Init()
	Init2()
	Init3()
	Init4()
	Init5()
}

var _ = Describe("Scheduling", func() {
	initWarehouse()

	Describe("GetPrevScheduledTime", func() {
		It("should return prev scheduled time", func() {
			now := time.Date(2020, 0o4, 27, 20, 23, 54, 3424534, time.UTC)
			sTime := GetPrevScheduledTime("30", "14:00", now)
			Expect(sTime).To(Equal(time.Date(2020, 0o4, 27, 20, 0, 0, 0, time.UTC)))

			// current time exactly equal to scheduled time
			now = time.Date(2020, 0o4, 27, 20, 30, 0, 0, time.UTC)
			sTime = GetPrevScheduledTime("30", "14:00", now)
			Expect(sTime).To(Equal(time.Date(2020, 0o4, 27, 20, 30, 0, 0, time.UTC)))
		})

		// current time less than all scheduled times in a day
		It("should return prev day's last scheduled time if less than all of today's scheduled time", func() {
			now := time.Date(2020, 0o4, 27, 0o4, 23, 54, 3424534, time.UTC)
			sTime := GetPrevScheduledTime("360", "05:00", now)
			Expect(sTime).To(Equal(time.Date(2020, 0o4, 26, 23, 0, 0, 0, time.UTC)))
		})

		// current time greater than all scheduled times in a day
		It("should return today's last scheduled time if current time is greater than all of today's scheduled time", func() {
			now := time.Date(2020, 0o4, 27, 22, 23, 54, 3424534, time.UTC)
			sTime := GetPrevScheduledTime("180", "22:00", now)
			Expect(sTime).To(Equal(time.Date(2020, 0o4, 27, 22, 0, 0, 0, time.UTC)))
		})

		// current time is start of day
		It("should return appropriate scheduled time when current time is start of day", func() {
			now := time.Date(2020, 0o4, 27, 0o0, 0o0, 0o0, 0, time.UTC)
			sTime := GetPrevScheduledTime("180", "22:00", now)
			Expect(sTime).To(Equal(time.Date(2020, 0o4, 26, 22, 0, 0, 0, time.UTC)))

			now = time.Date(2020, 0o4, 27, 0o0, 0o0, 0o0, 0, time.UTC)
			sTime = GetPrevScheduledTime("180", "00:00", now)
			Expect(sTime).To(Equal(time.Date(2020, 0o4, 27, 0o0, 0o0, 0o0, 0, time.UTC)))
		})

		// current time is end of day
		It("should return appropriate scheduled time when current time is end of day", func() {
			now := time.Date(2020, 0o4, 27, 23, 59, 59, 999999, time.UTC)
			sTime := GetPrevScheduledTime("180", "00:00", now)
			Expect(sTime).To(Equal(time.Date(2020, 0o4, 27, 21, 0, 0, 0, time.UTC)))
		})
	})

	Describe("CheckCurrentTimeExistsInExcludeWindow", func() {
		It("should return true if current time falls in excludeWindow", func() {
			startTime := "05:00"
			endTime := "06:00"
			currentTime := time.Date(2009, time.November, 10, 5, 30, 0, 0, time.UTC)
			Expect(CheckCurrentTimeExistsInExcludeWindow(currentTime, startTime, endTime)).To(Equal(true))
			startTime = "22:00"
			endTime = "06:00"
			currentTime = time.Date(2009, time.November, 10, 5, 30, 0, 0, time.UTC)
			Expect(CheckCurrentTimeExistsInExcludeWindow(currentTime, startTime, endTime)).To(Equal(true))
			startTime = "22:00"
			endTime = "06:00"
			currentTime = time.Date(2009, time.November, 10, 23, 30, 0, 0, time.UTC)
			Expect(CheckCurrentTimeExistsInExcludeWindow(currentTime, startTime, endTime)).To(Equal(true))
		})
		It("should return false if current time falls in excludeWindow", func() {
			startTime := "05:00"
			endTime := "06:00"
			currentTime := time.Date(2009, time.November, 10, 7, 30, 0, 0, time.UTC)
			Expect(CheckCurrentTimeExistsInExcludeWindow(currentTime, startTime, endTime)).To(Equal(false))
			startTime = "22:00"
			endTime = "06:00"
			currentTime = time.Date(2009, time.November, 10, 7, 30, 0, 0, time.UTC)
			Expect(CheckCurrentTimeExistsInExcludeWindow(currentTime, startTime, endTime)).To(Equal(false))
			startTime = "22:00"
			endTime = "06:00"
			currentTime = time.Date(2009, time.November, 10, 21, 30, 0, 0, time.UTC)
			Expect(CheckCurrentTimeExistsInExcludeWindow(currentTime, startTime, endTime)).To(Equal(false))
			startTime = ""
			endTime = ""
			currentTime = time.Date(2009, time.November, 10, 21, 30, 0, 0, time.UTC)
			Expect(CheckCurrentTimeExistsInExcludeWindow(currentTime, startTime, endTime)).To(Equal(false))
			startTime = "22:00"
			endTime = ""
			currentTime = time.Date(2009, time.November, 10, 21, 30, 0, 0, time.UTC)
			Expect(CheckCurrentTimeExistsInExcludeWindow(currentTime, startTime, endTime)).To(Equal(false))
		})
	})

	DescribeTable("GetExcludeWindowStartEndTimes", func(excludeWindow map[string]interface{}, expectedStart, expectedEnd string) {
		start, end := GetExcludeWindowStartEndTimes(excludeWindow)
		Expect(start).To(Equal(expectedStart))
		Expect(end).To(Equal(expectedEnd))
	},
		Entry(nil, map[string]interface{}{}, "", ""),
		Entry(
			nil,
			map[string]interface{}{
				"excludeWindowStartTime": "2006-01-02 15:04:05.999999 Z",
				"excludeWindowEndTime":   "2006-01-02 15:05:05.999999 Z",
			},
			"2006-01-02 15:04:05.999999 Z",
			"2006-01-02 15:05:05.999999 Z",
		),
	)

	DescribeTable("DurationBeforeNextAttempt", func(attempt int, expected time.Duration) {
		Expect(DurationBeforeNextAttempt(int64(attempt))).To(Equal(expected))
	},
		Entry(nil, 0, time.Duration(0)),
		Entry(nil, 1, time.Second*60),
		Entry(nil, 2, time.Second*120),
	)
})
