package warehouse

import (
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	scheduledTimesCache     map[string][]int
	minUploadBackoff        time.Duration
	maxUploadBackoff        time.Duration
	startUploadAlways       bool
	scheduledTimesCacheLock sync.RWMutex
)

func Init3() {
	scheduledTimesCache = map[string][]int{}
	loadConfigScheduling()
}

func loadConfigScheduling() {
	config.RegisterDurationConfigVariable(60, &minUploadBackoff, true, time.Second, []string{"Warehouse.minUploadBackoff", "Warehouse.minUploadBackoffInS"}...)
	config.RegisterDurationConfigVariable(1800, &maxUploadBackoff, true, time.Second, []string{"Warehouse.maxUploadBackoff", "Warehouse.maxUploadBackoffInS"}...)
}

// ScheduledTimes returns all possible start times (minutes from start of day) as per schedule
// e.g. Syncing every 3hrs starting at 13:00 (scheduled times: 13:00, 16:00, 19:00, 22:00, 01:00, 04:00, 07:00, 10:00)
func ScheduledTimes(syncFrequency, syncStartAt string) []int {
	scheduledTimesCacheLock.RLock()
	cachedTimes, ok := scheduledTimesCache[fmt.Sprintf(`%s-%s`, syncFrequency, syncStartAt)]
	scheduledTimesCacheLock.RUnlock()
	if ok {
		return cachedTimes
	}
	syncStartAtInMin := timeutil.MinsOfDay(syncStartAt)
	syncFrequencyInMin, _ := strconv.Atoi(syncFrequency)
	times := []int{syncStartAtInMin}
	counter := 1
	for {
		mins := syncStartAtInMin + counter*syncFrequencyInMin
		if mins >= 1440 {
			break
		}
		times = append(times, mins)
		counter++
	}

	var prependTimes []int
	counter = 1
	for {
		mins := syncStartAtInMin - counter*syncFrequencyInMin
		if mins < 0 {
			break
		}
		prependTimes = append(prependTimes, mins)
		counter++
	}

	times = append(lo.Reverse(prependTimes), times...)
	scheduledTimesCacheLock.Lock()
	scheduledTimesCache[fmt.Sprintf(`%s-%s`, syncFrequency, syncStartAt)] = times
	scheduledTimesCacheLock.Unlock()
	return times
}

// GetPrevScheduledTime returns the closest previous scheduled time
// e.g. Syncing every 3hrs starting at 13:00 (scheduled times: 13:00, 16:00, 19:00, 22:00, 01:00, 04:00, 07:00, 10:00)
// prev scheduled time for current time (e.g. 18:00 -> 16:00 same day, 00:30 -> 22:00 prev day)
func GetPrevScheduledTime(syncFrequency, syncStartAt string, currTime time.Time) time.Time {
	allStartTimes := ScheduledTimes(syncFrequency, syncStartAt)

	loc, _ := time.LoadLocation("UTC")
	now := currTime.In(loc)
	// current time in minutes since start of day
	currMins := now.Hour()*60 + now.Minute()

	// get position where current time can fit in the sorted list of allStartTimes
	pos := 0
	for idx, t := range allStartTimes {
		if currMins >= t {
			// case when currTime is greater than all the day's start time
			if idx == len(allStartTimes)-1 {
				pos = idx
			}
			continue
		}
		// case when currTime is less than all the day's start time
		pos = idx - 1
		break
	}

	// if current time is less than first start time in a day, take last start time in prev day
	if pos < 0 {
		return timeutil.StartOfDay(now).Add(time.Hour * time.Duration(-24)).Add(time.Minute * time.Duration(allStartTimes[len(allStartTimes)-1]))
	}
	return timeutil.StartOfDay(now).Add(time.Minute * time.Duration(allStartTimes[pos]))
}

// getLastUploadCreatedAt returns the start time of the last upload
func (wh *HandleT) getLastUploadCreatedAt(warehouse model.Warehouse) time.Time {
	var t sql.NullTime
	sqlStatement := fmt.Sprintf(`
		SELECT
		  created_at
		FROM
		  %s
		WHERE
		  source_id = '%s'
		  AND destination_id = '%s'
		ORDER BY
		  id DESC
		LIMIT
		  1;
`,
		warehouseutils.WarehouseUploadsTable,
		warehouse.Source.ID,
		warehouse.Destination.ID,
	)
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&t)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	if err == sql.ErrNoRows || !t.Valid {
		return time.Time{} // zero value
	}
	return t.Time
}

func GetExcludeWindowStartEndTimes(excludeWindow map[string]interface{}) (string, string) {
	var startTime, endTime string
	if st, ok := excludeWindow[warehouseutils.ExcludeWindowStartTime].(string); ok {
		startTime = st
	}
	if et, ok := excludeWindow[warehouseutils.ExcludeWindowEndTime].(string); ok {
		endTime = et
	}
	return startTime, endTime
}

func CheckCurrentTimeExistsInExcludeWindow(currentTime time.Time, windowStartTime, windowEndTime string) bool {
	if windowStartTime == "" || windowEndTime == "" {
		return false
	}
	startTimeMins := timeutil.MinsOfDay(windowStartTime)
	endTimeMins := timeutil.MinsOfDay(windowEndTime)
	currentTimeMins := timeutil.GetElapsedMinsInThisDay(currentTime)
	// startTime, currentTime, endTime: 05:09, 06:19, 09:07 - > window between this day 05:09 and 09:07
	if startTimeMins < currentTimeMins && currentTimeMins < endTimeMins {
		return true
	}
	// startTime, currentTime, endTime: 22:09, 06:19, 09:07 -> window between this day 22:09 and tomorrow 09:07
	if startTimeMins > currentTimeMins && currentTimeMins < endTimeMins && startTimeMins > endTimeMins {
		return true
	}
	// startTime, currentTime, endTime: 22:09, 23:19, 09:07 -> window between this day 22:09 and tomorrow 09:07
	if startTimeMins < currentTimeMins && currentTimeMins > endTimeMins && startTimeMins > endTimeMins {
		return true
	}
	return false
}

// canCreateUpload indicates if an upload can be started now for the warehouse based on its configured schedule
func (wh *HandleT) canCreateUpload(warehouse model.Warehouse) (bool, error) {
	// can be set from rudder-cli to force uploads always
	if startUploadAlways {
		return true, nil
	}
	// return true if the upload was triggered
	if isUploadTriggered(warehouse) {
		return true, nil
	}
	if warehouseSyncFreqIgnore {
		if uploadFrequencyExceeded(warehouse, "") {
			return false, fmt.Errorf("ignore sync freq: upload frequency exceeded")
		} else {
			return true, nil
		}
	}
	// gets exclude window start time and end time
	excludeWindow := warehouseutils.GetConfigValueAsMap(warehouseutils.ExcludeWindow, warehouse.Destination.Config)
	excludeWindowStartTime, excludeWindowEndTime := GetExcludeWindowStartEndTimes(excludeWindow)
	if CheckCurrentTimeExistsInExcludeWindow(timeutil.Now(), excludeWindowStartTime, excludeWindowEndTime) {
		return false, fmt.Errorf("exclude window: current time exists in exclude window")
	}
	syncFrequency := warehouseutils.GetConfigValue(warehouseutils.SyncFrequency, warehouse)
	syncStartAt := warehouseutils.GetConfigValue(warehouseutils.SyncStartAt, warehouse)
	if syncFrequency == "" || syncStartAt == "" {
		if uploadFrequencyExceeded(warehouse, syncFrequency) {
			return false, fmt.Errorf("upload frequency exceeded")
		} else {
			return true, nil
		}
	}
	prevScheduledTime := GetPrevScheduledTime(syncFrequency, syncStartAt, time.Now())
	lastUploadCreatedAt := wh.getLastUploadCreatedAt(warehouse)
	// start upload only if no upload has started in current window
	// e.g. with prev scheduled time 14:00 and current time 15:00, start only if prev upload hasn't started after 14:00
	if lastUploadCreatedAt.Before(prevScheduledTime) {
		return true, nil
	} else {
		return false, fmt.Errorf("before scheduled time")
	}
}

func DurationBeforeNextAttempt(attempt int64) time.Duration { // Add state(retryable/non-retryable) as an argument to decide backoff etc.)
	var d time.Duration
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = minUploadBackoff
	b.MaxInterval = maxUploadBackoff
	b.MaxElapsedTime = 0
	b.Multiplier = 2
	b.RandomizationFactor = 0
	b.Reset()
	for index := int64(0); index < attempt; index++ {
		d = b.NextBackOff()
	}
	return d
}
