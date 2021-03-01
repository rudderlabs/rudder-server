package warehouse

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/thoas/go-funk"
)

var (
	scheduledTimesCache map[string][]int
	minUploadBackoff    time.Duration
	maxUploadBackoff    time.Duration
	startUploadAlways   bool
)

func init() {
	scheduledTimesCache = map[string][]int{}
	minUploadBackoff = config.GetDuration("Warehouse.minUploadBackoffInS", time.Duration(60)) * time.Second
	maxUploadBackoff = config.GetDuration("Warehouse.maxUploadBackoffInS", time.Duration(1800)) * time.Second
}

// ScheduledTimes returns all possible start times (minutes from start of day) as per schedule
// eg. Syncing every 3hrs starting at 13:00 (scheduled times: 13:00, 16:00, 19:00, 22:00, 01:00, 04:00, 07:00, 10:00)
func ScheduledTimes(syncFrequency, syncStartAt string) []int {
	if cachedTimes, ok := scheduledTimesCache[fmt.Sprintf(`%s-%s`, syncFrequency, syncStartAt)]; ok {
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

	prependTimes := []int{}
	counter = 1
	for {
		mins := syncStartAtInMin - counter*syncFrequencyInMin
		if mins < 0 {
			break
		}
		prependTimes = append(prependTimes, mins)
		counter++
	}
	times = append(funk.ReverseInt(prependTimes), times...)
	scheduledTimesCache[fmt.Sprintf(`%s-%s`, syncFrequency, syncStartAt)] = times
	return times
}

// GetPrevScheduledTime returns closest previous scheduled time
// eg. Syncing every 3hrs starting at 13:00 (scheduled times: 13:00, 16:00, 19:00, 22:00, 01:00, 04:00, 07:00, 10:00)
// prev scheduled time for current time (eg. 18:00 -> 16:00 same day, 00:30 -> 22:00 prev day)
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
			// case when currTime is greater than all of the day's start time
			if idx == len(allStartTimes)-1 {
				pos = idx
			}
			continue
		}
		// case when currTime is less than all of the day's start time
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
func (wh *HandleT) getLastUploadCreatedAt(warehouse warehouseutils.WarehouseT) time.Time {
	var t sql.NullTime
	sqlStatement := fmt.Sprintf(`select created_at from %s where source_id='%s' and destination_id='%s' order by id desc limit 1`, warehouseutils.WarehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID)
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&t)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	if err == sql.ErrNoRows || !t.Valid {
		return time.Time{} // zero value
	}
	return t.Time
}

func GetExludeWindowStartEndTimes(excludeWindow map[string]interface{}) (string, string) {
	var startTime, endTime string
	if time, ok := excludeWindow[warehouseutils.ExcludeWindowStartTime].(string); ok {
		startTime = time
	}
	if time, ok := excludeWindow[warehouseutils.ExcludeWindowEndTime].(string); ok {
		endTime = time
	}
	return startTime, endTime
}

func CheckCurrentTimeExistsInExcludeWindow(currentTime time.Time, windowStartTime string, windowEndTime string) bool {
	if len(windowStartTime) == 0 || len(windowEndTime) == 0 {
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

// canCreateUpload indicates if a upload can be started now for the warehouse based on its configured schedule
func (wh *HandleT) canCreateUpload(warehouse warehouseutils.WarehouseT) bool {
	// can be set from rudder-cli to force uploads always
	if startUploadAlways {
		return true
	}
	if warehouseSyncFreqIgnore {
		return !uploadFrequencyExceeded(warehouse, "")
	}
	// gets exclude window start time and end time
	excludeWindow := warehouseutils.GetConfigValueAsMap(warehouseutils.ExcludeWindow, warehouse.Destination.Config)
	excludeWindowStartTime, excludeWindowEndTime := GetExludeWindowStartEndTimes(excludeWindow)
	if CheckCurrentTimeExistsInExcludeWindow(timeutil.Now(), excludeWindowStartTime, excludeWindowEndTime) {
		return false
	}
	syncFrequency := warehouseutils.GetConfigValue(warehouseutils.SyncFrequency, warehouse)
	syncStartAt := warehouseutils.GetConfigValue(warehouseutils.SyncStartAt, warehouse)
	if syncFrequency == "" || syncStartAt == "" {
		return !uploadFrequencyExceeded(warehouse, syncFrequency)
	}
	prevScheduledTime := GetPrevScheduledTime(syncFrequency, syncStartAt, time.Now())
	lastUploadCreatedAt := wh.getLastUploadCreatedAt(warehouse)
	// start upload only if no upload has started in current window
	// eg. with prev scheduled time 14:00 and current time 15:00, start only if prev upload hasn't started after 14:00
	return lastUploadCreatedAt.Before(prevScheduledTime)
}

func durationBeforeNextAttempt(attempt int64) time.Duration { //Add state(retryable/non-retryable) as an argument to decide backoff etc)
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
