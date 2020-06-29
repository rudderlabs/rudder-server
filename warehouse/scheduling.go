package warehouse

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/thoas/go-funk"
)

var (
	scheduledTimesCache map[string][]int
)

func init() {
	scheduledTimesCache = map[string][]int{}
}

// ScheduledTimes returns all possible start times as per schedule
// eg. Syncing every 3hrs starting at 13:00 (scheduled times: 13:00, 16:00, 19:00, 22:00, 01:00, 04:00, 07:00, 10:00)
func ScheduledTimes(syncFrequency, syncStartAt string) (times []int) {
	if cachedTimes, ok := scheduledTimesCache[fmt.Sprintf(`%s-%s`, syncFrequency, syncStartAt)]; ok {
		return cachedTimes
	}
	syncStartAtInMin := timeutil.MinsOfDay(syncStartAt)
	syncFrequencyInMin, _ := strconv.Atoi(syncFrequency)
	times = append(times, syncStartAtInMin)
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
	return
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

// getLastUploadStartTime returns the start time of the last upload
func (wh *HandleT) getLastUploadStartTime(warehouse warehouseutils.WarehouseT) (lastUploadTime time.Time) {
	var t sql.NullTime
	sqlStatement := fmt.Sprintf(`select last_exec_at from %s where source_id='%s' and destination_id='%s' order by id desc limit 1`, warehouseutils.WarehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID)
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&t)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	if err == sql.ErrNoRows || !t.Valid {
		return
	}
	return t.Time
}

// canStartUpload indicates if a upload can be started now for the warehouse based on its configured schedule
func (wh *HandleT) canStartUpload(warehouse warehouseutils.WarehouseT) bool {
	if warehouseSyncFreqIgnore {
		return !uploadFrequencyExceeded(warehouse, "")
	}
	syncFrequency := warehouseutils.GetConfigValue(warehouseutils.SyncFrequency, warehouse)
	syncStartAt := warehouseutils.GetConfigValue(warehouseutils.SyncStartAt, warehouse)
	if syncFrequency != "" && syncStartAt != "" {
		prevScheduledTime := GetPrevScheduledTime(syncFrequency, syncStartAt, time.Now())
		lastUploadExecTime := wh.getLastUploadStartTime(warehouse)
		// start upload only if no upload has started in current window
		// eg. with prev scheduled time 14:00 and current time 15:00, start only if prev upload hasn't started after 14:00
		if lastUploadExecTime.Before(prevScheduledTime) {
			return true
		}
	} else {
		return !uploadFrequencyExceeded(warehouse, syncFrequency)
	}
	return false
}
