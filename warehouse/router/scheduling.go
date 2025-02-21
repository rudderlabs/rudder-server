package router

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/samber/lo/mutable"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	errUploadFrequencyExceeded          = fmt.Errorf("upload frequency exceeded")
	errCurrentTimeExistsInExcludeWindow = fmt.Errorf("current time exists in exclude window")
	errBeforeScheduledTime              = fmt.Errorf("before scheduled time")
)

type createUploadAlwaysLoader interface {
	Load() bool
}

// canCreateUpload indicates if an upload can be started now for the warehouse based on its configured schedule
func (r *Router) canCreateUpload(ctx context.Context, warehouse model.Warehouse) error {
	// can be set from rudder-cli to force uploads always
	if r.createUploadAlways.Load() {
		return nil
	}

	// return true if the upload was triggered
	if _, isTriggered := r.triggerStore.Load(warehouse.Identifier); isTriggered {
		return nil
	}

	if r.config.warehouseSyncFreqIgnore.Load() {
		if r.uploadFrequencyExceeded(warehouse, "") {
			return nil
		}
		return errUploadFrequencyExceeded
	}

	// gets exclude window start time and end time
	excludeWindow := warehouse.GetMapDestinationConfig(model.ExcludeWindowSetting)
	excludeWindowStartTime, excludeWindowEndTime := excludeWindowStartEndTimes(excludeWindow)

	if checkCurrentTimeExistsInExcludeWindow(r.now().UTC(), excludeWindowStartTime, excludeWindowEndTime) {
		return errCurrentTimeExistsInExcludeWindow
	}

	syncFrequency := warehouse.GetStringDestinationConfig(r.conf, model.SyncFrequencySetting)
	syncStartAt := warehouse.GetStringDestinationConfig(r.conf, model.SyncStartAtSetting)
	if syncFrequency == "" || syncStartAt == "" {
		if r.uploadFrequencyExceeded(warehouse, syncFrequency) {
			return nil
		}
		return errUploadFrequencyExceeded
	}

	prevScheduledTime := r.prevScheduledTime(syncFrequency, syncStartAt, r.now())
	lastUploadCreatedAt, err := r.uploadRepo.LastCreatedAt(ctx, warehouse.Source.ID, warehouse.Destination.ID)
	if err != nil {
		return err
	}

	// start upload only if no upload has started in current window
	// e.g. with prev scheduled time 14:00 and current time 15:00, start only if prev upload hasn't started after 14:00
	if lastUploadCreatedAt.Before(prevScheduledTime) {
		return nil
	}
	return errBeforeScheduledTime
}

func excludeWindowStartEndTimes(excludeWindow map[string]interface{}) (string, string) {
	var startTime, endTime string

	if st, ok := excludeWindow[whutils.ExcludeWindowStartTime].(string); ok {
		startTime = st
	}
	if et, ok := excludeWindow[whutils.ExcludeWindowEndTime].(string); ok {
		endTime = et
	}
	return startTime, endTime
}

func checkCurrentTimeExistsInExcludeWindow(currentTime time.Time, windowStartTime, windowEndTime string) bool {
	if windowStartTime == "" || windowEndTime == "" {
		return false
	}

	startTimeInMin := timeutil.MinsOfDay(windowStartTime)
	endTimeInMin := timeutil.MinsOfDay(windowEndTime)

	currentTimeMins := timeutil.GetElapsedMinsInThisDay(currentTime)

	// startTime, currentTime, endTime: 05:09, 06:19, 09:07 - > window between this day 05:09 and 09:07
	if startTimeInMin < currentTimeMins && currentTimeMins < endTimeInMin {
		return true
	}
	// startTime, currentTime, endTime: 22:09, 06:19, 09:07 -> window between this day 22:09 and tomorrow 09:07
	if startTimeInMin > currentTimeMins && currentTimeMins < endTimeInMin && startTimeInMin > endTimeInMin {
		return true
	}
	// startTime, currentTime, endTime: 22:09, 23:19, 09:07 -> window between this day 22:09 and tomorrow 09:07
	if startTimeInMin < currentTimeMins && currentTimeMins > endTimeInMin && startTimeInMin > endTimeInMin {
		return true
	}
	return false
}

// prevScheduledTime returns the closest previous scheduled time
// e.g. Syncing every 3hrs starting at 13:00 (scheduled times: 13:00, 16:00, 19:00, 22:00, 01:00, 04:00, 07:00, 10:00)
// prev scheduled time for current time (e.g. 18:00 -> 16:00 same day, 00:30 -> 22:00 prev day)
func (r *Router) prevScheduledTime(syncFrequency, syncStartAt string, currTime time.Time) time.Time {
	allStartTimes := r.scheduledTimes(syncFrequency, syncStartAt)

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

// scheduledTimes returns all possible start times (minutes from start of day) as per schedule
// e.g. Syncing every 3hrs starting at 13:00 (scheduled times: 13:00, 16:00, 19:00, 22:00, 01:00, 04:00, 07:00, 10:00)
func (r *Router) scheduledTimes(syncFrequency, syncStartAt string) []int {
	r.scheduledTimesCacheLock.RLock()
	cachedTimes, ok := r.scheduledTimesCache[fmt.Sprintf(`%s-%s`, syncFrequency, syncStartAt)]
	r.scheduledTimesCacheLock.RUnlock()

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
	mutable.Reverse(prependTimes)
	times = append(prependTimes, times...)

	r.scheduledTimesCacheLock.Lock()
	r.scheduledTimesCache[fmt.Sprintf(`%s-%s`, syncFrequency, syncStartAt)] = times
	r.scheduledTimesCacheLock.Unlock()

	return times
}
