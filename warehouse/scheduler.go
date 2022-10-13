//go:generate mockgen -source=scheduler.go -destination=../mocks/warehouse/mock_scheduler.go -package=mock_warehouse github.com/rudderlabs/rudder-server/warehouse Scheduler

package warehouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/rudderlabs/rudder-server/warehouse/db"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	scheduledTimesCache     map[string][]int
	minUploadBackoff        time.Duration
	maxUploadBackoff        time.Duration
	startUploadAlways       bool
	scheduledTimesCacheLock sync.RWMutex
	enableJitterForSyncs    bool
)

type Scheduler interface {
	createUploadJobsFromStagingFiles(w warehouseutils.Warehouse, stagingFilesList []*StagingFileT, priority int, uploadStartAfter time.Time)
	createJobs(w warehouseutils.Warehouse) (err error)
	getLatestUploadStatus(w *warehouseutils.Warehouse) (int64, string, int)
	canCreateUpload(w warehouseutils.Warehouse) bool
	getLastUploadCreatedAt(w warehouseutils.Warehouse) time.Time
	getPendingStagingFiles(w warehouseutils.Warehouse) ([]*StagingFileT, error)
	initUpload(w warehouseutils.Warehouse, jsonUploadsList []*StagingFileT, isUploadTriggered bool, priority int, uploadStartAfter time.Time)
	isUploadJobInProgress(w warehouseutils.Warehouse, jobID int64) (inProgressIdx int, inProgress bool)
	deleteWaitingUploadJob(jobID int64)
}

type SchedulerImpl struct {
	destType                          string
	dbHandle                          *sql.DB
	warehouseDBHandle                 db.DB
	areBeingEnqueuedLock              *sync.RWMutex
	inProgressMap                     map[WorkerIdentifierT][]JobIDT
	allowMultipleSourcesForJobsPickup bool
}

func Init3() {
	scheduledTimesCache = map[string][]int{}
	loadConfigScheduling()
}

func loadConfigScheduling() {
	config.RegisterDurationConfigVariable(60, &minUploadBackoff, true, time.Second, []string{"Warehouse.minUploadBackoff", "Warehouse.minUploadBackoffInS"}...)
	config.RegisterDurationConfigVariable(1800, &maxUploadBackoff, true, time.Second, []string{"Warehouse.maxUploadBackoff", "Warehouse.maxUploadBackoffInS"}...)
	config.RegisterBoolConfigVariable(false, &enableJitterForSyncs, true, "Warehouse.enableJitterForSyncs")
}

func NewScheduler(
	destType string,
	dbHandle *sql.DB,
	warehouseDBHandle db.DB,
	areBeingEnqueuedLock *sync.RWMutex,
	inProgressMap map[WorkerIdentifierT][]JobIDT,
	allowMultipleSourcesForJobsPickup bool,
) Scheduler {
	return &SchedulerImpl{
		destType:                          destType,
		dbHandle:                          dbHandle,
		warehouseDBHandle:                 warehouseDBHandle,
		areBeingEnqueuedLock:              areBeingEnqueuedLock,
		inProgressMap:                     inProgressMap,
		allowMultipleSourcesForJobsPickup: allowMultipleSourcesForJobsPickup,
	}
}

func (s *SchedulerImpl) createUploadJobsFromStagingFiles(warehouse warehouseutils.Warehouse, stagingFilesList []*StagingFileT, priority int, uploadStartAfter time.Time) {
	// count := 0
	// Process staging files in batches of stagingFilesBatchSize
	// E.g. If there are 1000 pending staging files and stagingFilesBatchSize is 100,
	// Then we create 10 new entries in wh_uploads table each with 100 staging files
	var stagingFilesInUpload []*StagingFileT
	var counter int
	uploadTriggered := isUploadTriggered(warehouse)

	initUpload := func() {
		s.initUpload(warehouse, stagingFilesInUpload, uploadTriggered, priority, uploadStartAfter)
		stagingFilesInUpload = []*StagingFileT{}
		counter = 0
	}
	for idx, sFile := range stagingFilesList {
		if idx > 0 && counter > 0 && sFile.UseRudderStorage != stagingFilesList[idx-1].UseRudderStorage {
			initUpload()
		}

		stagingFilesInUpload = append(stagingFilesInUpload, sFile)
		counter++
		if counter == stagingFilesBatchSize || idx == len(stagingFilesList)-1 {
			initUpload()
		}
	}

	// reset upload trigger if the upload was triggered
	if uploadTriggered {
		clearTriggeredUpload(warehouse)
	}
}

func (s *SchedulerImpl) createJobs(warehouse warehouseutils.Warehouse) (err error) {
	whManager, err := manager.New(s.destType)
	if err != nil {
		return err
	}

	// Step 1: Crash recovery after restart
	// Remove pending temp tables in Redshift etc.
	_, ok := inRecoveryMap[warehouse.Destination.ID]
	if ok {
		pkgLogger.Infof("[WH]: Crash recovering for %s:%s", s.destType, warehouse.Destination.ID)
		err = whManager.CrashRecover(warehouse)
		if err != nil {
			return err
		}
		delete(inRecoveryMap, warehouse.Destination.ID)
	}

	if !s.canCreateUpload(warehouse) {
		pkgLogger.Debugf("[WH]: Skipping upload loop since %s upload freq not exceeded", warehouse.Identifier)
		return nil
	}

	s.areBeingEnqueuedLock.Lock()

	priority := 0
	uploadID, uploadStatus, uploadPriority := s.getLatestUploadStatus(&warehouse)
	if uploadStatus == Waiting {
		// If it is present do nothing else delete it
		if _, inProgress := s.isUploadJobInProgress(warehouse, uploadID); !inProgress {
			s.deleteWaitingUploadJob(uploadID)
			priority = uploadPriority // copy the priority from the latest upload job.
		}
	}

	s.areBeingEnqueuedLock.Unlock()

	stagingFilesFetchStat := stats.Default.NewTaggedStat("wh_scheduler.pending_staging_files", stats.TimerType, stats.Tags{
		"workspaceId":   warehouse.WorkspaceID,
		"destinationID": warehouse.Destination.ID,
		"destType":      warehouse.Destination.DestinationDefinition.Name,
	})
	stagingFilesFetchStat.Start()
	stagingFilesList, err := s.getPendingStagingFiles(warehouse)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to get pending staging files: %s with error %v", warehouse.Identifier, err)
		return err
	}
	stagingFilesFetchStat.End()

	if len(stagingFilesList) == 0 {
		pkgLogger.Debugf("[WH]: Found no pending staging files for %s", warehouse.Identifier)
		return nil
	}

	uploadJobCreationStat := stats.Default.NewTaggedStat("wh_scheduler.create_upload_jobs", stats.TimerType, stats.Tags{
		"workspaceId":   warehouse.WorkspaceID,
		"destinationID": warehouse.Destination.ID,
		"destType":      warehouse.Destination.DestinationDefinition.Name,
	})
	uploadJobCreationStat.Start()

	uploadStartAfter := getUploadStartAfterTime()
	s.createUploadJobsFromStagingFiles(warehouse, stagingFilesList, priority, uploadStartAfter)
	setLastProcessedMarker(warehouse, uploadStartAfter)

	uploadJobCreationStat.End()

	return nil
}

func (s *SchedulerImpl) getLatestUploadStatus(warehouse *warehouseutils.Warehouse) (int64, string, int) {
	uploadID, status, priority, err := s.warehouseDBHandle.GetLatestUploadStatus(
		context.TODO(),
		warehouse.Type,
		warehouse.Source.ID,
		warehouse.Destination.ID)
	if err != nil {
		pkgLogger.Errorf(`Error getting latest upload status for warehouse: %v`, err)
	}

	return uploadID, status, priority
}

// canCreateUpload indicates if an upload can be started now for the warehouse based on its configured schedule
func (s *SchedulerImpl) canCreateUpload(warehouse warehouseutils.Warehouse) bool {
	// can be set from rudder-cli to force uploads always
	if startUploadAlways {
		return true
	}
	// return true if the upload was triggered
	if isUploadTriggered(warehouse) {
		return true
	}
	if warehouseSyncFreqIgnore {
		return !uploadFrequencyExceeded(warehouse, "")
	}
	// gets exclude window start time and end time
	excludeWindow := warehouseutils.GetConfigValueAsMap(warehouseutils.ExcludeWindow, warehouse.Destination.Config)
	excludeWindowStartTime, excludeWindowEndTime := GetExcludeWindowStartEndTimes(excludeWindow)
	if CheckCurrentTimeExistsInExcludeWindow(timeutil.Now(), excludeWindowStartTime, excludeWindowEndTime) {
		return false
	}
	syncFrequency := warehouseutils.GetConfigValue(warehouseutils.SyncFrequency, warehouse)
	syncStartAt := warehouseutils.GetConfigValue(warehouseutils.SyncStartAt, warehouse)
	if syncFrequency == "" || syncStartAt == "" {
		return !uploadFrequencyExceeded(warehouse, syncFrequency)
	}
	prevScheduledTime := GetPrevScheduledTime(syncFrequency, syncStartAt, time.Now())
	lastUploadCreatedAt := s.getLastUploadCreatedAt(warehouse)
	// start upload only if no upload has started in current window
	// e.g. with prev scheduled time 14:00 and current time 15:00, start only if prev upload hasn't started after 14:00
	return lastUploadCreatedAt.Before(prevScheduledTime)
}

// getLastUploadCreatedAt returns the start time of the last upload
func (s *SchedulerImpl) getLastUploadCreatedAt(warehouse warehouseutils.Warehouse) time.Time {
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
	err := s.dbHandle.QueryRow(sqlStatement).Scan(&t)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	if err == sql.ErrNoRows || !t.Valid {
		return time.Time{} // zero value
	}
	return t.Time
}

func (s *SchedulerImpl) getPendingStagingFiles(warehouse warehouseutils.Warehouse) ([]*StagingFileT, error) {
	var lastStagingFileID int64
	sqlStatement := fmt.Sprintf(`
	SELECT
	  end_staging_file_id
	FROM
	  %[1]s UT
	WHERE
	  UT.destination_type = '%[2]s'
	  AND UT.source_id = '%[3]s'
	  AND UT.destination_id = '%[4]s'
	ORDER BY
	  UT.id DESC;
`,
		warehouseutils.WarehouseUploadsTable,
		warehouse.Type,
		warehouse.Source.ID,
		warehouse.Destination.ID,
	)

	err := s.dbHandle.QueryRow(sqlStatement).Scan(&lastStagingFileID)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("query: %s failed with Error : %w", sqlStatement, err))
	}

	sqlStatement = fmt.Sprintf(`
		SELECT
		  id,
		  location,
		  status,
		  first_event_at,
		  last_event_at,
		  metadata ->> 'source_batch_id',
		  metadata ->> 'source_task_id',
		  metadata ->> 'source_task_run_id',
		  metadata ->> 'source_job_id',
		  metadata ->> 'source_job_run_id',
		  metadata ->> 'use_rudder_storage',
		  metadata ->> 'time_window_year',
		  metadata ->> 'time_window_month',
		  metadata ->> 'time_window_day',
		  metadata ->> 'time_window_hour',
		  metadata ->> 'destination_revision_id'
		FROM
		  %[1]s ST
		WHERE
		  ST.id > %[2]v
		  AND ST.source_id = '%[3]s'
		  AND ST.destination_id = '%[4]s'
		ORDER BY
		  id ASC;
`,
		warehouseutils.WarehouseStagingFilesTable,
		lastStagingFileID,
		warehouse.Source.ID,
		warehouse.Destination.ID,
	)
	rows, err := s.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("query: %s failed with Error : %w", sqlStatement, err))
	}
	defer rows.Close()

	var stagingFilesList []*StagingFileT
	var firstEventAt, lastEventAt sql.NullTime
	var sourceBatchID, sourceTaskID, sourceTaskRunID, sourceJobID, sourceJobRunID, destinationRevisionID sql.NullString
	var timeWindowYear, timeWindowMonth, timeWindowDay, timeWindowHour sql.NullInt64
	var UseRudderStorage sql.NullBool
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(
			&jsonUpload.ID,
			&jsonUpload.Location,
			&jsonUpload.Status,
			&firstEventAt,
			&lastEventAt,
			&sourceBatchID,
			&sourceTaskID,
			&sourceTaskRunID,
			&sourceJobID,
			&sourceJobRunID,
			&UseRudderStorage,
			&timeWindowYear,
			&timeWindowMonth,
			&timeWindowDay,
			&timeWindowHour,
			&destinationRevisionID,
		)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		jsonUpload.FirstEventAt = firstEventAt.Time
		jsonUpload.LastEventAt = lastEventAt.Time
		jsonUpload.TimeWindow = time.Date(int(timeWindowYear.Int64), time.Month(timeWindowMonth.Int64), int(timeWindowDay.Int64), int(timeWindowHour.Int64), 0, 0, 0, time.UTC)
		jsonUpload.UseRudderStorage = UseRudderStorage.Bool
		jsonUpload.DestinationRevisionID = destinationRevisionID.String
		// add cloud sources metadata
		jsonUpload.SourceBatchID = sourceBatchID.String
		jsonUpload.SourceTaskID = sourceTaskID.String
		jsonUpload.SourceTaskRunID = sourceTaskRunID.String
		jsonUpload.SourceJobID = sourceJobID.String
		jsonUpload.SourceJobRunID = sourceJobRunID.String
		stagingFilesList = append(stagingFilesList, &jsonUpload)
	}

	return stagingFilesList, nil
}

func (s *SchedulerImpl) initUpload(warehouse warehouseutils.Warehouse, jsonUploadsList []*StagingFileT, isUploadTriggered bool, priority int, uploadStartAfter time.Time) {
	sqlStatement := fmt.Sprintf(`
		INSERT INTO %s (
		  source_id, namespace, workspace_id, destination_id,
		  destination_type, start_staging_file_id,
		  end_staging_file_id, start_load_file_id,
		  end_load_file_id, status, schema,
		  error, metadata, first_event_at,
		  last_event_at, created_at, updated_at
		)
		VALUES
		  (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17
		  ) RETURNING id;
`,
		warehouseutils.WarehouseUploadsTable,
	)
	pkgLogger.Infof("WH: %s: Creating record in %s table: %v", s.destType, warehouseutils.WarehouseUploadsTable, sqlStatement)
	stmt, err := s.dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	startJSONID := jsonUploadsList[0].ID
	endJSONID := jsonUploadsList[len(jsonUploadsList)-1].ID
	namespace := warehouse.Namespace

	var firstEventAt, lastEventAt time.Time
	if ok := jsonUploadsList[0].FirstEventAt.IsZero(); !ok {
		firstEventAt = jsonUploadsList[0].FirstEventAt
	}
	if ok := jsonUploadsList[len(jsonUploadsList)-1].LastEventAt.IsZero(); !ok {
		lastEventAt = jsonUploadsList[len(jsonUploadsList)-1].LastEventAt
	}

	now := timeutil.Now()
	metadataMap := map[string]interface{}{
		"use_rudder_storage": jsonUploadsList[0].UseRudderStorage, // TODO: Since the use_rudder_storage is now being populated for both the staging and load files. Let's try to leverage it instead of hard coding it from the first staging file.
		"source_batch_id":    jsonUploadsList[0].SourceBatchID,
		"source_task_id":     jsonUploadsList[0].SourceTaskID,
		"source_task_run_id": jsonUploadsList[0].SourceTaskRunID,
		"source_job_id":      jsonUploadsList[0].SourceJobID,
		"source_job_run_id":  jsonUploadsList[0].SourceJobRunID,
		"load_file_type":     warehouseutils.GetLoadFileType(s.destType),
		"nextRetryTime":      uploadStartAfter.Format(time.RFC3339),
	}
	if isUploadTriggered {
		// set priority to 50 if the upload was manually triggered
		metadataMap["priority"] = 50
	}
	if priority != 0 {
		metadataMap["priority"] = priority
	}
	metadata, err := json.Marshal(metadataMap)
	if err != nil {
		panic(err)
	}
	row := stmt.QueryRow(
		warehouse.Source.ID,
		namespace,
		warehouse.WorkspaceID,
		warehouse.Destination.ID,
		s.destType,
		startJSONID,
		endJSONID,
		0,
		0,
		Waiting,
		"{}",
		"{}",
		metadata,
		firstEventAt,
		lastEventAt,
		now,
		now,
	)

	var uploadID int64
	err = row.Scan(&uploadID)
	if err != nil {
		panic(err)
	}
}

func (s *SchedulerImpl) isUploadJobInProgress(warehouse warehouseutils.Warehouse, jobID int64) (inProgressIdx int, inProgress bool) {
	identifier := s.workerIdentifier(warehouse)
	for idx, id := range s.inProgressMap[WorkerIdentifierT(identifier)] {
		if jobID == int64(id) {
			inProgress = true
			inProgressIdx = idx
			return
		}
	}
	return
}

func (s *SchedulerImpl) deleteWaitingUploadJob(jobID int64) {
	sqlStatement := fmt.Sprintf(`
		DELETE FROM
		  %s
		WHERE
		  id = %d
		  AND status = '%s';
`,
		warehouseutils.WarehouseUploadsTable,
		jobID,
		Waiting,
	)
	_, err := s.dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf(`Error deleting upload job: %d in waiting state: %v`, jobID, err)
	}
}

// get name of the worker (`destID_namespace`) to be stored in map wh.workerChannelMap
func (s *SchedulerImpl) workerIdentifier(warehouse warehouseutils.Warehouse) (identifier string) {
	identifier = fmt.Sprintf(`%s_%s`, warehouse.Destination.ID, warehouse.Namespace)

	if s.allowMultipleSourcesForJobsPickup {
		identifier = fmt.Sprintf(`%s_%s_%s`, warehouse.Source.ID, warehouse.Destination.ID, warehouse.Namespace)
	}
	return
}

// GetPrevScheduledTime returns closest previous scheduled time
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
	times = append(misc.ReverseInt(prependTimes), times...)
	scheduledTimesCacheLock.Lock()
	scheduledTimesCache[fmt.Sprintf(`%s-%s`, syncFrequency, syncStartAt)] = times
	scheduledTimesCacheLock.Unlock()
	return times
}

func uploadFrequencyExceeded(warehouse warehouseutils.Warehouse, syncFrequency string) bool {
	freqInS := getUploadFreqInS(syncFrequency)
	lastProcessedMarkerMapLock.Lock()
	defer lastProcessedMarkerMapLock.Unlock()
	if lastExecTime, ok := lastProcessedMarkerMap[warehouse.Identifier]; ok && timeutil.Now().Unix()-lastExecTime < freqInS {
		return true
	}
	return false
}

func getUploadFreqInS(syncFrequency string) int64 {
	freqInS := uploadFreqInS
	if syncFrequency != "" {
		freqInMin, _ := strconv.ParseInt(syncFrequency, 10, 64)
		freqInS = freqInMin * 60
	}
	return freqInS
}

func GetExcludeWindowStartEndTimes(excludeWindow map[string]interface{}) (string, string) {
	var startTime, endTime string
	if time, ok := excludeWindow[warehouseutils.ExcludeWindowStartTime].(string); ok {
		startTime = time
	}
	if time, ok := excludeWindow[warehouseutils.ExcludeWindowEndTime].(string); ok {
		endTime = time
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

func setLastProcessedMarker(warehouse warehouseutils.Warehouse, lastProcessedTime time.Time) {
	lastProcessedMarkerMapLock.Lock()
	defer lastProcessedMarkerMapLock.Unlock()
	lastProcessedMarkerMap[warehouse.Identifier] = lastProcessedTime.Unix()
}

func getUploadStartAfterTime() time.Time {
	if enableJitterForSyncs {
		return timeutil.Now().Add(time.Duration(rand.Intn(15)) * time.Second)
	}
	return time.Now()
}

func isUploadTriggered(wh warehouseutils.Warehouse) bool {
	triggerUploadsMapLock.Lock()
	isTriggered := triggerUploadsMap[wh.Identifier]
	triggerUploadsMapLock.Unlock()
	return isTriggered
}

func clearTriggeredUpload(wh warehouseutils.Warehouse) {
	triggerUploadsMapLock.Lock()
	delete(triggerUploadsMap, wh.Identifier)
	triggerUploadsMapLock.Unlock()
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
