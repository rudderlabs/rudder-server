package warehouse

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const moduleName = "warehouse"

type Tag struct {
	Name  string
	Value string
}

func getWarehouseTagName(destID, sourceName, destName, sourceID string) string {
	return misc.GetTagName(destID, sourceName, destName, misc.TailTruncateStr(sourceID, 6))
}

func (job *UploadJob) warehouseID() string {
	return getWarehouseTagName(job.warehouse.Destination.ID, job.warehouse.Source.Name, job.warehouse.Destination.Name, job.warehouse.Source.ID)
}

func (jobRun *JobRun) warehouseID() string {
	return getWarehouseTagName(jobRun.job.DestinationID, jobRun.job.SourceName, jobRun.job.DestinationName, jobRun.job.SourceID)
}

func (job *UploadJob) timerStat(name string, extraTags ...Tag) stats.Measurement {
	tags := stats.Tags{
		"module":      moduleName,
		"destType":    job.warehouse.Type,
		"warehouseID": job.warehouseID(),
		"workspaceId": job.upload.WorkspaceID,
		"destID":      job.upload.DestinationID,
		"sourceID":    job.upload.SourceID,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.Name] = extraTag.Value
	}
	return job.stats.NewTaggedStat(name, stats.TimerType, tags)
}

func (job *UploadJob) counterStat(name string, extraTags ...Tag) stats.Measurement {
	tags := stats.Tags{
		"module":      moduleName,
		"destType":    job.warehouse.Type,
		"warehouseID": job.warehouseID(),
		"workspaceId": job.upload.WorkspaceID,
		"destID":      job.upload.DestinationID,
		"sourceID":    job.upload.SourceID,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.Name] = extraTag.Value
	}
	return job.stats.NewTaggedStat(name, stats.CountType, tags)
}

func (job *UploadJob) guageStat(name string, extraTags ...Tag) stats.Measurement {
	tags := stats.Tags{
		"module":         moduleName,
		"destType":       job.warehouse.Type,
		"warehouseID":    job.warehouseID(),
		"workspaceId":    job.upload.WorkspaceID,
		"destID":         job.upload.DestinationID,
		"sourceID":       job.upload.SourceID,
		"sourceCategory": job.warehouse.Source.SourceDefinition.Category,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.Name] = extraTag.Value
	}
	return job.stats.NewTaggedStat(name, stats.GaugeType, tags)
}

func (jobRun *JobRun) timerStat(name string, extraTags ...Tag) stats.Measurement {
	tags := stats.Tags{
		"module":      moduleName,
		"destType":    jobRun.job.DestinationType,
		"warehouseID": jobRun.warehouseID(),
		"workspaceId": jobRun.job.WorkspaceID,
		"destID":      jobRun.job.DestinationID,
		"sourceID":    jobRun.job.SourceID,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.Name] = extraTag.Value
	}
	return jobRun.stats.NewTaggedStat(name, stats.TimerType, tags)
}

func (jobRun *JobRun) counterStat(name string, extraTags ...Tag) stats.Measurement {
	tags := stats.Tags{
		"module":      moduleName,
		"destType":    jobRun.job.DestinationType,
		"warehouseID": jobRun.warehouseID(),
		"workspaceId": jobRun.job.WorkspaceID,
		"destID":      jobRun.job.DestinationID,
		"sourceID":    jobRun.job.SourceID,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.Name] = extraTag.Value
	}
	return jobRun.stats.NewTaggedStat(name, stats.CountType, tags)
}

func (job *UploadJob) generateUploadSuccessMetrics() {
	var (
		numUploadedEvents int64
		numStagedEvents   int64
		err               error
	)
	numUploadedEvents, err = job.tableUploadsRepo.TotalExportedEvents(
		context.TODO(),
		job.upload.ID,
		[]string{},
	)
	if err != nil {
		pkgLogger.Warnw("sum of total exported events for upload",
			logfield.UploadJobID, job.upload.ID,
			logfield.SourceID, job.upload.SourceID,
			logfield.DestinationID, job.upload.DestinationID,
			logfield.DestinationType, job.upload.DestinationType,
			logfield.WorkspaceID, job.upload.WorkspaceID,
			logfield.Error, err.Error(),
		)
		return
	}

	numStagedEvents, err = repo.NewStagingFiles(dbHandle).TotalEventsForUpload(
		context.TODO(),
		job.upload,
	)
	if err != nil {
		pkgLogger.Warnw("total events for upload",
			logfield.UploadJobID, job.upload.ID,
			logfield.SourceID, job.upload.SourceID,
			logfield.DestinationID, job.upload.DestinationID,
			logfield.DestinationType, job.upload.DestinationType,
			logfield.WorkspaceID, job.upload.WorkspaceID,
			logfield.Error, err.Error(),
		)
		return
	}

	job.counterStat("total_rows_synced").Count(int(numUploadedEvents))
	job.counterStat("num_staged_events").Count(int(numStagedEvents))

	attempts := job.getAttemptNumber()
	job.counterStat("upload_success", Tag{
		Name:  "attempt_number",
		Value: strconv.Itoa(attempts),
	}).Count(1)
}

func (job *UploadJob) generateUploadAbortedMetrics() {
	var (
		numUploadedEvents int64
		numStagedEvents   int64
		err               error
	)
	numUploadedEvents, err = job.tableUploadsRepo.TotalExportedEvents(
		context.TODO(),
		job.upload.ID,
		[]string{},
	)
	if err != nil {
		pkgLogger.Warnw("sum of total exported events for upload",
			logfield.UploadJobID, job.upload.ID,
			logfield.SourceID, job.upload.SourceID,
			logfield.DestinationID, job.upload.DestinationID,
			logfield.DestinationType, job.upload.DestinationType,
			logfield.WorkspaceID, job.upload.WorkspaceID,
			logfield.Error, err.Error(),
		)
		return
	}

	numStagedEvents, err = repo.NewStagingFiles(dbHandle).TotalEventsForUpload(
		context.TODO(),
		job.upload,
	)
	if err != nil {
		pkgLogger.Warnw("total events for upload",
			logfield.UploadJobID, job.upload.ID,
			logfield.SourceID, job.upload.SourceID,
			logfield.DestinationID, job.upload.DestinationID,
			logfield.DestinationType, job.upload.DestinationType,
			logfield.WorkspaceID, job.upload.WorkspaceID,
			logfield.Error, err.Error(),
		)
		return
	}

	job.counterStat("total_rows_synced").Count(int(numUploadedEvents))
	job.counterStat("num_staged_events").Count(int(numStagedEvents))
}

func (job *UploadJob) recordTableLoad(tableName string, numEvents int64) {
	rudderAPISupportedEventTypes := []string{"tracks", "identifies", "pages", "screens", "aliases", "groups"}
	if misc.Contains(rudderAPISupportedEventTypes, strings.ToLower(tableName)) {
		// record total events synced (ignoring additional row synced to the event table for e.g.track call)
		job.counterStat(`event_delivery`, Tag{
			Name:  "tableName",
			Value: strings.ToLower(tableName),
		}).Count(int(numEvents))
	}

	skipMetricTagForEachEventTable := config.GetBool("Warehouse.skipMetricTagForEachEventTable", false)
	if skipMetricTagForEachEventTable {
		standardTablesToRecordEventsMetric := []string{"tracks", "users", "identifies", "pages", "screens", "aliases", "groups", "rudder_discards"}
		if !misc.Contains(standardTablesToRecordEventsMetric, strings.ToLower(tableName)) {
			// club all event table metric tags under one tag to avoid too many tags
			tableName = "others"
		}
	}

	job.counterStat(`rows_synced`, Tag{
		Name:  "tableName",
		Value: strings.ToLower(tableName),
	}).Count(int(numEvents))
	// Delay for the oldest event in the batch
	firstEventAt, err := repo.NewStagingFiles(dbHandle).FirstEventForUpload(context.TODO(), job.upload)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to generate delay metrics: %s, Err: %v", job.warehouse.Identifier, err)
		return
	}

	if !job.upload.Retried {
		config := job.warehouse.Destination.Config
		syncFrequency := "1440"
		if config[warehouseutils.SyncFrequency] != nil {
			syncFrequency, _ = config[warehouseutils.SyncFrequency].(string)
		}
		job.timerStat("event_delivery_time",
			Tag{Name: "tableName", Value: strings.ToLower(tableName)},
			Tag{Name: "syncFrequency", Value: syncFrequency},
		).Since(firstEventAt)
	}
}

func (job *UploadJob) recordLoadFileGenerationTimeStat(startID, endID int64) (err error) {
	stmt := fmt.Sprintf(`SELECT EXTRACT(EPOCH FROM (f2.created_at - f1.created_at))::integer as delta
		FROM (SELECT created_at FROM %[1]s WHERE id=%[2]d) f1
		CROSS JOIN
		(SELECT created_at FROM %[1]s WHERE id=%[3]d) f2
	`, warehouseutils.WarehouseLoadFilesTable, startID, endID)
	var timeTakenInS time.Duration
	err = job.dbHandle.QueryRow(stmt).Scan(&timeTakenInS)
	if err != nil {
		return
	}
	job.timerStat("load_file_generation_time").SendTiming(timeTakenInS * time.Second)
	return nil
}

func persistSSLFileErrorStat(workspaceID, destType, destName, destID, sourceName, sourceID, errTag string) {
	tags := stats.Tags{
		"workspaceId":   workspaceID,
		"module":        moduleName,
		"destType":      destType,
		"warehouseID":   getWarehouseTagName(destID, sourceName, destName, sourceID),
		"destinationID": destID,
		"errTag":        errTag,
	}
	stats.Default.NewTaggedStat("persist_ssl_file_failure", stats.CountType, tags).Count(1)
}
