package warehouse

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/tidwall/gjson"
)

const moduleName = "warehouse"

type tag struct {
	name  string
	value string
}

func getWarehouseTagName(destID, sourceName, destName, sourceID string) string {
	return misc.GetTagName(destID, sourceName, destName, misc.TailTruncateStr(sourceID, 6))
}

func (job *UploadJobT) warehouseID() string {
	return getWarehouseTagName(job.warehouse.Destination.ID, job.warehouse.Source.Name, job.warehouse.Destination.Name, job.warehouse.Source.ID)
}

func (jobRun *JobRunT) warehouseID() string {
	return getWarehouseTagName(jobRun.job.DestinationID, jobRun.job.SourceName, jobRun.job.DestinationName, jobRun.job.SourceID)
}

func (job *UploadJobT) timerStat(name string, extraTags ...tag) stats.RudderStats {
	tags := map[string]string{
		"module":      moduleName,
		"destType":    job.warehouse.Type,
		"warehouseID": job.warehouseID(),
		"destID":      job.upload.DestinationID,
		"sourceID":    job.upload.SourceID,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.name] = extraTag.value
	}
	return stats.NewTaggedStat(name, stats.TimerType, tags)
}

func (job *UploadJobT) counterStat(name string, extraTags ...tag) stats.RudderStats {
	tags := map[string]string{
		"module":      moduleName,
		"destType":    job.warehouse.Type,
		"warehouseID": job.warehouseID(),
		"destID":      job.upload.DestinationID,
		"sourceID":    job.upload.SourceID,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.name] = extraTag.value
	}
	return stats.NewTaggedStat(name, stats.CountType, tags)
}

func (job *UploadJobT) guageStat(name string, extraTags ...tag) stats.RudderStats {
	tags := map[string]string{
		"module":         moduleName,
		"destType":       job.warehouse.Type,
		"warehouseID":    job.warehouseID(),
		"destID":         job.upload.DestinationID,
		"sourceID":       job.upload.SourceID,
		"sourceCategory": job.upload.SourceCategory,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.name] = extraTag.value
	}
	return stats.NewTaggedStat(name, stats.GaugeType, tags)
}

func (jobRun *JobRunT) timerStat(name string, extraTags ...tag) stats.RudderStats {
	tags := map[string]string{
		"module":      moduleName,
		"destType":    jobRun.job.DestinationType,
		"warehouseID": jobRun.warehouseID(),
		"destID":      jobRun.job.DestinationID,
		"sourceID":    jobRun.job.SourceID,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.name] = extraTag.value
	}
	return stats.NewTaggedStat(name, stats.TimerType, tags)
}

func (jobRun *JobRunT) counterStat(name string, extraTags ...tag) stats.RudderStats {
	tags := map[string]string{
		"module":      moduleName,
		"destType":    jobRun.job.DestinationType,
		"warehouseID": jobRun.warehouseID(),
		"destID":      jobRun.job.DestinationID,
		"sourceID":    jobRun.job.SourceID,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.name] = extraTag.value
	}
	return stats.NewTaggedStat(name, stats.CountType, tags)
}

func (job *UploadJobT) generateUploadSuccessMetrics() {
	// Total loaded events in the upload
	numUploadedEvents, err := job.getTotalEventsUploaded(true)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to generate load metrics: %s, Err: %v", job.warehouse.Identifier, err)
		return
	}
	job.counterStat("total_rows_synced").Count(int(numUploadedEvents))

	// Total staged events in the upload
	numStagedEvents, err := getTotalEventsStaged(job.upload.StartStagingFileID, job.upload.EndStagingFileID)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to generate stage metrics: %s, Err: %v", job.warehouse.Identifier, err)
		return
	}
	job.counterStat("num_staged_events").Count(int(numStagedEvents))
	attempts := job.getAttemptNumber()
	job.counterStat("upload_success", tag{name: "attempt_number", value: strconv.Itoa(attempts)}).Count(1)
}

func (job *UploadJobT) generateUploadAbortedMetrics() {
	// Total successfully loaded events in the upload
	numUploadedEvents, err := job.getTotalEventsUploaded(true)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to generate load metrics: %s, Err: %v", job.warehouse.Identifier, err)
		return
	}
	job.counterStat("total_rows_synced").Count(int(numUploadedEvents))

	// Total staged events in the upload
	numStagedEvents, err := getTotalEventsStaged(job.upload.StartStagingFileID, job.upload.EndStagingFileID)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to generate stage metrics: %s, Err: %v", job.warehouse.Identifier, err)
		return
	}

	job.counterStat("num_staged_events").Count(int(numStagedEvents))

	// Set the upload_aborted stat
	attempts := job.getAttemptNumber()
	tags := []tag{{name: "attempt_number", value: strconv.Itoa(attempts)}}
	valid, err := job.validateDestinationCredentials()
	if err == nil {
		// Only if error is nil, meaning we were able to
		// successfully validate the creds, we set this tag
		tags = append(tags, tag{name: "destination_creds_valid", value: strconv.FormatBool(valid)})
	}
	job.counterStat("upload_aborted", tags...).Count(1)
}

func (job *UploadJobT) recordTableLoad(tableName string, numEvents int64) {
	rudderAPISupportedEventTypes := []string{"tracks", "identifies", "pages", "screens", "aliases", "groups"}
	if misc.ContainsString(rudderAPISupportedEventTypes, strings.ToLower(tableName)) {
		// record total events synced (ignoring additional row synced to event table for eg.track call)
		job.counterStat(`event_delivery`, tag{name: "tableName", value: strings.ToLower(tableName)}).Count(int(numEvents))
	}

	skipMetricTagForEachEventTable := config.GetBool("Warehouse.skipMetricTagForEachEventTable", false)
	if skipMetricTagForEachEventTable {
		standardTablesToRecordEventsMetric := []string{"tracks", "users", "identifies", "pages", "screens", "aliases", "groups", "rudder_discards"}
		if !misc.ContainsString(standardTablesToRecordEventsMetric, strings.ToLower(tableName)) {
			// club all event table metric tags under one tag to avoid too many tags
			tableName = "others"
		}
	}

	job.counterStat(`rows_synced`, tag{name: "tableName", value: strings.ToLower(tableName)}).Count(int(numEvents))
	// Delay for the oldest event in the batch
	firstEventAt, err := getFirstStagedEventAt(job.upload.StartStagingFileID)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to generate delay metrics: %s, Err: %v", job.warehouse.Identifier, err)
		return
	}

	retried := gjson.GetBytes(job.upload.Metadata, "retried").Bool()
	if !retried {
		config := job.warehouse.Destination.Config
		syncFrequency := "1440"
		if config[warehouseutils.SyncFrequency] != nil {
			syncFrequency, _ = config[warehouseutils.SyncFrequency].(string)
		}
		job.timerStat("event_delivery_time", tag{name: "tableName", value: strings.ToLower(tableName)}, tag{name: "syncFrequency", value: syncFrequency}).SendTiming(time.Since(firstEventAt))
	}
}

func (job *UploadJobT) recordLoadFileGenerationTimeStat(startID, endID int64) (err error) {
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

func recordStagedRowsStat(totalEvents int, destType, destID, sourceName, destName, sourceID string) {
	tags := map[string]string{
		"module":      moduleName,
		"destType":    destType,
		"warehouseID": getWarehouseTagName(destID, sourceName, destName, sourceID),
	}
	stats.NewTaggedStat("rows_staged", stats.CountType, tags).Count(totalEvents)
}

func getUploadStatusStat(name, destType, destID, sourceName, destName, sourceID string) stats.RudderStats {
	tags := map[string]string{
		"module":      moduleName,
		"destType":    destType,
		"warehouseID": getWarehouseTagName(destID, sourceName, destName, sourceID),
	}
	return stats.NewTaggedStat(name, stats.CountType, tags)
}

func persisteSSLFileErrorStat(destType, destName, destID, sourceName, sourceID, errTag string) {
	tags := map[string]string{
		"module":        moduleName,
		"destType":      destType,
		"warehouseID":   getWarehouseTagName(destID, sourceName, destName, sourceID),
		"destinationID": destID,
		"errTag":        errTag,
	}
	stats.NewTaggedStat("persist_ssl_file_failure", stats.CountType, tags).Count(1)
}
