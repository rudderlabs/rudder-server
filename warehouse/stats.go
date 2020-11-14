package warehouse

import (
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const moduleName = "warehouse"

type tag struct {
	name  string
	value string
}

func (job *UploadJobT) warehouseID() string {
	return misc.GetTagName(job.warehouse.Destination.ID, job.warehouse.Source.Name, job.warehouse.Destination.Name)
}

func (job *UploadJobT) timerStat(name string, extraTags ...tag) stats.RudderStats {
	tags := map[string]string{
		"module":      moduleName,
		"destType":    job.warehouse.Type,
		"warehouseID": job.warehouseID(),
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
	}
	for _, extraTag := range extraTags {
		tags[extraTag.name] = extraTag.value
	}
	return stats.NewTaggedStat(name, stats.CountType, tags)
}

func (jobRun *JobRunT) timerStat(name string) stats.RudderStats {
	return stats.NewTaggedStat(name, stats.TimerType, map[string]string{
		"module":      moduleName,
		"destType":    jobRun.job.DestinationType,
		"warehouseID": jobRun.whIdentifier,
	})
}

func (jobRun *JobRunT) counterStat(name string) stats.RudderStats {
	return stats.NewTaggedStat(name, stats.CountType, map[string]string{
		"module":      moduleName,
		"destType":    jobRun.job.DestinationType,
		"warehouseID": jobRun.whIdentifier,
	})
}

func (job *UploadJobT) generateUploadSuccessMetrics() {
	// Total loaded events in the upload
	numUploadedEvents, err := getTotalEventsUploaded(job.upload.ID)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to generate load metrics: %s, Err: %w", job.warehouse.Identifier, err)
		return
	}
	job.counterStat("rows_synced").Count(int(numUploadedEvents))

	// Total staged events in the upload
	numStagedEvents, err := getTotalEventsStaged(job.upload.StartStagingFileID, job.upload.EndStagingFileID)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to generate stage metrics: %s, Err: %w", job.warehouse.Identifier, err)
		return
	}
	job.counterStat("num_staged_events").Count(int(numStagedEvents))

	// Delay for the oldest event in the batch
	firstEventAt, err := getFirstStagedEventAt(job.upload.StartStagingFileID)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to generate delay metrics: %s, Err: %w", job.warehouse.Identifier, err)
		return
	}

	job.timerStat("event_delivery_time").SendTiming(time.Now().Sub(firstEventAt))

	job.counterStat("upload_success").Count(1)
}

func (job *UploadJobT) generateUploadAbortedMetrics() {
	// Total successfully loaded events in the upload
	numUploadedEvents, err := getTotalEventsUploaded(job.upload.ID)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to generate load metrics: %s, Err: %w", job.warehouse.Identifier, err)
		return
	}
	job.counterStat("rows_synced").Count(int(numUploadedEvents))

	// Total staged events in the upload
	numStagedEvents, err := getTotalEventsStaged(job.upload.StartStagingFileID, job.upload.EndStagingFileID)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to generate stage metrics: %s, Err: %w", job.warehouse.Identifier, err)
		return
	}
	job.counterStat("num_staged_events").Count(int(numStagedEvents))

	job.counterStat("upload_aborted").Count(1)
}

func (job *UploadJobT) recordTableLoad(tableName string, numEvents int64) {
	// add metric to record total loaded rows to standard tables
	// adding metric for all event tables might result in too many metrics
	tablesToRecordEventsMetric := []string{"tracks", "users", "identifies", "pages", "screens", "aliases", "groups", "rudder_discards"}
	if misc.Contains(tablesToRecordEventsMetric, strings.ToLower(tableName)) {
		job.counterStat(`rows_synced`, tag{name: "tableName", value: strings.ToLower(tableName)}).Count(int(numEvents))
		// Delay for the oldest event in the batch
		firstEventAt, err := getFirstStagedEventAt(job.upload.StartStagingFileID)
		if err != nil {
			pkgLogger.Errorf("[WH]: Failed to generate delay metrics: %s, Err: %w", job.warehouse.Identifier, err)
			return
		}
		job.timerStat("event_delivery_time", tag{name: "tableName", value: strings.ToLower(tableName)}).SendTiming(time.Now().Sub(firstEventAt))
	}
}
