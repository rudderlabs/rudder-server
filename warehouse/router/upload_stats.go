package router

import (
	"fmt"
	"strings"
	"time"

	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const moduleName = "warehouse"

func warehouseTagName(destID, sourceName, destName, sourceID string) string {
	return misc.GetTagName(destID, sourceName, destName, misc.TailTruncateStr(sourceID, 6))
}

func (job *UploadJob) buildTags(extraTags ...warehouseutils.Tag) stats.Tags {
	tags := stats.Tags{
		"module":      moduleName,
		"destType":    job.warehouse.Type,
		"warehouseID": warehouseTagName(job.warehouse.Destination.ID, job.warehouse.Source.Name, job.warehouse.Destination.Name, job.warehouse.Source.ID),
		"workspaceId": job.upload.WorkspaceID,
		"destID":      job.upload.DestinationID,
		"sourceID":    job.upload.SourceID,
	}
	for _, extraTag := range extraTags {
		tags[extraTag.Name] = extraTag.Value
	}
	return tags
}

func (job *UploadJob) timerStat(name string, extraTags ...warehouseutils.Tag) stats.Measurement {
	return job.statsFactory.NewTaggedStat(name, stats.TimerType, job.buildTags(extraTags...))
}

func (job *UploadJob) counterStat(name string, extraTags ...warehouseutils.Tag) stats.Measurement {
	return job.statsFactory.NewTaggedStat(name, stats.CountType, job.buildTags(extraTags...))
}

func (job *UploadJob) guageStat(name string, extraTags ...warehouseutils.Tag) stats.Measurement {
	extraTags = append(extraTags, warehouseutils.Tag{
		Name:  "sourceCategory",
		Value: job.warehouse.Source.SourceDefinition.Category,
	})
	return job.statsFactory.NewTaggedStat(name, stats.GaugeType, job.buildTags(extraTags...))
}

func (job *UploadJob) generateUploadSuccessMetrics() {
	var (
		numUploadedEvents int64
		numStagedEvents   int64
		err               error
	)
	numUploadedEvents, err = job.tableUploadsRepo.TotalExportedEvents(
		job.ctx,
		job.upload.ID,
		[]string{},
	)
	if err != nil {
		job.logger.Warnw("sum of total exported events for upload",
			logfield.UploadJobID, job.upload.ID,
			logfield.SourceID, job.upload.SourceID,
			logfield.DestinationID, job.upload.DestinationID,
			logfield.DestinationType, job.upload.DestinationType,
			logfield.WorkspaceID, job.upload.WorkspaceID,
			logfield.Error, err.Error(),
		)
		return
	}

	numStagedEvents, err = repo.NewStagingFiles(job.dbHandle).TotalEventsForUpload(
		job.ctx,
		job.upload,
	)
	if err != nil {
		job.logger.Warnw("total events for upload",
			logfield.UploadJobID, job.upload.ID,
			logfield.SourceID, job.upload.SourceID,
			logfield.DestinationID, job.upload.DestinationID,
			logfield.DestinationType, job.upload.DestinationType,
			logfield.WorkspaceID, job.upload.WorkspaceID,
			logfield.Error, err.Error(),
		)
		return
	}

	job.stats.totalRowsSynced.Count(int(numUploadedEvents))
	job.stats.numStagedEvents.Count(int(numStagedEvents))
	job.stats.uploadSuccess.Count(1)
}

func (job *UploadJob) generateUploadAbortedMetrics() {
	var (
		numUploadedEvents int64
		numStagedEvents   int64
		err               error
	)
	numUploadedEvents, err = job.tableUploadsRepo.TotalExportedEvents(
		job.ctx,
		job.upload.ID,
		[]string{},
	)
	if err != nil {
		job.logger.Warnw("sum of total exported events for upload",
			logfield.UploadJobID, job.upload.ID,
			logfield.SourceID, job.upload.SourceID,
			logfield.DestinationID, job.upload.DestinationID,
			logfield.DestinationType, job.upload.DestinationType,
			logfield.WorkspaceID, job.upload.WorkspaceID,
			logfield.Error, err.Error(),
		)
		return
	}

	numStagedEvents, err = repo.NewStagingFiles(job.dbHandle).TotalEventsForUpload(
		job.ctx,
		job.upload,
	)
	if err != nil {
		job.logger.Warnw("total events for upload",
			logfield.UploadJobID, job.upload.ID,
			logfield.SourceID, job.upload.SourceID,
			logfield.DestinationID, job.upload.DestinationID,
			logfield.DestinationType, job.upload.DestinationType,
			logfield.WorkspaceID, job.upload.WorkspaceID,
			logfield.Error, err.Error(),
		)
		return
	}

	job.stats.totalRowsSynced.Count(int(numUploadedEvents))
	job.stats.numStagedEvents.Count(int(numStagedEvents))
}

func (job *UploadJob) recordTableLoad(tableName string, numEvents int64) {
	rudderAPISupportedEventTypes := []string{"tracks", "identifies", "pages", "screens", "aliases", "groups"}
	if slices.Contains(rudderAPISupportedEventTypes, strings.ToLower(tableName)) {
		// record total events synced (ignoring additional row synced to the event table for e.g.track call)
		job.counterStat(`event_delivery`, warehouseutils.Tag{
			Name:  "tableName",
			Value: strings.ToLower(tableName),
		}).Count(int(numEvents))
	}

	skipMetricTagForEachEventTable := config.GetBool("Warehouse.skipMetricTagForEachEventTable", false)
	if skipMetricTagForEachEventTable {
		standardTablesToRecordEventsMetric := []string{"tracks", "users", "identifies", "pages", "screens", "aliases", "groups", "rudder_discards"}
		if !slices.Contains(standardTablesToRecordEventsMetric, strings.ToLower(tableName)) {
			// club all event table metric tags under one tag to avoid too many tags
			tableName = "others"
		}
	}

	job.counterStat(`rows_synced`, warehouseutils.Tag{
		Name:  "tableName",
		Value: strings.ToLower(tableName),
	}).Count(int(numEvents))
	// Delay for the oldest event in the batch
	firstEventAt, err := repo.NewStagingFiles(job.dbHandle).FirstEventForUpload(job.ctx, job.upload)
	if err != nil {
		job.logger.Errorf("[WH]: Failed to generate delay metrics: %s, Err: %v", job.warehouse.Identifier, err)
		return
	}

	if !job.upload.Retried {
		conf := job.warehouse.Destination.Config
		syncFrequency := "1440"
		if conf[warehouseutils.SyncFrequency] != nil {
			syncFrequency, _ = conf[warehouseutils.SyncFrequency].(string)
		}
		job.timerStat("event_delivery_time",
			warehouseutils.Tag{Name: "tableName", Value: strings.ToLower(tableName)},
			warehouseutils.Tag{Name: "syncFrequency", Value: syncFrequency},
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
	err = job.dbHandle.QueryRowContext(job.ctx, stmt).Scan(&timeTakenInS)
	if err != nil {
		job.logger.Errorf("[WH]: Failed to generate load file generation time stat: %s, Err: %v", job.warehouse.Identifier, err)
		return
	}

	job.stats.loadFileGenerationTime.SendTiming(timeTakenInS * time.Second)
	return nil
}
