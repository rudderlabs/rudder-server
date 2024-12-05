package router

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/utils/misc"
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

func (job *UploadJob) gaugeStat(name string, extraTags ...warehouseutils.Tag) stats.Measurement {
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
		job.logger.Warnw("sum of total exported events for upload", logfield.Error, err.Error())
		return
	}

	numStagedEvents, err = job.stagingFileRepo.TotalEventsForUploadID(
		job.ctx,
		job.upload.ID,
	)
	if err != nil {
		job.logger.Warnw("total events for upload", logfield.Error, err.Error())
		return
	}

	eventTimeRanges, err := job.stagingFileRepo.GetEventTimeRangesByUploadID(job.ctx, job.upload.ID)
	if err != nil {
		job.logger.Warnn("event time ranges for upload", obskit.Error(err))
		return
	}

	for _, eventTimeRange := range eventTimeRanges {
		job.stats.eventDeliveryTime.Since(eventTimeRange.FirstEventAt)
		job.stats.eventDeliveryTime.Since(eventTimeRange.LastEventAt)
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
		job.logger.Warnw("sum of total exported events for upload", logfield.Error, err.Error())
		return
	}

	numStagedEvents, err = job.stagingFileRepo.TotalEventsForUploadID(
		job.ctx,
		job.upload.ID,
	)
	if err != nil {
		job.logger.Warnw("total events for upload", logfield.Error, err.Error())
		return
	}

	job.stats.totalRowsSynced.Count(int(numUploadedEvents))
	job.stats.numStagedEvents.Count(int(numStagedEvents))
}

func (job *UploadJob) recordTableLoad(tableName string, numEvents int64) {
	capturedTableName := warehouseutils.TableNameForStats(tableName)

	job.counterStat(`event_delivery`, warehouseutils.Tag{
		Name:  "tableName",
		Value: capturedTableName,
	}).Count(int(numEvents))

	job.counterStat(`rows_synced`, warehouseutils.Tag{
		Name:  "tableName",
		Value: capturedTableName,
	}).Count(int(numEvents))
}

func (job *UploadJob) recordLoadFileGenerationTimeStat(startID, endID int64) error {
	startLoadFile, err := job.loadFilesRepo.GetByID(job.ctx, startID)
	if err != nil {
		return fmt.Errorf("getting start load file by id %d: %w", startID, err)
	}
	endLoadFile, err := job.loadFilesRepo.GetByID(job.ctx, endID)
	if err != nil {
		return fmt.Errorf("getting end load file by id %d: %w", endID, err)
	}

	job.stats.loadFileGenerationTime.SendTiming(endLoadFile.CreatedAt.Sub(startLoadFile.CreatedAt))
	return nil
}
