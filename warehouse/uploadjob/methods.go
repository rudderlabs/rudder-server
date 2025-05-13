package uploadjob

import (
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (job *UploadJob) generateUploadSchema() error {
	return job.schemaHandle.GenerateUploadSchema(job.ctx, job.upload)
}

func (job *UploadJob) createTableUploads() error {
	return job.schemaHandle.CreateTableUploads(job.ctx, job.upload)
}

func (job *UploadJob) generateLoadFiles() error {
	return job.loadfile.GenerateLoadFiles(job.ctx, job.upload, job.stagingFiles)
}

func (job *UploadJob) updateTableUploadsCounts() error {
	return job.schemaHandle.UpdateTableUploadsCounts(job.ctx, job.upload)
}

func (job *UploadJob) createRemoteSchema(whManager manager.Manager) error {
	return job.schemaHandle.CreateRemoteSchema(job.ctx, whManager)
}

func (job *UploadJob) exportData() error {
	return job.schemaHandle.ExportData(job.ctx)
}

func (job *UploadJob) generateUploadAbortedMetrics() {
	job.statsFactory.NewTaggedStat(
		"warehouse.upload_aborted",
		stats.CountType,
		stats.Tags{
			"workspaceId": job.warehouse.WorkspaceID,
			"destID":      job.warehouse.Destination.ID,
		},
	).Count(1)
}

func (job *UploadJob) timerStat(name string, tags ...whutils.Tag) stats.Timer {
	statsTags := make(stats.Tags)
	for _, tag := range tags {
		statsTags[tag.Name] = tag.Value
	}
	return job.statsFactory.NewTaggedStat(name, stats.TimerType, statsTags)
}

func (job *UploadJob) counterStat(name string, tags ...whutils.Tag) stats.Counter {
	statsTags := make(stats.Tags)
	for _, tag := range tags {
		statsTags[tag.Name] = tag.Value
	}
	return job.statsFactory.NewTaggedStat(name, stats.CountType, statsTags)
}

func (job *UploadJob) gaugeStat(name string, tags ...whutils.Tag) stats.Gauge {
	statsTags := make(stats.Tags)
	for _, tag := range tags {
		statsTags[tag.Name] = tag.Value
	}
	return job.statsFactory.NewTaggedStat(name, stats.GaugeType, statsTags)
}
