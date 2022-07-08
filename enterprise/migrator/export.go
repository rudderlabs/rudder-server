package migrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/rudderlabs/rudder-server/enterprise/pathfinder"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type uploadWorkerT struct {
	channel chan []*jobsdb.JobT
	nodeID  string
}

type notifyWorkerT struct {
	channel chan jobsdb.MigrationCheckpointT
	nodeID  string
}

// Exporter is a handle to this object used in main.go
type exporterT struct {
	migrator      *MigratorT
	pf            pathfinder.ClusterStateT
	uploadWorkers map[string]*uploadWorkerT
	notifyWorkers map[string]*notifyWorkerT
	statVal       string
	eventStat     stats.RudderStats
	logger        logger.LoggerI
}

var (
	dbReadBatchSize              int
	exportDoneCheckSleepDuration time.Duration
)

const (
	localExportTmpDirName = "/migrator-export/"
)

// Setup sets up exporter with underlying-migrator, pathfinder and initializes uploadWorkers and notifyWorkers
func (exp *exporterT) Setup(migrator *MigratorT, pf pathfinder.ClusterStateT) {
	exp.logger = pkgLogger.Child("export")
	exp.crashRecover()
	exp.migrator = migrator
	exp.statVal = fmt.Sprintf("%s-exporter", exp.migrator.jobsDB.GetTablePrefix())
	exp.eventStat = stats.NewStat("export_events", stats.GaugeType)

	exp.logger.Infof("[[ %s-Export-Migrator ]] setup for jobsdb", migrator.jobsDB.GetTablePrefix())
	exp.pf = pf

	exp.uploadWorkers = make(map[string]*uploadWorkerT)
	exp.notifyWorkers = make(map[string]*notifyWorkerT)

	exp.migrator.jobsDB.SetupForExport()
	rruntime.Go(func() {
		exp.export()
	})
}

// crashRecover performs the tasks need to be done in case of any crash
func (exp *exporterT) crashRecover() {
	// Cleaning up residue files in localExportTmpDirName folder.
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	os.RemoveAll(fmt.Sprint(tmpDirPath + localExportTmpDirName))
}

func (exp *exporterT) waitForExportDone() {
	exp.logger.Infof("[[%s-Export-migrator ]] All jobs have been queried. Waiting for the same to be exported and acknowledged on notification", exp.migrator.jobsDB.GetTablePrefix())
	isExportDone := false
	for ok := true; ok; ok = !isExportDone {
		time.Sleep(exportDoneCheckSleepDuration)
		exportEvents := exp.migrator.jobsDB.GetCheckpoints(jobsdb.ExportOp, jobsdb.Exported)
		isExportDone = (len(exportEvents) == 0) && !exp.migrator.jobsDB.IsMigrating()
	}
}

func (exp *exporterT) preExport() {
	exp.logger.Infof("[[ %s-Export-migrator ]] Pre export", exp.migrator.jobsDB.GetTablePrefix())
	exp.migrator.jobsDB.PreExportCleanup()
}

func (exp *exporterT) export() {
	queryStat := stats.NewTaggedStat("export_main", stats.TimerType, stats.Tags{"migrationType": exp.statVal})
	queryStat.Start()
	defer queryStat.End()

	if exp.isExportDone() {
		return
	}

	exp.preExport()

	rruntime.Go(func() {
		exp.readFromCheckpointAndNotify()
	})
	exp.eventStat.Gauge(1)
	exp.logger.Infof("[[ %s-Export-migrator ]] export loop is starting", exp.migrator.jobsDB.GetTablePrefix())
	for {
		toQuery := dbReadBatchSize

		jobList := exp.migrator.jobsDB.GetNonMigratedAndMarkMigrating(toQuery)
		if len(jobList) == 0 {
			break
		}

		migrateJobsByNode := exp.groupByNode(jobList)

		for nodeID, migrateJobs := range migrateJobsByNode {
			dumpChannel := exp.getDumpChannelForNode(nodeID)
			dumpChannel <- migrateJobs
		}
	}

	exp.waitForExportDone()

	exp.postExport()
}

func (exp *exporterT) groupByNode(jobList []*jobsdb.JobT) map[string][]*jobsdb.JobT {
	queryStat := stats.NewTaggedStat("group_by_node", stats.TimerType, stats.Tags{"migrationType": exp.statVal})
	queryStat.Start()
	defer queryStat.End()
	exp.logger.Infof("[[ %s-Export-migrator ]] Grouping a batch by destination nodes", exp.migrator.jobsDB.GetTablePrefix())
	filteredData := make(map[string][]*jobsdb.JobT)
	for _, job := range jobList {
		userID := exp.migrator.jobsDB.GetUserID(job)
		destNode := exp.pf.GetNodeFromUserID(userID)
		nodeID := destNode.ID
		filteredData[nodeID] = append(filteredData[nodeID], job)
	}
	return filteredData
}

func (exp *exporterT) getDumpChannelForNode(nodeID string) chan []*jobsdb.JobT {
	if _, ok := exp.uploadWorkers[nodeID]; !ok {
		// Setup DumpChannel and corresponding goroutine
		channel := make(chan []*jobsdb.JobT, 1000)
		uploadWorker := &uploadWorkerT{
			channel: channel,
			nodeID:  nodeID,
		}
		exp.uploadWorkers[nodeID] = uploadWorker
		rruntime.Go(func() {
			exp.uploadWorkerProcess(uploadWorker)
		})
	}

	return exp.uploadWorkers[nodeID].channel
}

func (exp *exporterT) uploadWorkerProcess(uploadWorker *uploadWorkerT) {
	destNodeID := uploadWorker.nodeID
	for {
		waitStat := stats.NewTaggedStat("upload_worker_wait_time", stats.TimerType, stats.Tags{"migrationType": exp.statVal})
		waitStat.Start()
		var jobList []*jobsdb.JobT
		{
			jobList = <-uploadWorker.channel
		}
		waitStat.End()

		queryStat := stats.NewTaggedStat("upload_worker_time", stats.TimerType, stats.Tags{"migrationType": exp.statVal})
		queryStat.Start()
		exp.logger.Infof("[[ %s-Export-migrator ]] Received a batch for node:%s to be written to file and upload it", exp.migrator.jobsDB.GetTablePrefix(), destNodeID)

		var statusList []*jobsdb.JobStatusT

		if destNodeID == misc.GetNodeID() {
			for _, job := range jobList {
				statusList = append(statusList, jobsdb.BuildStatus(job, jobsdb.WontMigrate.State))
			}
			exp.migrator.jobsDB.UpdateJobStatus(statusList, []string{}, []jobsdb.ParameterFilterT{})
			queryStat.End()
			continue
		}

		marshalStat := stats.NewTaggedStat("marshalling_time", stats.TimerType, stats.Tags{"migrationType": exp.statVal})
		marshalStat.Start()
		contentSlice := make([][]byte, len(jobList))
		for idx, job := range jobList {
			m, err := json.Marshal(job)
			if err != nil {
				panic("Marshalling error")
			}

			contentSlice[idx] = m
			statusList = append(statusList, jobsdb.BuildStatus(job, jobsdb.Migrated.State))
		}
		marshalStat.End()

		exp.logger.Info(destNodeID, len(jobList))

		content := bytes.Join(contentSlice[:], []byte("\n"))

		exportFileName := fmt.Sprintf(`%s_%s_%s_%d_%d_%d.gz`,
			exp.migrator.jobsDB.GetTablePrefix(),
			misc.GetNodeID(),
			destNodeID,
			jobList[0].JobID,
			jobList[len(jobList)-1].JobID,
			len(jobList))
		exportFilePath := writeContentToFile(content, exportFileName)
		uploadOutput := exp.upload(exportFilePath)
		exp.migrator.jobsDB.UpdateJobStatusAndCheckpoint(statusList, misc.GetNodeID(), destNodeID, int64(len(jobList)), uploadOutput.Location)

		os.Remove(exportFilePath)

		queryStat.End()
	}
}

func writeContentToFile(content []byte, fileName string) string {
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}

	exportFilePath := fmt.Sprintf(`%v%s`, tmpDirPath+localExportTmpDirName, fileName)

	err = os.MkdirAll(filepath.Dir(exportFilePath), os.ModePerm)
	if err != nil {
		panic(err)
	}

	gzWriter, err := misc.CreateGZ(exportFilePath)
	if err != nil {
		panic(err)
	}

	gzWriter.Write(content)
	gzWriter.CloseGZ()

	return exportFilePath
}

func (exp *exporterT) upload(exportFilePath string) filemanager.UploadOutput {
	queryStat := stats.NewTaggedStat("upload_time", stats.TimerType, stats.Tags{"migrationType": exp.statVal})
	queryStat.Start()
	defer queryStat.End()

	file, err := os.Open(exportFilePath)
	if err != nil {
		panic(err)
	}
	var uploadOutput filemanager.UploadOutput

	operation := func() error {
		var uploadError error
		uploadOutput, uploadError = exp.migrator.fileManager.Upload(context.TODO(), file)
		return uploadError
	}

	for {
		err := backoff.RetryNotify(operation, backoff.NewExponentialBackOff(), func(err error, t time.Duration) {
			exp.logger.Errorf("[[ %s-Export-migrator ]] Failed to upload file: %s with error: %s, retrying after %v",
				exp.migrator.jobsDB.GetTablePrefix(),
				file.Name(),
				err.Error(),
				t)
		})
		if err == nil {
			break
		}
		exp.logger.Errorf("[[ %s-Export-migrator ]] Failed to export file to %s", exp.migrator.jobsDB.GetTablePrefix(), uploadOutput.Location)
	}

	err = file.Close()
	if err != nil {
		exp.logger.Errorf("error while closing file : %s, err : %v", file.Name(), err)
	}
	exp.logger.Infof("[[ %s-Export-migrator ]] Uploaded an export file to %s", exp.migrator.jobsDB.GetTablePrefix(), uploadOutput.Location)
	return uploadOutput
}

func (exp *exporterT) readFromCheckpointAndNotify() {
	notifiedCheckpoints := make(map[int64]jobsdb.MigrationCheckpointT)
	for {
		time.Sleep(workLoopSleepDuration)
		checkpoints := exp.migrator.jobsDB.GetCheckpoints(jobsdb.ExportOp, jobsdb.Exported)
		for _, checkpoint := range checkpoints {
			_, found := notifiedCheckpoints[checkpoint.ID]
			if !found {
				notifyChannel := exp.getNotifyChannelForNode(checkpoint.ToNode)
				notifyChannel <- checkpoint
				notifiedCheckpoints[checkpoint.ID] = checkpoint
			}
		}
	}
}

func (exp *exporterT) getNotifyChannelForNode(nodeID string) chan jobsdb.MigrationCheckpointT {
	if _, ok := exp.notifyWorkers[nodeID]; !ok {
		channel := make(chan jobsdb.MigrationCheckpointT, 1000)
		notifyWorker := &notifyWorkerT{
			channel: channel,
			nodeID:  nodeID,
		}
		exp.notifyWorkers[nodeID] = notifyWorker
		rruntime.Go(func() {
			exp.notifyWorkerProcess(notifyWorker)
		})
	}
	return exp.notifyWorkers[nodeID].channel
}

func (exp *exporterT) notifyWorkerProcess(notifyWorker *notifyWorkerT) {
	destURL, err := exp.pf.GetConnectionStringForNodeID(notifyWorker.nodeID)
	if err != nil {
		panic(err)
	}
	for {
		checkpoint := <-notifyWorker.channel
		for {
			_, statusCode, err := misc.MakeRetryablePostRequest(destURL, exp.migrator.getURI(notificationURI), checkpoint)
			if err == nil && statusCode == 200 {
				exp.logger.Infof("[[ %s-Export-migrator ]] Notified destination node %s to download and import file from %s.", exp.migrator.jobsDB.GetTablePrefix(), checkpoint.ToNode, checkpoint.FileLocation)
				checkpoint.Status = jobsdb.Notified
				exp.migrator.jobsDB.Checkpoint(checkpoint)
				break
			}
			exp.logger.Errorf("[[ %s-Export-migrator ]] Failed to Notify: %s, Checkpoint: %+v, Error: %v, status: %d", destURL, checkpoint, err, statusCode)
		}
		exp.eventStat.Gauge(1)
	}
}

func (exp *exporterT) postExport() {
	exp.logger.Infof("[[ %s-Export-migrator ]] postExport", exp.migrator.jobsDB.GetTablePrefix())
	exp.migrator.jobsDB.PostExportCleanup()
	migrationCheckpoint := jobsdb.NewMigrationCheckpoint(jobsdb.ExportOp, misc.GetNodeID(), "All", int64(0), "", jobsdb.Completed, 0)
	exp.migrator.jobsDB.Checkpoint(migrationCheckpoint)
	exp.eventStat.Gauge(1)
}

// isExportDone tells if export has finished exporting everything its tasked to do so.
func (exp *exporterT) isExportDone() bool {
	// Instead of this write a query to get a single checkpoint directly
	migrationStates := exp.migrator.jobsDB.GetCheckpoints(jobsdb.ExportOp, jobsdb.Completed)
	if len(migrationStates) == 1 {
		return true
	} else if len(migrationStates) == 0 {
		return false
	}
	panic("More than 1 completed events found. This should not happen. Go debug")
}

// ExportStatusHandler returns true if export for this jobsdb is finished
func (exp *exporterT) exportStatusHandler() bool {
	return exp.isExportDone()
}
