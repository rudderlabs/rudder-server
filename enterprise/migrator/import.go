package migrator

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// Importer is a handle to this object used in main.go
type importerT struct {
	migrator           *MigratorT
	importWorkers      map[string]*importWorkerT
	statVal            string
	eventStat          stats.RudderStats
	nextAvailableJobID int64
	globalImportWorker *importWorkerT
	logger             logger.LoggerI
}

type importWorkerT struct {
	channel chan jobsdb.MigrationCheckpointT
	nodeID  string
}

const (
	localImportTmpDirName = "/migrator-import/"
)

// Setup sets up importer with underlying-migrator  and initializes importQueues
func (imp *importerT) Setup(migrator *MigratorT) {
	imp.logger = pkgLogger.Child("import")
	imp.crashRecover()
	imp.migrator = migrator
	imp.statVal = fmt.Sprintf("%s-importer", imp.migrator.jobsDB.GetTablePrefix())
	imp.eventStat = stats.NewStat("import_events", stats.GaugeType)
	imp.logger.Infof("[[ %s-Import-Migrator ]] setup for jobsdb", migrator.jobsDB.GetTablePrefix())
	imp.importWorkers = make(map[string]*importWorkerT)
	imp.migrator.jobsDB.SetupForImport()
	imp.nextAvailableJobID = imp.getNextAvailableJobIDForImport()

	channel := make(chan jobsdb.MigrationCheckpointT)

	imp.globalImportWorker = &importWorkerT{
		channel: channel,
		nodeID:  "generic-node-id",
	}
	rruntime.Go(func() {
		imp.importProcess(imp.globalImportWorker)
	})

	rruntime.Go(func() {
		imp.readFromCheckpointAndTriggerImport()
	})
}

func (imp *importerT) getNextAvailableJobIDForImport() int64 {
	checkpoints := imp.migrator.jobsDB.GetCheckpoints(jobsdb.ImportOp, "")
	lastCheckpointIdxWithStartSeq := -1
	for idx, checkpoint := range checkpoints {
		if checkpoint.StartSeq == 0 {
			lastCheckpointIdxWithStartSeq = idx - 1
			break
		}
	}

	if lastCheckpointIdxWithStartSeq == -1 {
		return imp.migrator.jobsDB.GetLastJobIDBeforeImport() + 1
	}

	return checkpoints[lastCheckpointIdxWithStartSeq].StartSeq + checkpoints[lastCheckpointIdxWithStartSeq].JobsCount
}

// crashRecover performs the tasks need to be done in case of any crash
func (imp *importerT) crashRecover() {
	// Cleaning up residue files in `localImportTmpDirName` folder.
	tmpDirPath, _ := misc.CreateTMPDIR()
	os.RemoveAll(tmpDirPath + localImportTmpDirName)
}

// importHandler accepts a request from an export node and checkpoints it for readFromCheckpointAndTriggerImport go routine to process each
func (imp *importerT) importHandler(migrationCheckpoint jobsdb.MigrationCheckpointT) error {
	queryStat := stats.NewTaggedStat("import_notify_latency", stats.TimerType, stats.Tags{"migrationType": imp.statVal})
	queryStat.Start()
	defer queryStat.End()

	imp.logger.Infof("[[ %s-Import-migrator ]] Request received to import %s", imp.migrator.jobsDB.GetTablePrefix(), migrationCheckpoint.FileLocation)
	if migrationCheckpoint.MigrationType != jobsdb.ExportOp {
		errorString := fmt.Sprintf("[[ %s-Import-migrator ]] Wrong migration event received. Only export type events are expected. migrationType: %s, migrationCheckpoint: %v", imp.migrator.jobsDB.GetTablePrefix(), migrationCheckpoint.MigrationType, migrationCheckpoint)
		imp.logger.Error(errorString)
		return errors.New(errorString)
	}

	if migrationCheckpoint.ToNode != misc.GetNodeID() {
		errorString := fmt.Sprintf("[[ %s-Import-migrator ]] Wrong migration event received. Supposed to be sent to %s. Received by %s instead, migrationCheckpoint: %v", imp.migrator.jobsDB.GetTablePrefix(), migrationCheckpoint.ToNode, misc.GetNodeID(), migrationCheckpoint)
		imp.logger.Error(errorString)
		return errors.New(errorString)
	}

	migrationCheckpoint.MigrationType = jobsdb.ImportOp
	migrationCheckpoint.ID = 0
	migrationCheckpoint.Status = jobsdb.PreparedForImport
	migrationCheckpoint.TimeStamp = time.Now()
	migrationCheckpoint.ID = imp.migrator.jobsDB.Checkpoint(migrationCheckpoint)
	imp.eventStat.Gauge(2)
	imp.logger.Debug(" %s-Import-migrator: Ack: %v", imp.migrator.jobsDB.GetTablePrefix(), migrationCheckpoint)
	return nil
}

func (imp *importerT) getWorkerChannelForNode(nodeID string) chan jobsdb.MigrationCheckpointT {
	return imp.globalImportWorker.channel
}

func (imp *importerT) readFromCheckpointAndTriggerImport() {
	importTriggeredCheckpoints := make(map[int64]jobsdb.MigrationCheckpointT)
	for {
		time.Sleep(workLoopSleepDuration)
		checkpoints := imp.migrator.jobsDB.GetCheckpoints(jobsdb.ImportOp, jobsdb.PreparedForImport)
		for idx, checkpoint := range checkpoints {
			if checkpoint.StartSeq == 0 {
				var possibleStartSeq, startSeq int64
				if idx > 0 {
					possibleStartSeq = checkpoints[idx-1].StartSeq + checkpoints[idx-1].JobsCount
				}
				if possibleStartSeq < imp.nextAvailableJobID {
					startSeq = imp.nextAvailableJobID
				} else {
					startSeq = possibleStartSeq
				}
				checkpoint.StartSeq = startSeq

				imp.migrator.jobsDB.Checkpoint(checkpoint)
				imp.nextAvailableJobID = startSeq + checkpoint.JobsCount
			}
			_, found := importTriggeredCheckpoints[checkpoint.ID]
			if !found {
				workerChannel := imp.getWorkerChannelForNode(checkpoint.FromNode)
				workerChannel <- checkpoint
				importTriggeredCheckpoints[checkpoint.ID] = checkpoint
			}
		}
	}
}

func (imp *importerT) importProcess(importWorker *importWorkerT) {
	for {
		waitStat := stats.NewTaggedStat("import_worker_process_wait_time", stats.TimerType, stats.Tags{"migrationType": imp.statVal})
		waitStat.Start()
		var migrationCheckpoint jobsdb.MigrationCheckpointT
		{
			migrationCheckpoint = <-importWorker.channel
		}
		waitStat.End()

		queryStat := stats.NewTaggedStat("import_worker_process_time", stats.TimerType, stats.Tags{"migrationType": imp.statVal})
		queryStat.Start()
		imp.logger.Infof("[[ %s-Import-migrator ]] Downloading file:%s for import", imp.migrator.jobsDB.GetTablePrefix(), migrationCheckpoint.FileLocation)

		jsonPath := imp.download(migrationCheckpoint.FileLocation)

		jobList, err := imp.getJobsFromFile(jsonPath)
		if err != nil {
			panic(err)
		}
		imp.migrator.jobsDB.StoreJobsAndCheckpoint(jobList, migrationCheckpoint)
		imp.logger.Infof("[[ %s-Import-migrator ]] Done importing file %s", imp.migrator.jobsDB.GetTablePrefix(), jsonPath)

		os.Remove(jsonPath)
		queryStat.End()
	}
}

func (imp *importerT) download(fileLocation string) string {
	queryStat := stats.NewTaggedStat("download_time", stats.TimerType, stats.Tags{"migrationType": imp.statVal})
	queryStat.Start()
	defer queryStat.End()

	tmpDirPath, err := misc.CreateTMPDIR()

	filePathSlice := strings.Split(fileLocation, "/")
	fileName := filePathSlice[len(filePathSlice)-1]
	jsonPath := fmt.Sprintf("%v%v.json", tmpDirPath+localImportTmpDirName, fileName)

	err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	jsonFile, err := os.Create(jsonPath)
	if err != nil {
		panic(err)
	}

	operation := func() error {
		var downloadError error
		downloadError = imp.migrator.fileManager.Download(context.TODO(), jsonFile, imp.migrator.fileManager.GetDownloadKeyFromFileLocation(fileLocation))
		return downloadError
	}

	for {
		err := backoff.Retry(operation, backoff.NewExponentialBackOff())
		if err == nil {
			break
		}
		imp.logger.Errorf("[[ %s-Import-migrator ]] Failed to download file %s", imp.migrator.jobsDB.GetTablePrefix(), fileLocation)
	}

	jsonFile.Close()
	imp.logger.Infof("[[ %s-Import-migrator ]] Downloaded an import file %s", imp.migrator.jobsDB.GetTablePrefix(), fileLocation)

	return jsonPath
}

func (imp *importerT) getJobsFromFile(jsonPath string) ([]*jobsdb.JobT, error) {
	var jobList []*jobsdb.JobT
	file, err := os.Open(jsonPath)
	if err != nil {
		return jobList, err
	}

	imp.logger.Infof("[[ %s-Import-migrator ]] Parsing the file:%s for import and passing it to jobsDb", imp.migrator.jobsDB.GetTablePrefix(), jsonPath)

	reader, err := gzip.NewReader(file)
	if err != nil {
		return jobList, err
	}

	sc := bufio.NewScanner(reader)
	// Scan() reads next line and returns false when reached end or error
	// default scanner buffer maxCapacity is 64K
	// set it to higher value to avoid read stop on read size error
	maxCapacity := 1024 * 1024
	buf := make([]byte, maxCapacity)
	sc.Buffer(buf, maxCapacity)

	unMarshalStat := stats.NewTaggedStat("unmarshalling_time", stats.TimerType, stats.Tags{"migrationType": imp.statVal})
	unMarshalStat.Start()
	for sc.Scan() {
		lineBytes := sc.Bytes()
		job, err := imp.processSingleLine(lineBytes)
		if err != nil {
			return jobList, err
		}
		jobList = append(jobList, &job)
	}
	unMarshalStat.End()
	reader.Close()
	file.Close()
	return jobList, sc.Err()
}

func (imp *importerT) processSingleLine(line []byte) (jobsdb.JobT, error) {
	job := jobsdb.JobT{}
	err := json.Unmarshal(line, &job)
	if err != nil {
		return jobsdb.JobT{}, err
	}
	return job, nil
}

// ImportStatusHandler checks if there are no more prepared_for_import events are left and returns true. This indicates import finish only if all exports are finished.
func (imp *importerT) importStatusHandler() bool {
	checkpointsPreparedForImport := imp.migrator.jobsDB.GetCheckpoints(jobsdb.ImportOp, jobsdb.PreparedForImport)
	imp.eventStat.Gauge(2)
	return len(checkpointsPreparedForImport) == 0
}
