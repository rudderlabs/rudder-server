package migrator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/pathfinder"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	dbReadBatchSize int
)

func loadConfig() {
	dbReadBatchSize = config.GetInt("Migrator.dbReadBatchSize", 1000)
}

func (migrator *Migrator) isExportDone() {
	migrator.jobsDB.GetCheckpoints(jobsdb.ExportOp)
}

func (migrator *Migrator) export() {
	logger.Infof("Migrator: Export loop is starting")

	if !migrator.jobsDB.ShouldExport() {
		return
	}

	for {
		toQuery := dbReadBatchSize

		jobList := migrator.jobsDB.GetNonMigrated(toQuery)
		if len(jobList) == 0 {
			break
		}

		statusList := migrator.filterAndDump(jobList)
		migrator.jobsDB.UpdateJobStatus(statusList, []string{}, []jobsdb.ParameterFilterT{})
	}

	migrator.postExport()
	migrator.doneLock.Lock()
	defer migrator.doneLock.Unlock()
	migrator.done = true
}

func (migrator *Migrator) filterAndDump(jobList []*jobsdb.JobT) []*jobsdb.JobStatusT {
	logger.Infof("Inside filter and dump")
	m := make(map[pathfinder.NodeMeta][]*jobsdb.JobT)
	for _, job := range jobList {
		userID := migrator.jobsDB.GetUserID(job)

		nodeMeta := migrator.pf.GetNodeFromID(userID)
		m[nodeMeta] = append(m[nodeMeta], job)
	}

	backupPathDirName := "/migrator-export/"
	tmpDirPath, err := misc.CreateTMPDIR()

	var statusList []*jobsdb.JobStatusT
	for nMeta, jobList := range m {
		var jobState string
		var writeToFile bool
		if nMeta.GetNodeID() != misc.GetNodeID() {
			jobState = jobsdb.MigratedState
			writeToFile = true
		} else {
			jobState = jobsdb.WontMigrateState
			writeToFile = false
		}

		if writeToFile {
			path := fmt.Sprintf(`%v%s_%s_%s_%d_%d.gz`, tmpDirPath+backupPathDirName, migrator.jobsDB.GetTablePrefix(), misc.GetNodeID(), nMeta.GetNodeID(), jobList[0].JobID, len(jobList))

			err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
			if err != nil {
				panic(err)
			}

			gzWriter, err := misc.CreateGZ(path)

			contentSlice := make([][]byte, len(jobList))
			for idx, job := range jobList {
				m, err := json.Marshal(job)
				if err != nil {
					logger.Error("Something went wrong in marshalling")
				}

				contentSlice[idx] = m
				statusList = append(statusList, buildStatus(job, jobState))
			}

			logger.Info(nMeta, len(jobList))

			content := bytes.Join(contentSlice[:], []byte("\n"))
			gzWriter.Write(content)

			gzWriter.CloseGZ()
			file, err := os.Open(path)
			if err != nil {
				panic(err)
			}
			migrator.uploadToS3(file, nMeta)
			file.Close()

			os.Remove(path)
		} else {
			for _, job := range jobList {
				statusList = append(statusList, buildStatus(job, jobState))
			}
		}
	}
	return statusList
}

func buildStatus(job *jobsdb.JobT, jobState string) *jobsdb.JobStatusT {
	newStatus := jobsdb.JobStatusT{
		JobID:         job.JobID,
		JobState:      jobState,
		AttemptNum:    1,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		ErrorCode:     "200",
		ErrorResponse: []byte(`{"success":"OK"}`),
	}
	return &newStatus
}

func (migrator *Migrator) uploadToS3(file *os.File, nMeta pathfinder.NodeMeta) {
	uploadOutput, err := migrator.fileManager.Upload(file)
	if err != nil {
		//TODO: Retry
		panic(err.Error())
	} else {
		logger.Infof("Migrator: Uploaded an export file to %s", uploadOutput.Location)
		//TODO: delete this file otherwise in failure case, the file exists and same data will be appended to it
		migrationEvent := jobsdb.NewMigrationEvent("export", misc.GetNodeID(), nMeta.GetNodeID(), uploadOutput.Location, jobsdb.Exported, 0)

		migrationEvent.ID = migrator.jobsDB.Checkpoint(&migrationEvent)
		go migrator.readFromCheckpointAndNotify(migrationEvent, nMeta)
	}
}

func (migrator *Migrator) readFromCheckpointAndNotify(migrationEvent jobsdb.MigrationEvent, nMeta pathfinder.NodeMeta) {
	// migrator.jobsDB.GetCheckpoints()
	logger.Infof("Migrator: Notifying destination node %s to download and import file from %s", migrationEvent.ToNode, migrationEvent.FileLocation)
	go misc.MakeAsyncPostRequest(nMeta.GetNodeConnectionString(migrator.port), migrator.getURI("/fileToImport"), migrationEvent, 5, migrator.postHandler)

}

func (migrator *Migrator) postHandler(retryCount int, response interface{}, endpoint string, uri string, data interface{}) {
	if retryCount == -1 {
		responseMigrationEvent := jobsdb.MigrationEvent{}
		mapstructure.Decode(response, &responseMigrationEvent)
		migrationEvent := jobsdb.MigrationEvent{}
		mapstructure.Decode(data, &migrationEvent)
		migrationEvent.StartSeq = responseMigrationEvent.StartSeq
		migrationEvent.Status = jobsdb.Imported
		migrationEvent.TimeStamp = time.Now()
		migrator.jobsDB.Checkpoint(&migrationEvent)
	} else if retryCount > 0 {
		misc.MakeAsyncPostRequest(endpoint, uri, data, retryCount-1, migrator.postHandler)
	} else if retryCount == 0 {
		//panic("Ran out of retries. Go debug")
	}
}

func (migrator *Migrator) postExport() {
	migrator.jobsDB.PostMigrationCleanup()
	migrationEvent := jobsdb.NewMigrationEvent(jobsdb.ExportOp, misc.GetNodeID(), "All", "", jobsdb.Exported, 0)
	migrator.jobsDB.Checkpoint(&migrationEvent)
}
