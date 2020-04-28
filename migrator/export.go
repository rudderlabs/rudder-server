package migrator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/pathfinder"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	dbReadBatchSize              int
	exportDoneCheckSleepDuration time.Duration
)

func loadConfig() {
	dbReadBatchSize = config.GetInt("Migrator.dbReadBatchSize", 1000)
	exportDoneCheckSleepDuration = (config.GetDuration("Migrator.exportDoneCheckSleepDurationIns", time.Duration(2)) * time.Second)
}

func (migrator *Migrator) waitForExportDone() {
	anyPendingNotifications := true
	for ok := true; ok; ok = anyPendingNotifications {
		time.Sleep(exportDoneCheckSleepDuration)
		exportEvents := migrator.jobsDB.GetCheckpoints(jobsdb.ExportOp)
		anyPendingNotifications = false
		for _, exportEvent := range exportEvents {
			if exportEvent.Status == jobsdb.Exported {
				anyPendingNotifications = true
			}
		}
		//TODO: additionally makesure there are no jobs left in migrating state
	}
}

func (migrator *Migrator) preExport() {
	migrator.jobsDB.PreExportCleanup()
}

func (migrator *Migrator) export() {
	logger.Infof("Migrator: Export loop is starting")

	if !migrator.jobsDB.ShouldExport() {
		return
	}

	migrator.preExport()

	rruntime.Go(func() {
		migrator.readFromCheckpointAndNotify()
	})

	for {
		toQuery := dbReadBatchSize

		jobList := migrator.jobsDB.GetNonMigrated(toQuery)
		if len(jobList) == 0 {
			break
		}

		filteredData := migrator.filterByNode(jobList)
		migrator.delegateDump(filteredData)
	}

	migrator.waitForExportDone()

	migrator.postExport()
	migrator.doneLock.Lock()
	defer migrator.doneLock.Unlock()
	migrator.done = true
}

func (migrator *Migrator) filterByNode(jobList []*jobsdb.JobT) map[pathfinder.NodeMeta][]*jobsdb.JobT {
	logger.Infof("Inside filter and dump")
	filteredData := make(map[pathfinder.NodeMeta][]*jobsdb.JobT)
	for _, job := range jobList {
		userID := migrator.jobsDB.GetUserID(job)
		nodeMeta := migrator.pf.GetNodeFromID(userID)
		filteredData[nodeMeta] = append(filteredData[nodeMeta], job)
	}
	return filteredData
}

func (migrator *Migrator) delegateDump(filteredData map[pathfinder.NodeMeta][]*jobsdb.JobT) {
	for nMeta, jobList := range filteredData {
		dumpQ, isNew := migrator.getDumpQForNode(nMeta.GetNodeID())
		if isNew {
			rruntime.Go(func() {
				migrator.writeToFileAndUpload(nMeta, dumpQ)
			})
		}
		dumpQ <- jobList
	}
}

func (migrator *Migrator) getDumpQForNode(nodeID string) (chan []*jobsdb.JobT, bool) {
	isNewChannel := false
	if _, ok := migrator.dumpQueues[nodeID]; !ok {
		dumpQ := make(chan []*jobsdb.JobT)
		migrator.dumpQueues[nodeID] = dumpQ
		isNewChannel = true
	}
	return migrator.dumpQueues[nodeID], isNewChannel
}

func (migrator *Migrator) writeToFileAndUpload(nMeta pathfinder.NodeMeta, ch chan []*jobsdb.JobT) {
	for {
		jobList := <-ch
		backupPathDirName := "/migrator-export/"
		tmpDirPath, err := misc.CreateTMPDIR()

		var jobState string
		var writeToFile bool
		if nMeta.GetNodeID() != misc.GetNodeID() {
			jobState = jobsdb.MigratedState
			writeToFile = true
		} else {
			jobState = jobsdb.WontMigrateState
			writeToFile = false
		}

		var statusList []*jobsdb.JobStatusT
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
				statusList = append(statusList, jobsdb.BuildStatus(job, jobState))
			}

			logger.Info(nMeta, len(jobList))

			content := bytes.Join(contentSlice[:], []byte("\n"))
			gzWriter.Write(content)

			gzWriter.CloseGZ()
			file, err := os.Open(path)
			if err != nil {
				panic(err)
			}
			migrator.upload(file, nMeta)
			file.Close()

			os.Remove(path)
		} else {
			for _, job := range jobList {
				statusList = append(statusList, jobsdb.BuildStatus(job, jobState))
			}
		}
		migrator.jobsDB.UpdateJobStatus(statusList, []string{}, []jobsdb.ParameterFilterT{})
	}
}

func (migrator *Migrator) upload(file *os.File, nMeta pathfinder.NodeMeta) {
	uploadOutput, err := migrator.fileManager.Upload(file)
	if err != nil {
		//TODO: Retry
		panic(err.Error())
	} else {
		logger.Infof("Migrator: Uploaded an export file to %s", uploadOutput.Location)
		//TODO: delete this file otherwise in failure case, the file exists and same data will be appended to it
		migrationEvent := jobsdb.NewMigrationEvent("export", misc.GetNodeID(), nMeta.GetNodeID(), uploadOutput.Location, jobsdb.Exported, 0)
		migrationEvent.ID = migrator.jobsDB.Checkpoint(&migrationEvent)
	}
}

func (migrator *Migrator) readFromCheckpointAndNotify() {
	for {
		checkPoints := migrator.jobsDB.GetCheckpoints(jobsdb.ExportOp)
		for _, checkPoint := range checkPoints {
			if checkPoint.Status == jobsdb.Exported {
				notifyQ, isNew := migrator.getNotifyQForNode(checkPoint.ToNode)
				if isNew {
					rruntime.Go(func() {
						migrator.notify(migrator.pf.GetNodeFromID(checkPoint.ToNode), notifyQ)
					})
				}
				notifyQ <- checkPoint
			}
		}
	}
}

//TODO: Verify: this is similar to getDumpQForNode. Should we write a single function for both. How to do it?
func (migrator *Migrator) getNotifyQForNode(nodeID string) (chan *jobsdb.MigrationEvent, bool) {
	isNewChannel := false
	if _, ok := migrator.notifyQueues[nodeID]; !ok {
		notifyQ := make(chan *jobsdb.MigrationEvent)
		migrator.notifyQueues[nodeID] = notifyQ
		isNewChannel = true
	}
	return migrator.notifyQueues[nodeID], isNewChannel
}

func (migrator *Migrator) notify(nMeta pathfinder.NodeMeta, notifyQ chan *jobsdb.MigrationEvent) {
	for {
		checkPoint := <-notifyQ
		logger.Infof("Migrator: Notifying destination node %s to download and import file from %s", checkPoint.ToNode, checkPoint.FileLocation)
		statusCode := 0
		for ok := true; ok; ok = (statusCode != 200) {
			_, statusCode = misc.MakePostRequest(nMeta.GetNodeConnectionString(migrator.port), migrator.getURI("/fileToImport"), checkPoint)
			logger.Infof("Migrator: Notified destination node %s to download and import file from %s. Responded with statusCode: %d", checkPoint.ToNode, checkPoint.FileLocation, statusCode)
		}
		checkPoint.Status = jobsdb.Notified
		migrator.jobsDB.Checkpoint(checkPoint)
	}
}

func (migrator *Migrator) postExport() {
	migrator.jobsDB.PostExportCleanup()
	migrationEvent := jobsdb.NewMigrationEvent(jobsdb.ExportOp, misc.GetNodeID(), "All", "", jobsdb.Exported, 0)
	migrator.jobsDB.Checkpoint(&migrationEvent)
}
