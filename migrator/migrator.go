package migrator

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/pathfinder"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//Migrator is a handle to this object used in main.go
type Migrator struct {
	jobsDB *jobsdb.HandleT
	pf     pathfinder.Pathfinder
}

func init() {
	config.Initialize()
	loadConfig()
}

//Setup initializes the module
func (migrator *Migrator) Setup(jobsDB *jobsdb.HandleT, pf pathfinder.Pathfinder, isNew bool) {
	migrator.jobsDB = jobsDB
	migrator.pf = pf
	if pf.DoesNodeBelongToTheCluster(misc.GetNodeID()) {
		migrator.jobsDB.SetupForImportAndAcceptNewEvents(pf.GetVersion(), isNew)
		rruntime.Go(func() {
			migrator.importFromFiles()
		})
	}
	logger.Info("Shanmukh: inside migrator setup")
	migrator.export()
}

var (
	dbReadBatchSize int
)

func loadConfig() {
	dbReadBatchSize = config.GetInt("Migrator.dbReadBatchSize", 100)
}

func (migrator *Migrator) importFromFiles() {

	logger.Info("Shanmukh: import loop starting")
	importFiles := migrator.getImportFiles()

	for _, file := range importFiles {
		migrator.readFromFileAndWriteToDB(file) //Make this concurrent
	}
}

func (migrator *Migrator) readFromFileAndWriteToDB(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	// Scan() reads next line and returns false when reached end or error
	jobList := []*jobsdb.JobT{}
	for scanner.Scan() {
		line := scanner.Text()
		job, status := migrator.processSingleLine(line)
		if !status {
			return nil
		}
		jobList = append(jobList, &job)
		// process the line
	}

	migrator.jobsDB.StoreImportedJobsAndJobStatuses(jobList)
	logger.Info("done : ", file)
	// check if Scan() finished because of error or because it reached end of file
	return scanner.Err()
}

func (migrator *Migrator) processSingleLine(line string) (jobsdb.JobT, bool) {
	job := jobsdb.JobT{}
	err := json.Unmarshal([]byte(line), &job)
	if err != nil {
		logger.Error(err)
		return jobsdb.JobT{}, false
	}
	return job, true
}

func (migrator *Migrator) getImportFiles() []string {
	allFiles := []string{"0_1_0.json", "0_1_1.json", "0_1_2.json", "0_2_0.json", "0_2_1.json", "0_2_2.json", "0_3_0.json", "0_3_1.json", "0_3_2.json"}
	filesToImport := []string{}
	for _, file := range allFiles {
		fileSplits := strings.Split(file, "_")
		if len(fileSplits) != 3 {
			logger.Error("Must panic here")
		}
		if fileSplits[1] == strconv.Itoa(misc.GetNodeID()) {
			filesToImport = append(filesToImport, file)
		}
	}
	return filesToImport
}

func (migrator *Migrator) export() {

	logger.Info("Shanmukh: Migrator loop starting")

	for {
		toQuery := dbReadBatchSize

		jobList := migrator.jobsDB.GetNonMigrated(toQuery)
		if len(jobList) == 0 {
			break
		}
		var statusList []*jobsdb.JobStatusT
		statusList = migrator.filterAndDump(jobList)
		migrator.jobsDB.UpdateJobStatus(statusList, []string{}, []jobsdb.ParameterFilterT{})
	}

	migrator.postExport()
}

func (migrator *Migrator) filterAndDump(jobList []*jobsdb.JobT) []*jobsdb.JobStatusT {
	logger.Info("Shanmukh: inside filterAndMigrateLocal")

	m := make(map[pathfinder.NodeMeta][]*jobsdb.JobT)
	for _, job := range jobList {
		eventList, ok := misc.ParseRudderEventBatch(job.EventPayload)
		if !ok {
			//TODO: This can't be happening. This is done only to get userId/anonId. There should be a more reliable way.
			logger.Debug("This can't be happening. This is done only to get userId/anonId. There should be a more reliable way.")
			continue
		}
		userID, ok := misc.GetAnonymousID(eventList[0])

		nodeMeta := migrator.pf.GetNodeFromID(userID)
		m[nodeMeta] = append(m[nodeMeta], job)
	}

	fileIndex := 0
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
			fileName := fmt.Sprintf("%d_%d_%d.json", misc.GetNodeID(), nMeta.GetNodeID(), fileIndex)
			file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

			if err != nil {
				log.Fatalf("failed creating file: %s", err)
			}

			datawriter := bufio.NewWriter(file)

			for _, job := range jobList {
				m, err := json.Marshal(job)
				if err != nil {
					logger.Error("Something went wrong in marshalling")
				}
				_, _ = datawriter.WriteString(string(m) + "\n")
				buildStatusList(statusList, job, jobState)
			}
			logger.Info(nMeta, len(jobList))
			datawriter.Flush()
			file.Close()
			migrator.uploadToS3AndNotifyDestNode(fileName, nMeta)
			fileIndex++
		} else {
			for _, job := range jobList {
				buildStatusList(statusList, job, jobState)
			}
		}
	}
	return statusList
}

func buildStatusList(statusList []*jobsdb.JobStatusT, job *jobsdb.JobT, jobState string) {
	newStatus := jobsdb.JobStatusT{
		JobID:         job.JobID,
		JobState:      jobState,
		AttemptNum:    1,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		ErrorCode:     "200",
		ErrorResponse: []byte(`{"success":"OK"}`),
	}
	statusList = append(statusList, &newStatus)
}

func (migrator *Migrator) uploadToS3AndNotifyDestNode(fileName string, nMeta pathfinder.NodeMeta) {
	//TODO:
}

func (migrator *Migrator) postExport() {
	migrator.jobsDB.PostMigrationCleanup()
}
