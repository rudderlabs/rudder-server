package migrator

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bugsnag/bugsnag-go"
	"github.com/mitchellh/mapstructure"
	"github.com/rs/cors"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/pathfinder"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//Migrator is a handle to this object used in main.go
type Migrator struct {
	jobsDB      *jobsdb.HandleT
	pf          pathfinder.Pathfinder
	fileManager filemanager.FileManager
}

func init() {
	config.Initialize()
	loadConfig()
}

//Setup initializes the module
func (migrator *Migrator) Setup(jobsDB *jobsdb.HandleT, pf pathfinder.Pathfinder, port int) {
	logger.Info("Shanmukh: inside migrator setup")
	migrator.jobsDB = jobsDB
	migrator.pf = pf
	migrator.fileManager = migrator.setupFileManager()

	migrator.jobsDB.SetupCheckpointDBTable()

	if pf.DoesNodeBelongToTheCluster(misc.GetNodeID()) {
		migrator.jobsDB.SetupForImportAndAcceptNewEvents(pf.GetVersion())
	}

	go migrator.startWebHandler(port)
	migrator.export()
	//panic only if node doesn't belong to cluster
	if !pf.DoesNodeBelongToTheCluster(misc.GetNodeID()) {
		panic(fmt.Sprintf("Node not in cluster. Won't be accepting any more events %v", pf))
	}
}

func (migrator *Migrator) importHandler(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)
	r.Body.Close()
	migrationEvent := jobsdb.MigrationEvent{}
	json.Unmarshal(body, &migrationEvent)
	if migrationEvent.MigrationType == jobsdb.ExportOp {
		filePathSlice := strings.Split(migrationEvent.FileLocation, "/")
		fileName := filePathSlice[len(filePathSlice)-1]
		file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			panic("Handle this")
		}
		err = migrator.fileManager.Download(file, fileName)
		if err != nil {
			panic("Unable to download file")
		}
		migrationEvent.MigrationType = jobsdb.ImportOp
		migrationEvent.ID = 0
		migrationEvent.Status = jobsdb.Prepared
		migrationEvent.TimeStamp = time.Now()
		migrationEvent.ID = migrator.jobsDB.Checkpoint(&migrationEvent)
		migrator.readFromFileAndWriteToDB(file, migrationEvent)
		logger.Debug("Import done")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}
}

func (migrator *Migrator) statusHandler(w http.ResponseWriter, r *http.Request) {

}

func reflectOrigin(origin string) bool {
	return true
}

func (migrator *Migrator) startWebHandler(port int) {
	migratorPort := port
	logger.Infof("Starting in %d", migratorPort)

	http.HandleFunc("/fileToImport", migrator.importHandler)
	http.HandleFunc("/status", migrator.statusHandler)
	c := cors.New(cors.Options{
		AllowOriginFunc:  reflectOrigin,
		AllowCredentials: true,
		AllowedHeaders:   []string{"*"},
	})

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(migratorPort), c.Handler(bugsnag.Handler(nil))))
}

func (migrator *Migrator) setupFileManager() filemanager.FileManager {
	conf := map[string]interface{}{}
	conf["bucketName"] = config.GetString("migratorBucket", "1-2-migrations")
	conf["accessKeyID"] = config.GetString("accessKeyID", "")
	conf["accessKey"] = config.GetString("accessKey", "")
	settings := filemanager.SettingsT{"S3", conf}
	fm, err := filemanager.New(&settings)
	// _ = err
	// return fm
	if err == nil {
		return fm
	}
	panic("Unable to get filemanager")
}

var (
	dbReadBatchSize int
)

func loadConfig() {
	dbReadBatchSize = config.GetInt("Migrator.dbReadBatchSize", 1000)
}

func (migrator *Migrator) readFromFileAndWriteToDB(file *os.File, migrationEvent jobsdb.MigrationEvent) error {
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
	migrator.jobsDB.StoreImportedJobsAndJobStatuses(jobList, file.Name(), migrationEvent)
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

func (migrator *Migrator) export() {

	logger.Info("Shanmukh: Migrator loop starting")
	lastDSIndex := migrator.jobsDB.GetLatestDSIndex()
	for {
		toQuery := dbReadBatchSize

		jobList := migrator.jobsDB.GetNonMigrated(toQuery, lastDSIndex)
		if len(jobList) == 0 {
			break
		}

		statusList := migrator.filterAndDump(jobList)
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
			fileName := fmt.Sprintf("%s_%s_%s_%d_%d.json", migrator.jobsDB.GetTablePrefix(), misc.GetNodeID(), nMeta.GetNodeID(), jobList[0].JobID, len(jobList))
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
				statusList = append(statusList, buildStatus(job, jobState))
			}
			logger.Info(nMeta, len(jobList))
			datawriter.Flush()
			file.Close()
			file, err = os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			migrator.uploadToS3AndNotifyDestNode(file, nMeta)
			file.Close()
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

func (migrator *Migrator) uploadToS3AndNotifyDestNode(file *os.File, nMeta pathfinder.NodeMeta) {
	uploadOutput, err := migrator.fileManager.Upload(file)
	if err != nil {
		panic(uploadOutput)
	} else {
		migrationEvent := jobsdb.NewMigrationEvent("export", misc.GetNodeID(), nMeta.GetNodeID(), uploadOutput.Location, jobsdb.Exported, 0)

		migrationEvent.ID = migrator.jobsDB.Checkpoint(&migrationEvent)

		go misc.MakeAsyncPostRequest(nMeta.GetNodeConnectionString(), "/fileToImport", migrationEvent, 5, migrator.postHandler)
	}
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
	} else if retryCount > 1 {
		misc.MakeAsyncPostRequest(endpoint, uri, data, retryCount-1, migrator.postHandler)
	} else if retryCount == 1 {
		panic("Ran out of retries. Go debug")
	}
}

func (migrator *Migrator) postExport() {
	migrator.jobsDB.PostMigrationCleanup()
}
