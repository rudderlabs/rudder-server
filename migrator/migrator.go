package migrator

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
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
	port        int
}

func init() {
	config.Initialize()
	loadConfig()
}

//Setup initializes the module
func (migrator *Migrator) Setup(jobsDB *jobsdb.HandleT, pf pathfinder.Pathfinder, port int) {
	logger.Infof("Migrator: Setting up migrator for %s jobsdb", jobsDB.GetTablePrefix())
	migrator.jobsDB = jobsDB
	migrator.pf = pf
	migrator.fileManager = migrator.setupFileManager()
	migrator.port = port

	migrator.jobsDB.SetupCheckpointTable()

	migrator.jobsDB.SetupForMigrateAndAcceptNewEvents(pf.GetVersion())

	go migrator.startWebHandler()
	// MigrationEvent
	migrator.export()
	migrationEvent := jobsdb.NewMigrationEvent(jobsdb.ExportOp, misc.GetNodeID(), "All", "", jobsdb.Exported, 0)
	migrator.jobsDB.Checkpoint(&migrationEvent)
}

func (migrator *Migrator) importHandler(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)
	r.Body.Close()
	migrationEvent := jobsdb.MigrationEvent{}
	err := json.Unmarshal(body, &migrationEvent)
	if err != nil {
		panic(err)
	}
	logger.Infof("Migrator: Request received to import %s", migrationEvent.FileLocation)
	if migrationEvent.MigrationType == jobsdb.ExportOp {
		localTmpDirName := "/migrator-import/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			panic(err)
		}

		filePathSlice := strings.Split(migrationEvent.FileLocation, "/")
		fileName := filePathSlice[len(filePathSlice)-1]
		jsonPath := fmt.Sprintf("%v%v.json", tmpDirPath+localTmpDirName, fileName)

		err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
		if err != nil {
			panic(err)
		}
		jsonFile, err := os.Create(jsonPath)
		if err != nil {
			panic(err)
		}

		err = migrator.fileManager.Download(jsonFile, fileName)
		if err != nil {
			panic("Unable to download file")
		}

		jsonFile.Close()

		rawf, err := os.Open(jsonPath)
		if err != nil {
			panic(err)
		}

		migrationEvent.MigrationType = jobsdb.ImportOp
		migrationEvent.ID = 0
		migrationEvent.Status = jobsdb.PreparedForImport
		migrationEvent.TimeStamp = time.Now()
		migrationEvent.ID = migrator.jobsDB.Checkpoint(&migrationEvent)
		migrator.readFromFileAndWriteToDB(rawf, migrationEvent)
		logger.Debug("Import done")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))

		rawf.Close()
		os.Remove(jsonPath)
	}
}

func (migrator *Migrator) statusHandler(w http.ResponseWriter, r *http.Request) {
	// migrationEvents := migrator.jobsDB.GetCheckpoints()

	// w.WriteHeader(http.StatusOK)
	// resp, err := json.Marshal(migrationEvents)
	// if err != nil {
	// 	panic("Unable to Marshal")
	// }
	// w.Write(resp)
}

func reflectOrigin(origin string) bool {
	return true
}

func (migrator *Migrator) getURI(uri string) string {
	return fmt.Sprintf("/%s%s", migrator.jobsDB.GetTablePrefix(), uri)
}

func (migrator *Migrator) startWebHandler() {
	logger.Infof("Migrator: Starting migrationWebHandler on port %d", migrator.port)

	http.HandleFunc(migrator.getURI("/fileToImport"), migrator.importHandler)
	http.HandleFunc(migrator.getURI("/status"), migrator.statusHandler)
	c := cors.New(cors.Options{
		AllowOriginFunc:  reflectOrigin,
		AllowCredentials: true,
		AllowedHeaders:   []string{"*"},
	})

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(migrator.port), c.Handler(bugsnag.Handler(nil))))
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

	reader, err := gzip.NewReader(file)
	if err != nil {
		panic(err)
	}

	sc := bufio.NewScanner(reader)
	// Scan() reads next line and returns false when reached end or error
	jobList := []*jobsdb.JobT{}

	for sc.Scan() {
		lineBytes := sc.Bytes()
		job, status := migrator.processSingleLine(lineBytes)
		if !status {
			return nil
		}
		jobList = append(jobList, &job)
		// process the line
	}
	reader.Close()
	migrator.jobsDB.StoreImportedJobsAndJobStatuses(jobList, file.Name(), migrationEvent)
	logger.Infof("Migrator: Done importing file %s", file.Name())
	// check if Scan() finished because of error or because it reached end of file
	return sc.Err()
}

func (migrator *Migrator) processSingleLine(line []byte) (jobsdb.JobT, bool) {
	job := jobsdb.JobT{}
	err := json.Unmarshal(line, &job)
	if err != nil {
		logger.Error(err)
		return jobsdb.JobT{}, false
	}
	return job, true
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
			migrationEvent := jobsdb.NewMigrationEvent(jobsdb.ExportOp, misc.GetNodeID(), "All", "", jobsdb.Exported, 0)
			migrator.jobsDB.Checkpoint(&migrationEvent)
			break
		}

		statusList := migrator.filterAndDump(jobList)
		migrator.jobsDB.UpdateJobStatus(statusList, []string{}, []jobsdb.ParameterFilterT{})
	}

	migrator.postExport()
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
}
