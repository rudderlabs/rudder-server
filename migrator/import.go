package migrator

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//Importer is a handle to this object used in main.go
type Importer struct {
	migrator     *Migrator
	importQueues map[string]chan *jobsdb.MigrationEvent
}

//Setup sets up importer with underlying-migrator  and initializes importQueues
func (importer *Importer) Setup(jobsDB *jobsdb.HandleT, migratorPort int) {
	importer.importQueues = make(map[string]chan *jobsdb.MigrationEvent)
	importer.migrator = &Migrator{}
	importer.migrator.Setup(jobsDB, migratorPort)
	importer.migrator.jobsDB.SetupForImport()
	rruntime.Go(func() {
		importer.readFromCheckPointAndTriggerImport()
	})
}

func (importer *Importer) importHandler(w http.ResponseWriter, r *http.Request) {
	body, _ := ioutil.ReadAll(r.Body)
	r.Body.Close()
	migrationEvent := jobsdb.MigrationEvent{}
	err := json.Unmarshal(body, &migrationEvent)
	if err != nil {
		panic(err)
	}
	logger.Infof("Import-migrator: Request received to import %s", migrationEvent.FileLocation)
	if migrationEvent.ToNode != "All" {
		if migrationEvent.MigrationType == jobsdb.ExportOp {
			migrationEvent.MigrationType = jobsdb.ImportOp
			migrationEvent.ID = 0
			migrationEvent.Status = jobsdb.PreparedForImport
			migrationEvent.TimeStamp = time.Now()
			migrationEvent.ID = importer.migrator.jobsDB.Checkpoint(&migrationEvent)
		} else {
			logger.Errorf("Import-migrator: Wrong migration event received. Only export type events are expected. migrationType: %s, migrationEvent: %v", migrationEvent.MigrationType, migrationEvent)
		}
	}
	logger.Debug("Import-migrator: Ack: %v", migrationEvent)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

//TODO: Verify: this is similar to getDumpQForNode. Should we write a single function for both. How to do it?
func (importer *Importer) getImportQForNode(nodeID string) (chan *jobsdb.MigrationEvent, bool) {
	isNewChannel := false
	if _, ok := importer.importQueues[nodeID]; !ok {
		notifyQ := make(chan *jobsdb.MigrationEvent)
		importer.importQueues[nodeID] = notifyQ
		isNewChannel = true
	}
	return importer.importQueues[nodeID], isNewChannel
}

func (importer *Importer) readFromCheckPointAndTriggerImport() {
	importTriggeredCheckpoints := make(map[int64]*jobsdb.MigrationEvent)
	for {
		checkPoints := importer.migrator.jobsDB.GetCheckpoints(jobsdb.ImportOp)
		for _, checkPoint := range checkPoints {
			_, found := importTriggeredCheckpoints[checkPoint.ID]
			if checkPoint.Status == jobsdb.PreparedForImport && !found {
				importQ, isNew := importer.getImportQForNode(checkPoint.FromNode)
				if isNew {
					rruntime.Go(func() {
						importer.processImport(importQ)
					})
				}
				importQ <- checkPoint
				importTriggeredCheckpoints[checkPoint.ID] = checkPoint
			}
		}
	}
}

func (importer *Importer) processImport(importQ chan *jobsdb.MigrationEvent) {
	localTmpDirName := "/migrator-import/"
	tmpDirPath, err := misc.CreateTMPDIR()
	for {
		migrationEvent := <-importQ
		logger.Infof("Import-migrator: Downloading file:%s for import", migrationEvent.FileLocation)

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

		err = importer.migrator.fileManager.Download(jsonFile, fileName)
		if err != nil {
			panic(err.Error())
		}

		jsonFile.Close()

		rawf, err := os.Open(jsonPath)
		if err != nil {
			panic(err)
		}

		importer.readFromFileAndWriteToDB(rawf, migrationEvent)
		migrationEvent.Status = jobsdb.Imported
		importer.migrator.jobsDB.Checkpoint(migrationEvent)

		rawf.Close()
		os.Remove(jsonPath)
	}
}

func (importer *Importer) readFromFileAndWriteToDB(file *os.File, migrationEvent *jobsdb.MigrationEvent) error {
	logger.Infof("Import-migrator: Parsing the file:%s for import and passing it to jobsDb", migrationEvent.FileLocation)

	reader, err := gzip.NewReader(file)
	if err != nil {
		panic(err)
	}

	sc := bufio.NewScanner(reader)
	// Scan() reads next line and returns false when reached end or error
	jobList := []*jobsdb.JobT{}

	for sc.Scan() {
		lineBytes := sc.Bytes()
		job, status := importer.processSingleLine(lineBytes)
		if !status {
			return nil
		}
		jobList = append(jobList, &job)
	}
	reader.Close()
	importer.migrator.jobsDB.StoreImportedJobsAndJobStatuses(jobList, file.Name(), migrationEvent)
	logger.Infof("Import-migrator: Done importing file %s", file.Name())
	//TODO: check if Scan() finished because of error or because it reached end of file
	return sc.Err()
}

func (importer *Importer) processSingleLine(line []byte) (jobsdb.JobT, bool) {
	job := jobsdb.JobT{}
	err := json.Unmarshal(line, &job)
	if err != nil {
		logger.Error(err)
		return jobsdb.JobT{}, false
	}
	return job, true
}

func (importer *Importer) importStatusHandler() bool {
	migrationStates := importer.migrator.jobsDB.GetCheckpoints(jobsdb.ImportOp)
	if len(migrationStates) > 1 {
		for _, migrationState := range migrationStates {
			if migrationState.Status == jobsdb.PreparedForImport {
				return false
			}
		}
	}
	return true
}
