package migrator

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
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
func (importer *Importer) Setup(migrator *Migrator) {
	logger.Infof("[[ %s-Import-Migrator ]] setup for jobsdb", migrator.jobsDB.GetTablePrefix())
	importer.importQueues = make(map[string]chan *jobsdb.MigrationEvent)
	importer.migrator = migrator
	importer.migrator.jobsDB.SetupForImport()
	rruntime.Go(func() {
		importer.readFromCheckPointAndTriggerImport()
	})
}

//importHandler accepts a request from an export node and checkpoints it for readFromCheckPointAndTriggerImport go routine to process each
func (importer *Importer) importHandler(migrationEvent jobsdb.MigrationEvent) {
	logger.Infof("[[ %s-Import-migrator ]] Request received to import %s", importer.migrator.jobsDB.GetTablePrefix(), migrationEvent.FileLocation)
	if migrationEvent.ToNode != "All" {
		if migrationEvent.MigrationType == jobsdb.ExportOp {
			migrationEvent.MigrationType = jobsdb.ImportOp
			migrationEvent.ID = 0
			migrationEvent.Status = jobsdb.PreparedForImport
			migrationEvent.TimeStamp = time.Now()
			migrationEvent.ID = importer.migrator.jobsDB.Checkpoint(&migrationEvent)
		} else {
			logger.Errorf("[[ %s-Import-migrator ]] Wrong migration event received. Only export type events are expected. migrationType: %s, migrationEvent: %v", importer.migrator.jobsDB.GetTablePrefix(), migrationEvent.MigrationType, migrationEvent)
		}
	}
	logger.Debug(" %s-Import-migrator: Ack: %v", importer.migrator.jobsDB.GetTablePrefix(), migrationEvent)
}

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
		checkPoints := importer.migrator.jobsDB.GetCheckpoints(jobsdb.ImportOp, jobsdb.PreparedForImport)
		for _, checkPoint := range checkPoints {
			_, found := importTriggeredCheckpoints[checkPoint.ID]
			if !found {
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
		logger.Infof("[[ %s-Import-migrator ]] Downloading file:%s for import", importer.migrator.jobsDB.GetTablePrefix(), migrationEvent.FileLocation)

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

		err = importer.migrator.fileManager.Download(jsonFile, importer.migrator.fileManager.GetDownloadKeyFromFileLocation(migrationEvent.FileLocation))
		if err != nil {
			panic(err.Error())
		}

		jsonFile.Close()

		rawf, err := os.Open(jsonPath)
		if err != nil {
			panic(err)
		}

		err = importer.readFromFileAndWriteToDB(rawf, migrationEvent)
		if err != nil {
			panic(err)
		}

		rawf.Close()
		os.Remove(jsonPath)
	}
}

func (importer *Importer) readFromFileAndWriteToDB(file *os.File, migrationEvent *jobsdb.MigrationEvent) error {
	logger.Infof("[[ %s-Import-migrator ]] Parsing the file:%s for import and passing it to jobsDb", importer.migrator.jobsDB.GetTablePrefix(), migrationEvent.FileLocation)

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
	logger.Infof("[[ %s-Import-migrator ]] Done importing file %s", importer.migrator.jobsDB.GetTablePrefix(), file.Name())
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

//ImportStatusHandler checks if there are no more prepared_for_import events are left and returns true. This indicates import finish only if all exports are finished.
func (importer *Importer) importStatusHandler() bool {
	return len(importer.migrator.jobsDB.GetCheckpoints(jobsdb.ImportOp, jobsdb.PreparedForImport)) == 0
}
