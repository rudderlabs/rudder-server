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
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

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
			panic(err.Error())
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
