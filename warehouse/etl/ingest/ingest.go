package ingest

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/bugsnag/bugsnag-go"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	ci "github.com/rudderlabs/rudder-server/warehouse/clusterinterface"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	warehouseStagingFilesTable string
	warehouseLoadFilesTable    string
	warehouseUploadsTable      string
	stagingFilesBatchSize      int
)

type HandleT struct {
	dbHandle *sql.DB
	config   *ci.ClusterConfig
}

func (ig *HandleT) processHandler(w http.ResponseWriter, r *http.Request) {
	logger.LogRequest(r)

	// body, err := ioutil.ReadAll(r.Body)
	// r.Body.Close()

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var stagingFile warehouseutils.StagingFileT
	json.Unmarshal(body, &stagingFile)

	logger.Debugf("BRT: Creating record for uploaded json in %s table with schema: %+v", warehouseStagingFilesTable, stagingFile.Schema)
	schemaPayload, err := json.Marshal(stagingFile.Schema)
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (location, schema, source_id, destination_id, status, created_at, updated_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $6)`, warehouseStagingFilesTable)
	stmt, err := ig.dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(stagingFile.Location, schemaPayload, stagingFile.BatchDestination.Source.ID, stagingFile.BatchDestination.Destination.ID, warehouseutils.StagingFileWaitingState, time.Now())
	if err != nil {
		panic(err)
	}

	// req := webRequestT{request: r, writer: &w, done: done, reqType: reqType}
	// gateway.webRequestQ <- &req
	// //Wait for batcher process to be done
	// errorMessage := <-done
	// atomic.AddUint64(&gateway.ackCount, 1)
	// if errorMessage != "" {
	// 	logger.Debug(errorMessage)
	// 	http.Error(w, errorMessage, 400)
	// } else {
	// 	logger.Debug(getStatus(Ok))
	// 	w.Write([]byte(getStatus(Ok)))
	// }
}

func (ig *HandleT) setupTables() {
	sqlStatement := `DO $$ BEGIN
                                CREATE TYPE wh_staging_state_type
                                     AS ENUM(
                                              'waiting',
                                              'executing',
											  'failed',
											  'succeeded');
                                     EXCEPTION
                                        WHEN duplicate_object THEN null;
                            END $$;`

	_, err := ig.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
                                      id BIGSERIAL PRIMARY KEY,
									  location TEXT NOT NULL,
									  source_id VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  schema JSONB NOT NULL,
									  error TEXT,
									  status wh_staging_state_type,
									  created_at TIMESTAMP NOT NULL,
									  updated_at TIMESTAMP NOT NULL);`, warehouseStagingFilesTable)

	_, err = ig.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// index on source_id, destination_id combination
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_id_index ON %[1]s (source_id, destination_id);`, warehouseStagingFilesTable)
	_, err = ig.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
}

func init() {
	config.Initialize()
	loadConfig()
}

func loadConfig() {
	//Port where WH is running
	warehouseStagingFilesTable = config.GetString("Warehouse.stagingFilesTable", "wh_staging_files")
	warehouseLoadFilesTable = config.GetString("Warehouse.loadFilesTable", "wh_load_files")
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
	stagingFilesBatchSize = config.GetInt("Warehouse.stagingFilesBatchSize", 240)
	inProgressMap = map[string]bool{}
	inRecoveryMap = map[string]bool{}
	lastExecMap = map[string]int64{}
}

// Start inits ingester service
func (ig *HandleT) Start(dbHandle *sql.DB, config *ci.ClusterConfig) {
	ig.dbHandle = dbHandle
	ig.config = config

	ig.setupTables()

	fmt.Println("**********")
	http.HandleFunc("/v1/process", ig.processHandler)

	backendconfig.WaitForConfig()

	logger.Infof("Starting in %d", 8082)

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(8082), bugsnag.Handler(nil)))
}

//UpdateJobQueue updates job queue from Warehouse.stagingFilesTable
func (ig *HandleT) UpdateJobQueue(jobs []ProcessStagingFilesJobT) {
	for _, job := range jobs {
		for _, sFile := range job.List {
			stmt := fmt.Sprintf(`INSERT INTO %[1]s (staging_file_id ,  status ,worker_id, job_created_at  , status_updated_at )
												 VALUES ( %[2]d,'new','%[3]s','%[4]s','%[4]s')`, ig.config.JobQueueTable, sFile.ID, "", warehouseutils.GetCurrentSQLTimestamp(), warehouseutils.GetCurrentSQLTimestamp())
			_, err := ig.dbHandle.Exec(stmt)
			warehouseutils.AssertError(ig, err)
		}
	}
}
