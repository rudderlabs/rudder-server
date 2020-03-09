package clusterman

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	ci "github.com/rudderlabs/rudder-server/warehouse/clusterinterface"
	"github.com/rudderlabs/rudder-server/warehouse/etl/ingest"
	whStrings "github.com/rudderlabs/rudder-server/warehouse/strings"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	configSubscriberLock sync.RWMutex
	inProgressMap        map[string]bool
	inRecoveryMap        map[string]bool
	inProgressMapLock    sync.RWMutex
	lastExecMap          map[string]int64
	lastExecMapLock      sync.RWMutex
)

//MasterNodeT will be used on warehouse master
type MasterNodeT struct {
	bc *baseComponentT
	sn *SlaveNodeT //A Master Node is also a Worker

	ingester      *ingest.HandleT
	warehouses    []warehouseutils.WarehouseT
	etlInProgress bool // will be set if the etl batch is in progress

	isSetup bool
}

//Setup to initialise
func (mn *MasterNodeT) Setup(dbHandle *sql.DB, config *ci.ClusterConfig) {

	if mn.isSetup {
		warehouseutils.AssertString(mn, whStrings.STR_WH_MASTER_ALREADY_SETUP)
	}
	defer func() { mn.isSetup = true }()

	mn.setupTables(dbHandle, config)

	//Also init a slave because master is  also a slave
	mn.sn = &SlaveNodeT{}
	mn.sn.Setup(dbHandle, config)

	//Slave will take care of this
	/*mn.bc = &baseComponentT{}
	mn.bc.Setup(mn, dbHandle, config)*/
	mn.bc = mn.sn.bc
	mn.sn.mn = mn

	//start ingester
	rruntime.Go(func() {
		mn.ingester = &ingest.HandleT{}
		mn.ingester.Start(dbHandle, config)
	})

	//subscribe to updates from backend config
	rruntime.Go(func() {
		mn.backendConfigSubscriber()
	})

	//Main Loop  - Master Node  periodically polls job queue
	// & force updates a few jobs so that notifications are regenerated
	// rruntime.Go(func() {
	// })
	mn.masterLoop()
}

//TearDown to release resources
func (mn *MasterNodeT) TearDown() {
	mn.sn.TearDown()
	mn.bc.TearDown()
}

// const ETLBATCHTIME = 2 * time.Second

// var nextETLBatchTime = ETLBATCHTIME

func (mn *MasterNodeT) masterLoop() {

	for {
		select {
		case <-time.After(5 * time.Second):
			//TODO: find proper way to find when etl batch is stopped
			//if mn.etlInProgress {
			mn.updatePendingJobs()
			//}
		/*case <-time.After(nextETLBatchTime):
		if !mn.etlInProgress {
			nextETLBatchTime = ETLBATCHTIME
			mn.beginETLbatch()
		} else {
			nextETLBatchTime = 10 * time.Second
		}*/
		case <-time.After(2 * time.Second): // Run warehouse logic every period
			mn.beginETLbatch()
		}
	}
}

func (mn *MasterNodeT) getBaseComponent() *baseComponentT {
	return mn.bc
}

func (mn *MasterNodeT) isEtlInProgress() bool {
	return mn.etlInProgress
}

//Periodic ETL batch begin
func (mn *MasterNodeT) beginETLbatch() {
	mn.etlInProgress = true
	logger.Infof("WH-JQ: ETL Batch Begin")
	mn.ingester.BeginETLbatch(mn.warehouses)
}

//ETL batch has ended
func (mn *MasterNodeT) didEndETLbatch() {
	mn.etlInProgress = false
	logger.Infof("WH-JQ: ETL Batch End")
}

//force update a few jobs so that notifications are generated for workers
func (mn *MasterNodeT) updatePendingJobs() {

	//Check if jobs to be handled are finished
	row := mn.bc.dbHandle.QueryRow(
		fmt.Sprintf(`SELECT id FROM %[1]s WHERE status = 'new' LIMIT 1;`, mn.bc.config.JobQueueTable))
	var id int
	err := row.Scan(&id)
	if err == sql.ErrNoRows {
		mn.didEndETLbatch()
		return
	}
	warehouseutils.AssertError(mn, err)

	//Get number of workers available
	row = mn.bc.dbHandle.QueryRow(
		fmt.Sprintf(`SELECT count(*) AS wc FROM %[1]s WHERE updated_at < '%[2]s' LIMIT 1;`,
			mn.bc.config.WorkerInfoTable,
			warehouseutils.GetSQLTimestamp(time.Now().Add(-1*time.Minute))))
	var workerCount int
	err = row.Scan(&workerCount)
	warehouseutils.AssertError(mn, err)
	if workerCount < 1 {
		workerCount = 1
	}

	logger.Infof("WH-JQ: Notifying pending jobs after the end of poll time period")
	//notify as many jobs as number of workers available
	_, err = mn.bc.dbHandle.Exec(
		fmt.Sprintf(`UPDATE %[1]s SET status='new',
							status_updated_at = '%[2]s'
							WHERE id IN  (
							SELECT id
							FROM %[1]s
							WHERE status='new' AND status_updated_at < '%[3]s'
							ORDER BY id
							FOR UPDATE SKIP LOCKED
							LIMIT %[4]v
							);`,
			mn.bc.config.JobQueueTable,
			warehouseutils.GetCurrentSQLTimestamp(),
			warehouseutils.GetSQLTimestamp(time.Now().Add(-5*time.Second)),
			workerCount))
	warehouseutils.AssertError(mn, err)
}

//Create the required tables
func (mn *MasterNodeT) setupTables(dbHandle *sql.DB, config *ci.ClusterConfig) {
	logger.Infof("WH-JQ: Creating Job Queue Tables ")

	//create status type
	sqlStmt := `DO $$ BEGIN
						CREATE TYPE wh_job_queue_status_type
							AS ENUM(
								'new',
								'running',
								'success',
								'error'
									);
							EXCEPTION
								WHEN duplicate_object THEN null;
					END $$;`

	_, err := dbHandle.Exec(sqlStmt)
	warehouseutils.AssertError(mn, err)

	//create the job queue table
	sqlStmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
										  id BIGSERIAL PRIMARY KEY,
										  staging_file_id BIGINT,
										  status wh_job_queue_status_type NOT NULL,
										  worker_id VARCHAR(64) NOT NULL,
										  error_count INT DEFAULT 0,
										  job_created_at TIMESTAMP NOT NULL,
										  status_updated_at TIMESTAMP NOT NULL,
										  last_error VARCHAR(512));`, config.JobQueueTable)

	_, err = dbHandle.Exec(sqlStmt)
	warehouseutils.AssertError(mn, err)

	// create index on status
	sqlStmt = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_status_idx ON %[1]s (status);`, config.JobQueueTable)
	_, err = dbHandle.Exec(sqlStmt)
	warehouseutils.AssertError(mn, err)

	//create status type for worker
	sqlStmt = `DO $$ BEGIN
						CREATE TYPE wh_worker_status
							AS ENUM(
								'busy',
								'free' );
							EXCEPTION
								WHEN duplicate_object THEN null;
					END $$;`

	_, err = dbHandle.Exec(sqlStmt)
	warehouseutils.AssertError(mn, err)

	//create the worker info table
	sqlStmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			worker_id VARCHAR(64)  NOT NULL UNIQUE,
			status wh_worker_status NOT NULL,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL);`, config.WorkerInfoTable)
	_, err = dbHandle.Exec(sqlStmt)
	warehouseutils.AssertError(mn, err)
}

func (mn *MasterNodeT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, "backendconfigFull")
	for {
		config := <-ch
		configSubscriberLock.Lock()
		mn.warehouses = []warehouseutils.WarehouseT{}
		allSources := config.Data.(backendconfig.SourcesT)
		for _, source := range allSources.Sources {
			if len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if destination.Enabled {
						mn.warehouses = append(mn.warehouses, warehouseutils.WarehouseT{Source: source, Destination: destination})
						break
					}
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}
