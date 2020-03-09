package clusterman

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/strings"
	utils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

//MasterNodeT will be used on warehouse master
type MasterNodeT struct {
	bc *baseComponentT
	sn *SlaveNodeT //A Master Node is also a Worker

	isSetup bool
}

//Setup to initialise
func (mn *MasterNodeT) Setup(dbHandle *sql.DB, config *ClusterConfig) {

	if mn.isSetup {
		utils.AssertString(mn, strings.STR_WH_MASTER_ALREADY_SETUP)
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

	//Main Loop  - Master Node  periodically polls job queue
	// & force updates a few jobs so that notifications are regenerated
	rruntime.Go(func() {
		mn.masterLoop()
	})
}

//TearDown to release resources
func (mn *MasterNodeT) TearDown() {
	mn.sn.TearDown()
	mn.bc.TearDown()
}

func (mn *MasterNodeT) masterLoop() {

	for {
		select {
		case <-time.After(5 * time.Second):
			mn.updatePendingJobs()
		}
	}
}

func (mn *MasterNodeT) getBaseComponent() *baseComponentT {
	return mn.bc
}

//force update a few jobs so that notifications are generated for workers
func (mn *MasterNodeT) updatePendingJobs() {
	//TODO: limit by available number of workers from worker info table
	_, err := mn.bc.dbHandle.Exec(
		fmt.Sprintf(`UPDATE %[1]s SET status='new',  
							status_updated_at = '%[2]s'
							WHERE id IN  (
							SELECT id
							FROM %[1]s
							WHERE status='new' AND status_updated_at < '%[3]s'
							ORDER BY id
							FOR UPDATE SKIP LOCKED
							LIMIT 10
							);`,
			mn.bc.config.jobQueueTable,
			utils.GetCurrentSQLTimestamp(),
			utils.GetSQLTimestamp(time.Now().Add(5*time.Second))))
	utils.AssertError(mn, err)
}

//Create the required tables
func (mn *MasterNodeT) setupTables(dbHandle *sql.DB, config *ClusterConfig) {
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
	utils.AssertError(mn, err)

	//create the job queue table
	sqlStmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
										  id BIGSERIAL PRIMARY KEY,
										  staging_file_id BIGINT, 
										  status wh_job_queue_status_type NOT NULL, 
										  worker_id VARCHAR(64) NOT NULL,
										  error_count INT DEFAULT 0,
										  job_created_at TIMESTAMP NOT NULL,
										  status_updated_at TIMESTAMP NOT NULL,
										  last_error VARCHAR(512));`, config.jobQueueTable)

	_, err = dbHandle.Exec(sqlStmt)
	utils.AssertError(mn, err)

	// create index on status
	sqlStmt = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_status_idx ON %[1]s (status);`, config.jobQueueTable)
	_, err = dbHandle.Exec(sqlStmt)
	utils.AssertError(mn, err)

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
	utils.AssertError(mn, err)

	//create the worker info table
	sqlStmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			worker_id VARCHAR(64)  NOT NULL UNIQUE,
			status wh_worker_status NOT NULL, 
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL);`, config.workerInfoTable)
	_, err = dbHandle.Exec(sqlStmt)
	utils.AssertError(mn, err)
}
