package clusterman

import (
	"database/sql"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/strings"
	utils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

//MasterNodeT will be used on warehouse master
type MasterNodeT struct {
	cn *clusterNodeT

	isSetup bool
}

//Setup to initialise
func (mn *MasterNodeT) Setup(dbHandle *sql.DB, config *ClusterConfig) {

	if mn.isSetup {
		utils.AssertString(mn, strings.STR_WH_MASTER_ALREADY_SETUP)
	}
	defer func() { mn.isSetup = true }()

	mn.setupTables(dbHandle, config)

	mn.cn = &clusterNodeT{}
	mn.cn.Setup(mn, dbHandle, config)
}

//TearDown to release resources
func (mn *MasterNodeT) TearDown() {
	mn.cn.TearDown()
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

	//create the table
	sqlStmt = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
										  id BIGSERIAL PRIMARY KEY,
										  staging_file_id BIGINT, 
										  status wh_job_queue_status_type NOT NULL, 
										  worker_id VARCHAR(64) NOT NULL,
										  job_create_time TIMESTAMP NOT NULL,
										  status_update_time TIMESTAMP NOT NULL,
										  last_error VARCHAR(512));`, config.jobQueueTable)

	_, err = dbHandle.Exec(sqlStmt)
	utils.AssertError(mn, err)

	// create index on status
	sqlStmt = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_status_idx ON %[1]s (status);`, config.jobQueueTable)
	_, err = dbHandle.Exec(sqlStmt)
	utils.AssertError(mn, err)
}
