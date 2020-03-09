package clusterman

import (
	"database/sql"

	"github.com/rudderlabs/rudder-server/config"
)

//ClusterNodeI is the abstraction for mater and slave node types
type ClusterNodeI interface {
	Setup(dbHandle *sql.DB, config *ClusterConfig)
	TearDown()
	getBaseComponent() *baseComponentT
	isEtlInProgress() bool
}

//base component held by both master and slave node structures
type baseComponentT struct {
	dbHandle *sql.DB
	Jq       *JobQueueHandleT
	config   *ClusterConfig
}

//Setup Setup to initialise
func (bc *baseComponentT) Setup(ci ClusterNodeI, dbHandle *sql.DB, config *ClusterConfig) {
	bc.dbHandle = dbHandle
	bc.config = config

	var jobQueueHandle JobQueueHandleT
	bc.Jq = &jobQueueHandle
	bc.Jq.Setup(ci)
}

//TearDown to release resources
func (bc *baseComponentT) TearDown() {
	bc.Jq.TearDown()
}

//Generic Functions Section

//ClusterConfig parameters
type ClusterConfig struct {
	jobQueueTable         string
	jobQueueNotifyChannel string
	workerInfoTable       string
}

//LoadConfig loads the necessary config into ClusterConfig
func LoadConfig() *ClusterConfig {

	return &ClusterConfig{
		jobQueueTable:         config.GetString("Warehouse.jobQueueTable", "wh_job_queue"),
		jobQueueNotifyChannel: config.GetString("Warehouse.jobQueueNotifyChannel", "wh_job_queue_status_channel"),
		workerInfoTable:       config.GetString("Warehouse.workerInfoTable ", "wh_workers"),
	}
}
