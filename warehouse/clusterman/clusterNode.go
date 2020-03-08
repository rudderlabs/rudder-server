package clusterman

import (
	"database/sql"

	"github.com/rudderlabs/rudder-server/config"
)

//ClusterNodeI is the abstraction for mater and slave node types
type ClusterNodeI interface {
	Setup(dbHandle *sql.DB, config *ClusterConfig)
	TearDown()
}

//ClusterConfig parameters
type ClusterConfig struct {
	jobQueueTable         string
	jobQueueNotifyChannel string
}

type clusterNodeT struct {
	dbHandle *sql.DB
	Jq       *JobQueueHandleT
	config   *ClusterConfig
}

//LoadConfig loads the necessary config into ClusterConfig
func LoadConfig() *ClusterConfig {

	return &ClusterConfig{
		jobQueueTable:         config.GetString("Warehouse.jobQueueTable", "wh_job_queue"),
		jobQueueNotifyChannel: config.GetString("Warehouse.jobQueueNotifyChannel", "wh_job_queue_status_channel"),
	}
}

//Setup Setup to initialise
func (cn *clusterNodeT) Setup(ci ClusterNodeI, dbHandle *sql.DB, config *ClusterConfig) {
	cn.dbHandle = dbHandle
	cn.config = config

	var jobQueueHandle JobQueueHandleT
	cn.Jq = &jobQueueHandle
	cn.Jq.Setup(cn)
}

//TearDown to release resources
func (cn *clusterNodeT) TearDown() {
	cn.Jq.TearDown()
}

//TearDown to release resources
func (cn *clusterNodeT) GetDBHandle() {
	cn.Jq.TearDown()
}
