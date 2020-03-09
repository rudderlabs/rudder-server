package clusterman

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/rruntime"
	ci "github.com/rudderlabs/rudder-server/warehouse/clusterinterface"
	utils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

//SlaveNodeT will be used on warehouse workers
type SlaveNodeT struct {
	bc *baseComponentT

	//only available if started in master mode
	mn *MasterNodeT
}

//Setup to initialise
func (sn *SlaveNodeT) Setup(dbHandle *sql.DB, config *ci.ClusterConfig) {
	sn.bc = &baseComponentT{}
	sn.bc.Setup(sn, dbHandle, config)

	//Main Loop  - Slave Loop updates workers Info & others
	rruntime.Go(func() {
		sn.slaveLoop()
	})
}

//TearDown to release resources
func (sn *SlaveNodeT) TearDown() {
	sn.bc.TearDown()
}

func (sn *SlaveNodeT) slaveLoop() {

	for {
		select {
		case <-time.After(5 * time.Second):
			sn.updateWorkerInfo()
		}
	}
}

func (sn *SlaveNodeT) getBaseComponent() *baseComponentT {
	return sn.bc
}

//update worker Info so that master has a view of available workers
func (sn *SlaveNodeT) updateWorkerInfo() {
	//TODO: do one upsert query
	for i := 0; i < len(sn.bc.Jq.workers); i++ {
		_, err := sn.bc.dbHandle.Exec(
			fmt.Sprintf(`INSERT INTO  %[1]s (worker_id, status, created_at, updated_at)
							VALUES ('%[2]s', '%[3]s','%[4]s','%[4]s')
							ON
								CONFLICT (worker_id)
							DO
								UPDATE SET status = '%[3]s', updated_at = '%[4]s';`,
				sn.bc.config.WorkerInfoTable,
				sn.bc.Jq.workers[i].IP_UUID,
				sn.bc.Jq.workerStatuses[i], //concurrent access- should not be a problem
				utils.GetCurrentSQLTimestamp()))
		utils.AssertError(sn, err)
	}
}

//Always true for slave
func (sn *SlaveNodeT) isEtlInProgress() bool {
	if sn.mn != nil {
		return sn.mn.isEtlInProgress()
	}
	return true
}
