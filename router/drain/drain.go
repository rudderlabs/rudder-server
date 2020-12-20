package drain

import (
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"sync"
)

type DrainI interface {
	//CanJobBeDrained(id int64) bool
	//CanJobBeDrained(job *jobsdb.JobT) bool
	CanJobBeDrained(jobID int64, destID string) bool
}

type DrainHandleT struct {
	//TODO : need no boolean, i.e if either min/max is set, it is enabled
	//enableDrainSupport						bool  `json:"-"`
	MinDrainJobID      int64  `json:"minDrainJobID"`
	MaxDrainJobID      int64  `json:"maxDrainJobID"`
	DrainDestinationID string `json:"drainDestinationID"`
	drainUpdateLock    sync.RWMutex
}

var (
	pkgLogger    logger.LoggerI
	drainHandler *DrainHandleT
)

func loadConfig() {
	drainHandler = &DrainHandleT{
		//enableDrainSupport: config.GetBool("Router.enableDrainJobs", false),
		MinDrainJobID:      config.GetInt64("Router.minDrainJobID", 0),
		MaxDrainJobID:      config.GetInt64("Router.maxDrainJobID", 0),
		DrainDestinationID: config.GetString("Router.drainDestinationID", ""),
	}
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("drain")
}

func SetDrainJobIDs(minID int64, maxID int64, destID string) (DrainHandleT, error) {
	drainHandler.drainUpdateLock.Lock()
	//drainHandler.enableDrainSupport = true
	drainHandler.MinDrainJobID = minID
	drainHandler.MaxDrainJobID = maxID
	drainHandler.DrainDestinationID = destID
	drainHandler.drainUpdateLock.Unlock()

	pkgLogger.Infof("Drain config set to : MinJobID : %d, MaxJobID : %d, DestID : %s", drainHandler.MinDrainJobID, drainHandler.MaxDrainJobID, drainHandler.DrainDestinationID)
	return *drainHandler, nil
}

func GetDrainJobHandler() *DrainHandleT {
	return drainHandler
}

/*func (d DrainHandleT) CanJobBeDrained(id int64) bool{
	if d.enableDrainSupport && d.minDrainJobID < id && id < d.maxDrainJobID {
		return true
	}
	return false;
}*/

/*
func (d DrainHandleT) CanJobBeDrained(job *jobsdb.JobT) bool{
	var parameters router.JobParametersT
	err := json.Unmarshal(job.Parameters, &parameters)
	if err != nil {
		pkgLogger.Error("Unmarshal of job parameters failed. ", string(job.Parameters))
	}
	destId := parameters.DestinationID
	//Drains all dest, if no destId is provided.
	destBeDrained := d.drainDestinationID == "" || d.drainDestinationID == destId
	if d.enableDrainSupport && d.minDrainJobID < job.JobID && job.JobID < d.maxDrainJobID && destBeDrained {
		return true
	}
	return false;
}
*/

func (d *DrainHandleT) CanJobBeDrained(jobID int64, destID string) bool {
	destBeDrained := d.DrainDestinationID == "" || d.DrainDestinationID == destID
	//pkgLogger.Info("Checking against : ", d.MinDrainJobID, d.MaxDrainJobID, d.DrainDestinationID, "current Jon : ", jobID)
	if d.MinDrainJobID <= jobID && jobID < d.MaxDrainJobID && destBeDrained {
		return true
	}
	return false
}
