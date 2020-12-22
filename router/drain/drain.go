package drain

import (
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"sync"
)

type DrainI interface {
	//CanJobBeDrained(id int64) bool
	//CanJobBeDrained(job *jobsdb.JobT) bool
	CanJobBeDrained(jobID int64, destID string) bool
}

type DrainConfig struct {
	//TODO : need no boolean, i.e if either min/max is set, it is enabled
	//enableDrainSupport						bool  `json:"-"`
	MinDrainJobID      int64  `json:"minDrainJobID"`
	MaxDrainJobID      int64  `json:"maxDrainJobID"`
	DrainDestinationID string `json:"drainDestinationID"`
}

type DrainHandleT struct {
	DrainConfigs 		[]*DrainConfig
	drainUpdateLock    sync.RWMutex
	rt 					*router.HandleT
}

var (
	pkgLogger    logger.LoggerI
	drainHandler *DrainHandleT
	//drainConfig  []*DrainHandleT
)

/*func loadConfig() {
	drainHandler = &DrainHandleT{
		//enableDrainSupport: config.GetBool("Router.enableDrainJobs", false),
		MinDrainJobID:      config.GetInt64("Router.minDrainJobID", 0),
		MaxDrainJobID:      config.GetInt64("Router.maxDrainJobID", 0),
		DrainDestinationID: config.GetString("Router.drainDestinationID", ""),
	}
	drainConfig = append(drainConfig, drainHandler)
}*/

func init() {
	//loadConfig()
	pkgLogger = logger.NewLogger().Child("drain")
}

func setup(rtHandle *router.HandleT) *DrainHandleT{
	drainHandler = &DrainHandleT{
		rt: rtHandle,
	}
	return drainHandler
}

func SetDrainJobIDs(minID int64, maxID int64, destID string) (*DrainHandleT, error) {
	if maxID == 0 {
		maxID = drainHandler.rt.MaxJobId()
	}
	drainHandler.drainUpdateLock.Lock()
	newDrainConfig := &DrainConfig{MinDrainJobID: minID, MaxDrainJobID: maxID, DrainDestinationID: destID}
	drainHandler.DrainConfigs = append(drainHandler.DrainConfigs, newDrainConfig)
	drainHandler.drainUpdateLock.Unlock()

	pkgLogger.Infof(" New Drain config added : MinJobID : %d, MaxJobID : %d, DestID : %s", newDrainConfig.MinDrainJobID, newDrainConfig.MaxDrainJobID, newDrainConfig.DrainDestinationID)
	return drainHandler, nil
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
	for _, dConfig := range d.DrainConfigs{
		//pkgLogger.Info("Checking against : ", d.MinDrainJobID, d.MaxDrainJobID, d.DrainDestinationID, "current Jon : ", jobID)
		if dConfig.DrainDestinationID == destID && dConfig.MinDrainJobID <= jobID && jobID < dConfig.MaxDrainJobID {
			return true
		}
	}
	return false
}
