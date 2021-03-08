package drain

import (
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type DrainI interface {
	CanJobBeDrained(jobID int64, destIDExtractFn func() string) bool
}

type DrainConfig struct {
	MinDrainJobID      int64  `json:"minDrainJobID"`
	MaxDrainJobID      int64  `json:"maxDrainJobID"`
	DrainDestinationID string `json:"drainDestinationID"`
}
type DrainHandleT struct {
	DrainConfigs    []*DrainConfig
	drainUpdateLock sync.RWMutex
	jobsDB          *jobsdb.HandleT
}

var (
	pkgLogger    logger.LoggerI
	drainHandler *DrainHandleT
)

func init() {
	pkgLogger = logger.NewLogger().Child("drain")
}

func Setup(jdb *jobsdb.HandleT) *DrainHandleT {
	if drainHandler != nil {
		return drainHandler
	}
	drainHandler = &DrainHandleT{
		jobsDB: jdb,
	}
	return drainHandler
}

func SetDrainJobIDs(minID int64, maxID int64, destID string) (*DrainHandleT, error) {
	if maxID == 0 {
		maxID = drainHandler.jobsDB.GetLastJobID()
	}
	if maxID < minID {
		return drainHandler, fmt.Errorf("maxID : %d < minID : %d ,skipping drain config update", maxID, minID)
	}
	drainHandler.drainUpdateLock.Lock()
	defer drainHandler.drainUpdateLock.Unlock()
	overridedConfig := false
	for _, dConfig := range drainHandler.DrainConfigs {
		if dConfig.DrainDestinationID == destID {
			dConfig.MaxDrainJobID = maxID
			dConfig.MinDrainJobID = minID
			pkgLogger.Infof(" Drain config overrided : MinJobID : %d, MaxJobID : %d, DestID : %s", minID, maxID, destID)
			overridedConfig = true
			break
		}
	}
	if !overridedConfig {
		newDrainConfig := &DrainConfig{MinDrainJobID: minID, MaxDrainJobID: maxID, DrainDestinationID: destID}
		drainHandler.DrainConfigs = append(drainHandler.DrainConfigs, newDrainConfig)
		pkgLogger.Infof(" New Drain config added : MinJobID : %d, MaxJobID : %d, DestID : %s", newDrainConfig.MinDrainJobID, newDrainConfig.MaxDrainJobID, newDrainConfig.DrainDestinationID)
	}
	return drainHandler, nil
}

func GetDrainJobHandler() *DrainHandleT {
	return drainHandler
}

func FlushDrainJobConfig(destID string) string {
	reply := ""
	drainHandler.drainUpdateLock.Lock()
	defer drainHandler.drainUpdateLock.Unlock()

	if destID == "" {
		reply = "Pass all/<dest-id>"
		return reply
	}
	if destID == "all" {
		drainHandler.DrainConfigs = nil
		reply = "Flushed all DrainConfig"
		pkgLogger.Info(reply)
		return reply
	}
	var newDrainConfigs []*DrainConfig
	isDestIDPresent := false
	for _, dConfig := range drainHandler.DrainConfigs {
		if dConfig.DrainDestinationID == destID {
			isDestIDPresent = true
			continue
		}
		newDrainConfigs = append(newDrainConfigs, dConfig)
	}
	if isDestIDPresent {
		drainHandler.DrainConfigs = newDrainConfigs
		reply = fmt.Sprintf("Flushed drain config for : %s", destID)
		pkgLogger.Info(reply)
	} else {
		reply = fmt.Sprintf("No drain config found for : %s", destID)
	}
	return reply
}

func (d *DrainHandleT) CanJobBeDrained(jobID int64, destIDExtractFn func() string) bool {
	for _, dConfig := range d.DrainConfigs {
		if dConfig.DrainDestinationID == destIDExtractFn() && dConfig.MinDrainJobID <= jobID && jobID < dConfig.MaxDrainJobID {
			return true
		}
	}
	return false
}
