package dedup

import "github.com/rudderlabs/rudder-server/jobsdb"

var (
	dedupManager DedupI
)

// GetInstance returns an instance of DedupI
func GetInstance(gatewayDB jobsdb.JobsDB, clearDB *bool) DedupI {
	pkgLogger.Info("[[ Dedup ]] Setting up Dedup Manager")
	if dedupManager == nil {
		handler := &DedupHandleT{}
		handler.setup(gatewayDB, clearDB)
		dedupManager = handler
	}
	return dedupManager
}
