package trackedusers

import "github.com/rudderlabs/rudder-go-kit/logger"

type Factory struct {
	Log logger.Logger
}

func (f *Factory) Setup(dbConn string) (DataCollector, error) {
	return NewUniqueUsersCollector(f.Log, dbConn)
}
