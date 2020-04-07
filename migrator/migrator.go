package migrator

import (
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
)
//Migrator is a handle to this object used in main.go
type Migrator struct {
	jobsDB *jobsdb.HandleT
}

func init() {
	config.Initialize()
	loadConfig()
}

//Setup initializes the module
func (migrator *Migrator) Setup(jobsDB *jobsdb.HandleT) {
	migrator.jobsDB = jobsDB
	rruntime.Go(func() {
		migrator.migrate()
	})
}

var (
	dbReadBatchSize int
)

func loadConfig() {
	dbReadBatchSize = config.GetInt("Migrator.dbReadBatchSize", 10000)
}

func (migrator *Migrator) migrate() {

	logger.Info("Migrator loop starting")

	for {
		toQuery := dbReadBatchSize
		retryList := migrator.jobsDB.GetToRetry(nil, toQuery, nil) //TODO: First argument nil to be replaced with an appropriate version of []string{gateway.CustomVal}
		toQuery -= len(retryList)
		unprocessedList := proc.gatewayDB.GetUnprocessed(nil, toQuery, nil) //TODO: First argument nil to be replaced with an appropriate version of []string{gateway.CustomVal}
		if len(unprocessedList)+len(retryList) == 0 {
			logger.Debugf("Migrator has done with so and so jobsDb. No GW Jobs to process.")
		}

	}
}
