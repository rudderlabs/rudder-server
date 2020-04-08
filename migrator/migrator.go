package migrator

import (
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/pathfinder"
	"github.com/rudderlabs/rudder-server/gateway"

	"github.com/spaolacci/murmur3"
)

//Migrator is a handle to this object used in main.go
type Migrator struct {
	jobsDB *jobsdb.HandleT
	pf		pathfinder.Pathfinder
}

func init() {
	config.Initialize()
	loadConfig()
}

//Setup initializes the module
func (migrator *Migrator) Setup(jobsDB *jobsdb.HandleT, pf pathfinder.Pathfinder) {
	migrator.jobsDB = jobsDB
	logger.Info("Shanmukh: inside migrator setup")
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

	logger.Info("Shanmukh: Migrator loop starting")

	for {
		toQuery := dbReadBatchSize
		logger.Info("Shanmukh: query size ", toQuery)
		
		// customValFilters := []string{gateway.CustomVal}
		// customValFilters := []string{}
		customValFilters := []string{gateway.CustomVal}

		retryList := migrator.jobsDB.GetToRetry(customValFilters, toQuery, nil) //TODO: First argument nil to be replaced with an appropriate version of []string{gateway.CustomVal}
		toQuery -= len(retryList)
		logger.Info("Shanmukh: retryList size ", len(retryList))
		unprocessedList := migrator.jobsDB.GetUnprocessed(customValFilters, toQuery, nil) //TODO: First argument nil to be replaced with an appropriate version of []string{gateway.CustomVal}
		logger.Info("Shanmukh: unprocessed size ", len(unprocessedList))
		if len(unprocessedList)+len(retryList) == 0 {
			logger.Debugf("Shanmukh: Migrator has done with so and so jobsDb. No GW Jobs to process.")
			break
		}
		combinedList := append(unprocessedList, retryList...)
		migrator.filterAndMigrateLocal(combinedList)
	}
}

func (migrator *Migrator) filterAndMigrateLocal(jobList []*jobsdb.JobT) {
	logger.Info("Shanmukh: inside filterAndMigrateLocal")

	m := make(map[pathfinder.NodeMeta][]*jobsdb.JobT)
	for _, job := range jobList {
		eventList, ok := misc.ParseRudderEventBatch(job.EventPayload)
		if !ok {
			//TODO: This can't be happening. This is done only to get userId/anonId. There should be a more reliable way.
			logger.Debug("This can't be happening. This is done only to get userId/anonId. There should be a more reliable way.")
			continue
		}
		userID, ok := misc.GetAnonymousID(eventList[0])
		nodeMeta := migrator.pf.GetNodeFromHash(murmur3.Sum32([]byte(userID)))
		m[nodeMeta] = append(m[nodeMeta], job)
	}

	for nMeta, jobList := range m {
		logger.Info(nMeta, jobList)
	}
}
