package migrator

import (
	"bufio"
	"encoding/json"
	"os"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/pathfinder"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

//Migrator is a handle to this object used in main.go
type Migrator struct {
	jobsDB *jobsdb.HandleT
	pf     pathfinder.Pathfinder
}

func init() {
	config.Initialize()
	loadConfig()
}

//Setup initializes the module
func (migrator *Migrator) Setup(jobsDB *jobsdb.HandleT, pf pathfinder.Pathfinder) {
	migrator.jobsDB = jobsDB
	migrator.pf = pf
	logger.Info("Shanmukh: inside migrator setup")
	// rruntime.Go(func() {
	// 	migrator.export()
	// })
	rruntime.Go(func() {
		migrator.importFromFile()
	})

}

var (
	dbReadBatchSize int
)

func loadConfig() {
	dbReadBatchSize = config.GetInt("Migrator.dbReadBatchSize", 10000)
}

func (migrator *Migrator) importFromFile() {

	logger.Info("Shanmukh: import loop starting")
	importFiles := []string{"0.json", "1.json", "2.json", "3.json"}

	for _, file := range importFiles {
		migrator.readFromFileAndWriteToDB(file)
		logger.Info("done : ", file)
	}
}

func (migrator *Migrator) readFromFileAndWriteToDB(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	// Scan() reads next line and returns false when reached end or error
	for scanner.Scan() {
		line := scanner.Text()
		if !migrator.processSingleLine(line) {
			return nil
		}
		// process the line
	}
	// check if Scan() finished because of error or because it reached end of file
	return scanner.Err()
}

func (migrator *Migrator) processSingleLine(line string) bool {
	job := jobsdb.JobT{}
	err := json.Unmarshal([]byte(line), &job)
	if err != nil {
		logger.Error(err)
		return false
	}
	migrator.jobsDB.Store([]*jobsdb.JobT{&job})
	return true
}

// func (migrator *Migrator) export() {

// 	logger.Info("Shanmukh: Migrator loop starting")

// 	// for {
// 	toQuery := dbReadBatchSize

// 	jobList := migrator.jobsDB.Get(toQuery)
// 	migrator.filterAndMigrateLocal(jobList)
// 	// }
// }

// func (migrator *Migrator) filterAndMigrateLocal(jobList []*jobsdb.JobT) {
// 	logger.Info("Shanmukh: inside filterAndMigrateLocal")

// 	m := make(map[pathfinder.NodeMeta][]*jobsdb.JobT)
// 	for _, job := range jobList {
// 		eventList, ok := misc.ParseRudderEventBatch(job.EventPayload)
// 		if !ok {
// 			//TODO: This can't be happening. This is done only to get userId/anonId. There should be a more reliable way.
// 			logger.Debug("This can't be happening. This is done only to get userId/anonId. There should be a more reliable way.")
// 			continue
// 		}
// 		userID, ok := misc.GetAnonymousID(eventList[0])

// 		nodeMeta := migrator.pf.GetNodeFromHash(murmur3.Sum32([]byte(userID)))
// 		m[nodeMeta] = append(m[nodeMeta], job)
// 	}

// 	for nMeta, jobList := range m {
// 		file, err := os.OpenFile(fmt.Sprintf("%d.json", nMeta.GetNodeID()), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

// 		if err != nil {
// 			log.Fatalf("failed creating file: %s", err)
// 		}

// 		datawriter := bufio.NewWriter(file)

// 		for _, job := range jobList {
// 			m, err := json.Marshal(job)
// 			if err != nil {
// 				logger.Error("Something went wrong in marshalling")
// 			}
// 			_, _ = datawriter.WriteString(string(m) + "\n")
// 		}
// 		logger.Info(nMeta, len(jobList))
// 		datawriter.Flush()
// 		file.Close()
// 	}
// }
