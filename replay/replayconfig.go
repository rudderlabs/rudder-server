package replay

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	processReplays []replayT

	loopSleep            time.Duration
	maxLoopSleep         time.Duration
	dbReadBatchSize      int
	configSubscriberLock sync.RWMutex
)

func init() {
	loadConfig()
}

func loadConfig() {
	processReplays = []replayT{}
	dbReadBatchSize = config.GetInt("Processor.dbReadBatchSize", 10000)
	loopSleep = config.GetDuration("Processor.loopSleepInMS", time.Duration(10)) * time.Millisecond
	maxLoopSleep = config.GetDuration("Processor.maxLoopSleepInMS", time.Duration(5000)) * time.Millisecond
}

type ReplayProcessorT struct {
	gatewayDB *jobsdb.HandleT
	dbHandle  *sql.DB
}

type replayT struct {
	sourceID      string
	destinationID string
	notifyURL     string
}

type replayConfigT struct {
	sourceID      string
	destinationID string
	config        json.RawMessage
	notifyURL     string
	minJobID      int64
	maxDSIndex    int64
}

func (r *ReplayProcessorT) getDBConnectionString() string {
	host := config.GetEnv("JOBS_DB_HOST", "localhost")
	user := config.GetEnv("JOBS_DB_USER", "ubuntu")
	dbname := config.GetEnv("JOBS_DB_DB_NAME", "ubuntu")
	port, _ := strconv.Atoi(config.GetEnv("JOBS_DB_PORT", "5432"))
	password := config.GetEnv("JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
}

func (r *ReplayProcessorT) Setup(gatewayDB *jobsdb.HandleT) {
	r.gatewayDB = gatewayDB

	dbHandle, err := sql.Open("postgres", r.getDBConnectionString())
	if err != nil {
		panic(err)
	}

	r.dbHandle = dbHandle
	logger.Info("ReplayConfig: Connected to DB")
	err = dbHandle.Ping()
	if err != nil {
		panic(err)
	}

	r.createTableIfNotExists()

	rruntime.Go(func() {
		r.backendConfigSubscriber()
	})

	r.crashRecover()

	rruntime.Go(func() {
		r.mainLoop()
	})
}

func (r *ReplayProcessorT) mainLoop() {

	logger.Info("Replay Processor loop started")
	currLoopSleep := time.Duration(0)

	for {

		toQuery := dbReadBatchSize
		//Should not have any failure while processing (in v0) so
		//retryList should be empty. Remove the assert
		retryList := r.gatewayDB.GetToRetry([]string{gateway.CustomVal}, toQuery, nil)
		toQuery -= len(retryList)
		unprocessedList := r.gatewayDB.GetUnprocessed([]string{gateway.CustomVal}, toQuery, nil)

		if len(unprocessedList)+len(retryList) == 0 {
			currLoopSleep = 2*currLoopSleep + loopSleep
			if currLoopSleep > maxLoopSleep {
				currLoopSleep = maxLoopSleep
			}

			time.Sleep(currLoopSleep)
			continue
		} else {
			currLoopSleep = time.Duration(0)
		}

		combinedList := append(unprocessedList, retryList...)
		//Sort by JOBID
		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})

		// Need to process minJobID and new destinations at once
		r.handleReplay(combinedList)
	}
}

func (r *ReplayProcessorT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, "processConfig")
	for {
		config := <-ch
		configSubscriberLock.Lock()
		sources := config.Data.(backendconfig.SourcesT)
		for _, source := range sources.Sources {
			var replays = []replayT{}
			for _, dest := range source.Destinations {
				if dest.Config["Replay"] == true {
					notifyURL, ok := dest.Config["ReplayURL"].(string)
					if !ok {
						notifyURL = ""
					}
					replays = append(replays, replayT{sourceID: source.ID, destinationID: dest.ID, notifyURL: notifyURL})
				}
			}

			if len(replays) > 0 {
				processReplays = r.GetReplaysToProcess(replays)
			}
		}
		configSubscriberLock.Unlock()
	}
}

/*
 * If there is a new replay destination, compute the min JobID that the data plane would be routing for that source
 */
func (r *ReplayProcessorT) handleReplay(combinedList []*jobsdb.JobT) {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()

	if len(processReplays) > 0 {
		maxDSIndex := r.gatewayDB.GetMaxDSIndex()
		if len(combinedList) <= 0 {
			panic(fmt.Errorf("len(combinedList):%d <= 0", len(combinedList)))
		}
		replayMinJobID := combinedList[0].JobID

		r.ProcessNewReplays(processReplays, replayMinJobID, maxDSIndex)
		processReplays = []replayT{}
	}
}

func (r *ReplayProcessorT) crashRecover() {
	sqlStatement := fmt.Sprintf("SELECT source_id, destination_id, notify_url, config FROM replay_config WHERE notified=false")
	rows, err := r.dbHandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var replayConfigs []replayConfigT
	for rows.Next() {
		var replayConfig replayConfigT
		err := rows.Scan(&replayConfig.sourceID, &replayConfig.destinationID, &replayConfig.notifyURL, &replayConfig.config)
		if err != nil {
			panic(err)
		}
		replayConfigs = append(replayConfigs, replayConfig)
	}

	if len(replayConfigs) > 0 {
		rruntime.Go(func() {
			r.notifyReplayConfigs(replayConfigs)
		})
	}

}

// TODO: Make separate requests to for each notify URL
func (r *ReplayProcessorT) notifyReplayConfigs(replayConfigs []replayConfigT) {
	for {
		var replayConfigDataList []map[string]interface{}
		if len(replayConfigs) <= 0 {
			panic(fmt.Errorf("len(replayConfigs):%d <= 0", len(replayConfigs)))
		}
		for _, replayConfig := range replayConfigs {
			config := map[string]interface{}{
				"sourceId":      replayConfig.sourceID,
				"destinationId": replayConfig.destinationID,
				"minJobID":      replayConfig.minJobID,
				"maxDSIndex":    replayConfig.maxDSIndex,
			}
			replayConfigDataList = append(replayConfigDataList, config)
		}

		requestBody := map[string]interface{}{
			"instanceName":         config.GetEnv("INSTANCE_ID", "1"),
			"replayConfigDataList": replayConfigDataList,
		}

		_, ok := backendconfig.MakeBackendPostRequest("/replayConfig", requestBody)
		if ok {
			for _, replayConfig := range replayConfigs {
				r.markReplayConfigNotified(replayConfig)
			}
			break
		}
		time.Sleep(5 * time.Second)
	}
}

func (r *ReplayProcessorT) ProcessNewReplays(replays []replayT, minJobID int64, maxDSIndex int64) error {
	var replayConfigs = []replayConfigT{}

	for _, replay := range replays {
		replayConfig := NewReplayConfig(replay.sourceID, replay.destinationID, minJobID, maxDSIndex, replay.notifyURL)
		err := r.CreateReplayConfig(replayConfig)
		if err != nil {
			return err
		}
		replayConfigs = append(replayConfigs, replayConfig)
	}

	if len(replayConfigs) > 0 {
		rruntime.Go(func() {
			r.notifyReplayConfigs(replayConfigs)
		})
	}
	return nil
}

func (r *ReplayProcessorT) createTableIfNotExists() {
	sqlStatement := `
		CREATE TABLE IF NOT EXISTS replay_config(
				replay_id BIGSERIAL PRIMARY KEY,
				source_id VARCHAR(64) NOT NULL,
				destination_id VARCHAR(64) NOT NULL,
				config JSONB  NOT NULL,
				notify_url TEXT,
				notified BOOLEAN NOT NULL DEFAULT false,
				notified_at TIMESTAMP,
				error TEXT,
				created_at TIMESTAMP NOT NULL);
	`
	_, err := r.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
}

// Processed => entry present and notified
func (r *ReplayProcessorT) checkIfReplayExists(sourceID string, destinationID string) bool {
	var replayID sql.NullInt64
	sqlStatement := fmt.Sprintf("SELECT replay_id FROM replay_config WHERE source_id='%s' AND destination_id='%s'", sourceID, destinationID)
	row := r.dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&replayID)

	replayExists := true
	switch {
	case err == sql.ErrNoRows:
		replayExists = false
	case err != nil:
		if err != nil {
			panic(err)
		}
	default:
		replayExists = true
	}
	return replayExists
}

func (r *ReplayProcessorT) GetReplaysToProcess(replays []replayT) []replayT {
	var processReplays = []replayT{}
	for _, replay := range replays {
		processed := r.checkIfReplayExists(replay.sourceID, replay.destinationID)
		if !processed {
			processReplays = append(processReplays, replay)
		}
	}
	return processReplays
}

func (r *ReplayProcessorT) markReplayConfigNotified(replayConfig replayConfigT) {
	sqlStatement := fmt.Sprintf("UPDATE replay_config SET notified=$3, notified_at=$4 WHERE source_id=$1 AND destination_id=$2")
	_, err := r.dbHandle.Exec(sqlStatement, replayConfig.sourceID, replayConfig.destinationID, true, time.Now())
	if err != nil {
		panic(err)
	}
}

func (r *ReplayProcessorT) CreateReplayConfig(replayConfig replayConfigT) error {

	sqlStatement := fmt.Sprintf("INSERT INTO replay_config (source_id, destination_id, config, notify_url, created_at) VALUES ($1, $2, $3, $4, $5)")
	stmt, err := r.dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(replayConfig.sourceID, replayConfig.destinationID, replayConfig.config, replayConfig.notifyURL, time.Now())
	if err != nil {
		panic(err)
	}
	return err
}

func NewReplayConfig(sourceID string, destinationID string, minJobID int64, maxDSIndex int64, notifyURL string) replayConfigT {
	return replayConfigT{
		sourceID:      sourceID,
		destinationID: destinationID,
		config:        []byte(fmt.Sprintf(`{"minJobID": %v, "maxDSIndex": %v}`, minJobID, maxDSIndex)),
		notifyURL:     notifyURL,
		minJobID:      minJobID,
		maxDSIndex:    maxDSIndex,
	}
}
