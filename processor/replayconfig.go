package processor

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type ReplayProcessorT struct {
	dbHandle *sql.DB
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

func (r *ReplayProcessorT) Setup() {
	dbHandle, err := sql.Open("postgres", r.getDBConnectionString())
	misc.AssertError(err)

	r.dbHandle = dbHandle
	logger.Info("ReplayConfig: Connected to DB")
	err = dbHandle.Ping()
	misc.AssertError(err)

	r.createTableIfNotExists()
}

func (r *ReplayProcessorT) CrashRecover() {
	sqlStatement := fmt.Sprintf("SELECT source_id, destination_id, notify_url, config FROM replay_config WHERE notified=false")
	rows, err := r.dbHandle.Query(sqlStatement)
	misc.AssertError(err)
	defer rows.Close()

	var replayConfigs []replayConfigT
	for rows.Next() {
		var replayConfig replayConfigT
		err := rows.Scan(&replayConfig.sourceID, &replayConfig.destinationID, &replayConfig.notifyURL, &replayConfig.config)
		misc.AssertError(err)
		replayConfigs = append(replayConfigs, replayConfig)
	}

	if len(replayConfigs) > 0 {
		go r.notifyReplayConfigs(replayConfigs)
	}

}

// TODO: Make separate requests to for each notify URL
func (r *ReplayProcessorT) notifyReplayConfigs(replayConfigs []replayConfigT) {
	for {
		var replayConfigDataList []map[string]interface{}
		misc.Assert(len(replayConfigs) > 0)
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
			"instanceName":         config.GetEnv("INSTANCE_NAME", "1"),
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
		go r.notifyReplayConfigs(replayConfigs)
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
	misc.AssertError(err)
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
		misc.AssertError(err)
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
	misc.AssertError(err)
}

func (r *ReplayProcessorT) CreateReplayConfig(replayConfig replayConfigT) error {

	sqlStatement := fmt.Sprintf("INSERT INTO replay_config (source_id, destination_id, config, notify_url, created_at) VALUES ($1, $2, $3, $4, $5)")
	stmt, err := r.dbHandle.Prepare(sqlStatement)
	misc.AssertError(err)
	defer stmt.Close()

	_, err = stmt.Exec(replayConfig.sourceID, replayConfig.destinationID, replayConfig.config, replayConfig.notifyURL, time.Now())
	misc.AssertError(err)
	return err
}

func NewReplayProcessor() (replay *ReplayProcessorT) {
	return &ReplayProcessorT{}
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
