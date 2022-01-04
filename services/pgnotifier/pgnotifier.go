package pgnotifier

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	uuid "github.com/gofrs/uuid"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	queueName          string
	maxAttempt         int
	trackBatchInterval time.Duration
	maxPollSleep       time.Duration
	pkgLogger          logger.LoggerI
)

var (
	pgNotifierDBhost, pgNotifierDBuser, pgNotifierDBpassword, pgNotifierDBname, pgNotifierDBsslmode          string
	pgNotifierDBport                                                                                         int
	pgNotifierPublish, pgNotifierPublishTime                                                                 stats.RudderStats
	pgNotifierClaimSucceeded, pgNotifierClaimSucceededTime, pgNotifierClaimFailed, pgNotifierClaimFailedTime stats.RudderStats
	pgNotifierClaimUpdateFailed                                                                              stats.RudderStats
)

const (
	WaitingState   = "waiting"
	ExecutingState = "executing"
	SucceededState = "succeeded"
	FailedState    = "failed"
	AbortedState   = "aborted"
)

func Init() {
	loadPGNotifierConfig()
	queueName = "pg_notifier_queue"
	pkgLogger = logger.NewLogger().Child("warehouse").Child("pgnotifier")
}

type PgNotifierT struct {
	URI                 string
	dbHandle            *sql.DB
	workspaceIdentifier string
}

type JobPayload json.RawMessage

type ResponseT struct {
	JobID  int64
	Status string
	Output json.RawMessage
	Error  string
}

type ClaimT struct {
	ID            int64
	BatchID       string
	Status        string
	Payload       JobPayload
	ClaimResponse ClaimResponseT
}

type ClaimResponseT struct {
	Payload json.RawMessage
	Err     error
}

func loadPGNotifierConfig() {
	pgNotifierDBhost = config.GetEnv("PGNOTIFIER_DB_HOST", "localhost")
	pgNotifierDBuser = config.GetEnv("PGNOTIFIER_DB_USER", "ubuntu")
	pgNotifierDBname = config.GetEnv("PGNOTIFIER_DB_NAME", "ubuntu")
	pgNotifierDBport, _ = strconv.Atoi(config.GetEnv("PGNOTIFIER_DB_PORT", "5432"))
	pgNotifierDBpassword = config.GetEnv("PGNOTIFIER_DB_PASSWORD", "ubuntu") // Reading secrets from
	pgNotifierDBsslmode = config.GetEnv("PGNOTIFIER_DB_SSL_MODE", "disable")
	config.RegisterIntConfigVariable(3, &maxAttempt, false, 1, "PgNotifier.maxAttempt")
	trackBatchInterval = time.Duration(config.GetInt("PgNotifier.trackBatchIntervalInS", 2)) * time.Second
	config.RegisterDurationConfigVariable(time.Duration(5000), &maxPollSleep, true, time.Millisecond, "PgNotifier.maxPollSleep")
}

//New Given default connection info return pg notifiew object from it
func New(workspaceIdentifier string, fallbackConnectionInfo string) (notifier PgNotifierT, err error) {

	// by default connection info is fallback connection info
	connectionInfo := fallbackConnectionInfo

	// if PG Notifier variables are defined then use get values provided in env vars
	if CheckForPGNotifierEnvVars() {
		connectionInfo = GetPGNotifierConnectionString()
	}
	pkgLogger.Infof("PgNotifier: Initializing PgNotifier...")
	dbHandle, err := sql.Open("postgres", connectionInfo)
	if err != nil {
		return
	}

	// setup metrics
	pgNotifierModuleTag := warehouseutils.Tag{Name: "module", Value: "pgnotifier"}
	// publish metrics
	pgNotifierPublish = warehouseutils.NewCounterStat("pgnotifier_publish", pgNotifierModuleTag)
	pgNotifierPublishTime = warehouseutils.NewTimerStat("pgnotifier_publish_time", pgNotifierModuleTag)
	// claim metrics
	pgNotifierClaimSucceeded = warehouseutils.NewCounterStat("pgnotifier_claim", pgNotifierModuleTag, warehouseutils.Tag{Name: "status", Value: "succeeded"})
	pgNotifierClaimFailed = warehouseutils.NewCounterStat("pgnotifier_claim", pgNotifierModuleTag, warehouseutils.Tag{Name: "status", Value: "failed"})
	pgNotifierClaimSucceededTime = warehouseutils.NewTimerStat("pgnotifier_claim_time", pgNotifierModuleTag, warehouseutils.Tag{Name: "status", Value: "succeeded"})
	pgNotifierClaimFailedTime = warehouseutils.NewTimerStat("pgnotifier_claim_time", pgNotifierModuleTag, warehouseutils.Tag{Name: "status", Value: "failed"})
	pgNotifierClaimUpdateFailed = warehouseutils.NewCounterStat("pgnotifier_claim_update_failed", pgNotifierModuleTag)

	notifier = PgNotifierT{
		dbHandle:            dbHandle,
		URI:                 connectionInfo,
		workspaceIdentifier: workspaceIdentifier,
	}
	err = notifier.setupQueue()
	return
}

func (notifier PgNotifierT) GetDBHandle() *sql.DB {
	return notifier.dbHandle
}

func (notifier PgNotifierT) AddTopic(ctx context.Context, topic string) (err error) {

	// clean up all jobs in pgnotifier for same workspace
	// additional safety check to not delete all jobs with empty workspaceIdentifier
	if notifier.workspaceIdentifier != "" {
		stmt := fmt.Sprintf("DELETE FROM %s WHERE workspace='%s' AND topic ='%s'", queueName, notifier.workspaceIdentifier, topic)
		pkgLogger.Infof("PgNotifier: Deleting all jobs on topic: %s", topic)
		_, err = notifier.dbHandle.Exec(stmt)
		if err != nil {
			return
		}
	}

	return
}

// CheckForPGNotifierEnvVars Checks if all the required Env Variables for PG Notifier are present
func CheckForPGNotifierEnvVars() bool {
	return config.IsEnvSet("PGNOTIFIER_DB_HOST") &&
		config.IsEnvSet("PGNOTIFIER_DB_USER") &&
		config.IsEnvSet("PGNOTIFIER_DB_NAME") &&
		config.IsEnvSet("PGNOTIFIER_DB_PASSWORD")
}

// GetPGNotifierConnectionString Returns PG Notifier DB Connection Configuration
func GetPGNotifierConnectionString() string {
	pkgLogger.Debugf("WH: All Env variables required for separate PG Notifier are set... Check pg notifier says True...")
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s",
		pgNotifierDBhost, pgNotifierDBport, pgNotifierDBuser,
		pgNotifierDBpassword, pgNotifierDBname, pgNotifierDBsslmode)
}

func (notifier *PgNotifierT) trackBatch(batchID string, ch *chan []ResponseT) {
	rruntime.Go(func() {
		for {
			time.Sleep(trackBatchInterval)
			// keep polling db for batch status
			// or subscribe to triggers
			stmt := fmt.Sprintf(`SELECT count(*) FROM %s WHERE batch_id='%s' AND status!='%s' AND status!='%s'`, queueName, batchID, SucceededState, AbortedState)
			var count int
			err := notifier.dbHandle.QueryRow(stmt).Scan(&count)
			if err != nil {
				pkgLogger.Errorf("PgNotifier: Failed to query for tracking jobs by batch_id: %s, connInfo: %s", stmt, notifier.URI)
				panic(err)
			}

			if count == 0 {
				stmt = fmt.Sprintf(`SELECT payload->'StagingFileID', payload->'Output', status, error FROM %s WHERE batch_id = '%s'`, queueName, batchID)
				rows, err := notifier.dbHandle.Query(stmt)
				if err != nil {
					panic(err)
				}
				responses := []ResponseT{}
				for rows.Next() {
					var status, jobError, output sql.NullString
					var jobID int64
					err = rows.Scan(&jobID, &output, &status, &jobError)
					if err != nil {
						panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", stmt, err))
					}
					responses = append(responses, ResponseT{
						JobID:  jobID,
						Output: []byte(output.String),
						Status: status.String,
						Error:  jobError.String,
					})
				}
				rows.Close()
				*ch <- responses
				pkgLogger.Infof("PgNotifier: Completed processing all files  in batch: %s", batchID)
				stmt = fmt.Sprintf(`DELETE FROM %s WHERE batch_id = '%s'`, queueName, batchID)
				_, err = notifier.dbHandle.Exec(stmt)
				if err != nil {
					pkgLogger.Errorf("PgNotifier: Error deleting from %s for batch_id:%s : %v", queueName, batchID, err)
				}
				break
			} else {
				pkgLogger.Debugf("PgNotifier: Pending %d files to process in batch: %s", count, batchID)
			}
		}
	})
}

func (notifier *PgNotifierT) UpdateClaimedEvent(id int64, response *ClaimResponseT) {
	//rruntime.Go(func() {
	//	response := <-ch
	var err error
	if response.Err != nil {
		pkgLogger.Error(response.Err.Error())
		stmt := fmt.Sprintf(`UPDATE %[1]s SET status=(CASE
									WHEN attempt > %[2]d
									THEN CAST ( '%[3]s' AS pg_notifier_status_type)
									ELSE  CAST( '%[4]s' AS pg_notifier_status_type)
									END), attempt = attempt + 1, updated_at = '%[5]s', error = %[6]s
									WHERE id = %[7]v`, queueName, maxAttempt, AbortedState, FailedState, GetCurrentSQLTimestamp(), misc.QuoteLiteral(response.Err.Error()), id)
		_, err = notifier.dbHandle.Exec(stmt)
	} else {
		stmt := fmt.Sprintf(`UPDATE %[1]s SET status='%[2]s', updated_at = '%[3]s', payload = $1 WHERE id = %[4]v`, queueName, SucceededState, GetCurrentSQLTimestamp(), id)
		_, err = notifier.dbHandle.Exec(stmt, response.Payload)
	}

	if err != nil {
		pgNotifierClaimUpdateFailed.Increment()
		pkgLogger.Errorf("PgNotifier: Failed to update claimed event: %v", err)
	}
	//})
}

func (notifier *PgNotifierT) claim(workerID string) (claim ClaimT, err error) {
	claimStartTime := time.Now()
	defer func() {
		if err != nil {
			pgNotifierClaimFailedTime.Since(claimStartTime)
			pgNotifierClaimFailed.Increment()
			return
		}
		pgNotifierClaimSucceededTime.Since(claimStartTime)
		pgNotifierClaimSucceeded.Increment()
	}()
	var claimedID int64
	var batchID, status string
	var payload JobPayload
	stmt := fmt.Sprintf(`UPDATE %[1]s SET status='%[2]s',
						updated_at = '%[3]s',
						last_exec_time = '%[3]s',
						worker_id = '%[4]v'
						WHERE id = (
						SELECT id
						FROM %[1]s
						WHERE status='%[5]s' OR status='%[6]s'
						ORDER BY priority ASC, id ASC
						FOR UPDATE SKIP LOCKED
						LIMIT 1
						)
						RETURNING id, batch_id, status, payload;`, queueName, ExecutingState, GetCurrentSQLTimestamp(), workerID, WaitingState, FailedState)

	tx, err := notifier.dbHandle.Begin()
	if err != nil {
		return
	}
	err = tx.QueryRow(stmt).Scan(&claimedID, &batchID, &status, &payload)

	if err != nil {
		pkgLogger.Debugf("PgNotifier: Claim failed: %v, query: %s, connInfo: %s", err, stmt, notifier.URI)
		tx.Rollback()
		return
	}

	err = tx.Commit()

	if err != nil {
		pkgLogger.Errorf("PgNotifier: Error commiting claim txn: %v", err)
		tx.Rollback()
		return
	}

	claim = ClaimT{
		ID:      claimedID,
		BatchID: batchID,
		Status:  status,
		Payload: payload,
	}
	return claim, nil
}

func (notifier *PgNotifierT) Publish(topic string, jobs []json.RawMessage, priority int) (ch chan []ResponseT, err error) {
	publishStartTime := time.Now()
	defer func() {
		if err == nil {
			pgNotifierPublishTime.Since(publishStartTime)
			pgNotifierPublish.Increment()
		}
	}()

	ch = make(chan []ResponseT)

	//Using transactions for bulk copying
	txn, err := notifier.dbHandle.Begin()
	if err != nil {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(queueName, "batch_id", "status", "topic", "payload", "workspace", "priority"))
	if err != nil {
		return
	}
	defer stmt.Close()

	batchID := uuid.Must(uuid.NewV4()).String()
	pkgLogger.Infof("PgNotifier: Inserting %d records into %s as batch: %s", len(jobs), queueName, batchID)
	for _, job := range jobs {
		_, err = stmt.Exec(batchID, WaitingState, topic, string(job), notifier.workspaceIdentifier, priority)
		if err != nil {
			return
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		pkgLogger.Errorf("PgNotifier: Error publishing messages: %v", err)
		return
	}
	err = txn.Commit()
	if err != nil {
		pkgLogger.Errorf("PgNotifier: Error in publishing messages: %v", err)
		return
	}
	pkgLogger.Infof("PgNotifier: Inserted %d records into %s as batch: %s", len(jobs), queueName, batchID)
	stats.NewTaggedStat("pg_notifier_insert_records", stats.CountType, map[string]string{
		"queueName": queueName,
		"batchID":   batchID,
		"module":    "pg_notifier",
	}).Count(len(jobs))
	notifier.trackBatch(batchID, &ch)
	return
}

func (notifier *PgNotifierT) Subscribe(workerId string, topic string, channelSize int) chan ClaimT {

	ch := make(chan ClaimT, channelSize)
	rruntime.Go(func() {
		pollSleep := time.Duration(0)
		for {
			claimedJob, err := notifier.claim(workerId)
			if err == nil {
				ch <- claimedJob
				pollSleep = time.Duration(0)
			} else {
				pollSleep = 2*pollSleep + time.Duration(rand.Intn(100))*time.Millisecond
				if pollSleep > maxPollSleep {
					pollSleep = maxPollSleep
				}
			}
			time.Sleep(pollSleep)
		}
	})
	return ch
}

func (notifier *PgNotifierT) setupQueue() (err error) {
	pkgLogger.Infof("PgNotifier: Creating Job Queue Tables ")

	m := &migrator.Migrator{
		Handle:                     notifier.dbHandle,
		MigrationsTable:            "pg_notifier_queue_migrations",
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", false),
	}
	err = m.Migrate("pg_notifier_queue")
	if err != nil {
		panic(fmt.Errorf("could not run pg_notifier_queue migrations: %w", err))
	}

	return
}

//GetCurrentSQLTimestamp to get sql complaint current datetime string
func GetCurrentSQLTimestamp() string {
	const SQLTimeFormat = "2006-01-02 15:04:05"
	return time.Now().Format(SQLTimeFormat)
}

//GetSQLTimestamp to get sql complaint current datetime string from the given duration
func GetSQLTimestamp(t time.Time) string {
	const SQLTimeFormat = "2006-01-02 15:04:05"
	return t.Format(SQLTimeFormat)
}
