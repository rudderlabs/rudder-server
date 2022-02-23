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
	"github.com/spaolacci/murmur3"

	pglock "github.com/allisson/go-pglock/v2"
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
	jobOrphanTimeout   time.Duration
	pkgLogger          logger.LoggerI
	enableConnTuning   bool
	maxConnLifeTimeInS time.Duration
	maxIdleConnTimeInS time.Duration
	maxOpenConns       int
	maxIdleConns       int
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
	ID        int64
	BatchID   string
	Status    string
	Workspace string
	Payload   json.RawMessage
	Attempt   int
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
	config.RegisterDurationConfigVariable(time.Duration(120), &jobOrphanTimeout, true, time.Second, "PgNotifier.jobOrphanTimeout")
	config.RegisterDurationConfigVariable(time.Duration(1800), &maxConnLifeTimeInS, false, time.Second, "PgNotifier.maxConnLifeTimeInS")
	config.RegisterDurationConfigVariable(time.Duration(1), &maxIdleConnTimeInS, false, time.Second, "PgNotifier.maxIdleConnTimeInS")
	config.RegisterIntConfigVariable(25, &maxOpenConns, false, 1, "PgNotifier.maxOpenConns")
	config.RegisterIntConfigVariable(25, &maxIdleConns, false, 1, "PgNotifier.maxIdleConns")
	config.RegisterBoolConfigVariable(false, &enableConnTuning, false, "PgNotifier.enableConnTuning")
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

	// Reference: http://go-database-sql.org/connection-pool.html
	if enableConnTuning {
		dbHandle.SetConnMaxLifetime(maxConnLifeTimeInS)
		dbHandle.SetConnMaxIdleTime(maxIdleConnTimeInS)
		dbHandle.SetMaxOpenConns(maxOpenConns)
		dbHandle.SetMaxIdleConns(maxIdleConns)
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

func (notifier PgNotifierT) ClearJobs(ctx context.Context) (err error) {

	// clean up all jobs in pgnotifier for same workspace
	// additional safety check to not delete all jobs with empty workspaceIdentifier
	if notifier.workspaceIdentifier != "" {
		stmt := fmt.Sprintf("DELETE FROM %s WHERE workspace='%s'", queueName, notifier.workspaceIdentifier)
		pkgLogger.Infof("PgNotifier: Deleting all jobs for workspace: %s", notifier.workspaceIdentifier)
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

func (notifier *PgNotifierT) UpdateClaimedEvent(claim *ClaimT, response *ClaimResponseT) {
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
									WHERE id = %[7]v`, queueName, maxAttempt, AbortedState, FailedState, GetCurrentSQLTimestamp(), misc.QuoteLiteral(response.Err.Error()), claim.ID)
		_, err = notifier.dbHandle.Exec(stmt)

		// Sending stats when we mark pg_notifier status as aborted.
		if claim.Attempt > maxAttempt {
			stats.NewTaggedStat("pg_notifier_aborted_records", stats.CountType, map[string]string{
				"queueName": queueName,
				"workspace": claim.Workspace,
				"module":    "pg_notifier",
			}).Increment()
		}
	} else {
		stmt := fmt.Sprintf(`UPDATE %[1]s SET status='%[2]s', updated_at = '%[3]s', payload = $1 WHERE id = %[4]v`, queueName, SucceededState, GetCurrentSQLTimestamp(), claim.ID)
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
	var attempt int
	var batchID, status, workspace string
	var payload json.RawMessage
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
						RETURNING id, batch_id, status, payload, workspace, attempt;`, queueName, ExecutingState, GetCurrentSQLTimestamp(), workerID, WaitingState, FailedState)

	tx, err := notifier.dbHandle.Begin()
	if err != nil {
		return
	}
	err = tx.QueryRow(stmt).Scan(&claimedID, &batchID, &status, &payload, &workspace, &attempt)

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
		ID:        claimedID,
		BatchID:   batchID,
		Status:    status,
		Payload:   payload,
		Attempt:   attempt,
		Workspace: workspace,
	}
	return claim, nil
}

func (notifier *PgNotifierT) Publish(jobs []JobPayload, priority int) (ch chan []ResponseT, err error) {
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

	stmt, err := txn.Prepare(pq.CopyIn(queueName, "batch_id", "status", "payload", "workspace", "priority"))
	if err != nil {
		return
	}
	defer stmt.Close()

	batchID := uuid.Must(uuid.NewV4()).String()
	pkgLogger.Infof("PgNotifier: Inserting %d records into %s as batch: %s", len(jobs), queueName, batchID)
	for _, job := range jobs {
		_, err = stmt.Exec(batchID, WaitingState, string(job), notifier.workspaceIdentifier, priority)
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

func (notifier *PgNotifierT) Subscribe(ctx context.Context, workerId string, jobsBufferSize int) chan ClaimT {

	jobs := make(chan ClaimT, jobsBufferSize)
	rruntime.Go(func() {
		pollSleep := time.Duration(0)
		defer close(jobs)
		for {
			claimedJob, err := notifier.claim(workerId)
			if err == nil {
				jobs <- claimedJob
				pollSleep = time.Duration(0)
			} else {
				pollSleep = 2*pollSleep + time.Duration(rand.Intn(100))*time.Millisecond
				if pollSleep > maxPollSleep {
					pollSleep = maxPollSleep
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollSleep):
			}
		}
	})
	return jobs
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

// RunMaintenanceWorker (blocking - to be called from go routine) retriggers zombie jobs
// which were left behind by dead workers in executing state
//
func (notifier *PgNotifierT) RunMaintenanceWorker(ctx context.Context) error {
	maintenanceWorkerLockID := murmur3.Sum32([]byte(queueName))
	maintenanceWorkerLock, err := pglock.NewLock(ctx, int64(maintenanceWorkerLockID), notifier.dbHandle)
	if err != nil {
		return err
	}
	for {
		locked, err := maintenanceWorkerLock.Lock(ctx)
		if err != nil {
			pkgLogger.Errorf("Received error trying to acquire maintenance worker lock %v ", err)
		}
		if locked {
			defer maintenanceWorkerLock.Unlock(ctx)
			break
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(jobOrphanTimeout / 5):
		}
	}
	for {

		stmt := fmt.Sprintf(`UPDATE %[1]s SET status='%[3]s',
 								updated_at = '%[2]s'
 								WHERE id IN (
 									SELECT id FROM %[1]s
 									WHERE status='%[4]s' AND last_exec_time <= NOW() - INTERVAL '%[5]v seconds'
 									FOR UPDATE SKIP LOCKED
 								) RETURNING id`,
			queueName,
			GetCurrentSQLTimestamp(),
			WaitingState,
			ExecutingState,
			int(jobOrphanTimeout/time.Second))
		pkgLogger.Debugf("PgNotifier: retriggering zombie jobs: %v", stmt)
		rows, err := notifier.dbHandle.Query(stmt)
		if err != nil {
			panic(err)
		}
		var ids []int64
		for rows.Next() {
			var id int64
			err := rows.Scan(&id)
			if err != nil {
				pkgLogger.Errorf("PgNotifier: Error scanning returned id from retriggered jobs: %v", err)
				continue
			}
			ids = append(ids, id)
		}
		rows.Close()
		pkgLogger.Debugf("PgNotifier: Retriggered job ids: %v", ids)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(jobOrphanTimeout / 5):
		}

	}

}
