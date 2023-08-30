package pgnotifier

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/allisson/go-pglock/v2"
	"github.com/lib/pq"
	"github.com/spaolacci/murmur3"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/rruntime"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	whUtils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	queueName          string
	maxAttempt         int
	trackBatchInterval time.Duration
	maxPollSleep       config.Atomic[time.Duration]
	jobOrphanTimeout   config.Atomic[time.Duration]
	pkgLogger          logger.Logger
)

var (
	pgNotifierDBHost, pgNotifierDBUser, pgNotifierDBPassword, pgNotifierDBName, pgNotifierDBSSLMode          string
	pgNotifierDBPort                                                                                         int
	pgNotifierPublish, pgNotifierPublishTime                                                                 stats.Measurement
	pgNotifierClaimSucceeded, pgNotifierClaimSucceededTime, pgNotifierClaimFailed, pgNotifierClaimFailedTime stats.Measurement
	pgNotifierClaimUpdateFailed                                                                              stats.Measurement
)

const (
	WaitingState   = "waiting"
	ExecutingState = "executing"
	SucceededState = "succeeded"
	FailedState    = "failed"
	AbortedState   = "aborted"
)

const (
	AsyncJobType = "async_job"
)

func Init() {
	loadPGNotifierConfig()
	queueName = "pg_notifier_queue"
	pkgLogger = logger.NewLogger().Child("warehouse").Child("pgnotifier")
}

type PGNotifier struct {
	URI                 string
	db                  *sqlmiddleware.DB
	workspaceIdentifier string
}

type JobPayload json.RawMessage

type Response struct {
	JobID   int64
	Status  string
	Output  json.RawMessage
	Error   string
	JobType string
}

type JobsResponse struct {
	Status    string
	Output    json.RawMessage
	Error     string
	JobType   string
	JobRunID  string
	TaskRunID string
}
type Claim struct {
	ID        int64
	BatchID   string
	Status    string
	Workspace string
	Payload   json.RawMessage
	Attempt   int
	JobType   string
}

type ClaimResponse struct {
	Payload json.RawMessage
	Err     error
}

type MessagePayload struct {
	Jobs    []JobPayload
	JobType string
}

func loadPGNotifierConfig() {
	pgNotifierDBHost = config.GetString("PGNOTIFIER_DB_HOST", "localhost")
	pgNotifierDBUser = config.GetString("PGNOTIFIER_DB_USER", "ubuntu")
	pgNotifierDBName = config.GetString("PGNOTIFIER_DB_NAME", "ubuntu")
	pgNotifierDBPort = config.GetInt("PGNOTIFIER_DB_PORT", 5432)
	pgNotifierDBPassword = config.GetString("PGNOTIFIER_DB_PASSWORD", "ubuntu") // Reading secrets from
	pgNotifierDBSSLMode = config.GetString("PGNOTIFIER_DB_SSL_MODE", "disable")
	config.RegisterIntVar(3, &maxAttempt, 1, "PgNotifier.maxAttempt")
	trackBatchInterval = time.Duration(config.GetInt("PgNotifier.trackBatchIntervalInS", 2)) * time.Second
	config.RegisterAtomicDurationVar(5000, &maxPollSleep, time.Millisecond, "PgNotifier.maxPollSleep")
	config.RegisterAtomicDurationVar(120, &jobOrphanTimeout, time.Second, "PgNotifier.jobOrphanTimeout")
}

// New Given default connection info return pg notifier object from it
func New(workspaceIdentifier, fallbackConnectionInfo string) (notifier PGNotifier, err error) {
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
	dbHandle.SetMaxOpenConns(config.GetInt("PgNotifier.maxOpenConnections", 20))

	// setup metrics
	pgNotifierModuleTag := whUtils.Tag{Name: "module", Value: "pgnotifier"}
	// publish metrics
	pgNotifierPublish = whUtils.NewCounterStat("pgnotifier_publish", pgNotifierModuleTag)
	pgNotifierPublishTime = whUtils.NewTimerStat("pgnotifier_publish_time", pgNotifierModuleTag)
	// claim metrics
	pgNotifierClaimSucceeded = whUtils.NewCounterStat("pgnotifier_claim", pgNotifierModuleTag, whUtils.Tag{Name: "status", Value: "succeeded"})
	pgNotifierClaimFailed = whUtils.NewCounterStat("pgnotifier_claim", pgNotifierModuleTag, whUtils.Tag{Name: "status", Value: "failed"})
	pgNotifierClaimSucceededTime = whUtils.NewTimerStat("pgnotifier_claim_time", pgNotifierModuleTag, whUtils.Tag{Name: "status", Value: "succeeded"})
	pgNotifierClaimFailedTime = whUtils.NewTimerStat("pgnotifier_claim_time", pgNotifierModuleTag, whUtils.Tag{Name: "status", Value: "failed"})
	pgNotifierClaimUpdateFailed = whUtils.NewCounterStat("pgnotifier_claim_update_failed", pgNotifierModuleTag)

	notifier = PGNotifier{
		db: sqlmiddleware.New(
			dbHandle,
			sqlmiddleware.WithQueryTimeout(config.GetDuration("Warehouse.pgNotifierQueryTimeout", 5, time.Minute)),
		),
		URI:                 connectionInfo,
		workspaceIdentifier: workspaceIdentifier,
	}
	err = notifier.setupQueue()
	return
}

func (notifier *PGNotifier) GetDBHandle() *sql.DB {
	return notifier.db.DB
}

func (notifier *PGNotifier) ClearJobs(ctx context.Context) (err error) {
	// clean up all jobs in pgnotifier for same workspace
	// additional safety check to not delete all jobs with empty workspaceIdentifier
	if notifier.workspaceIdentifier != "" {
		stmt := fmt.Sprintf(`
			DELETE FROM
			  %s
			WHERE
			  workspace = '%s';
`,
			queueName,
			notifier.workspaceIdentifier,
		)
		pkgLogger.Infof("PgNotifier: Deleting all jobs for workspace: %s", notifier.workspaceIdentifier)
		_, err = notifier.db.ExecContext(ctx, stmt)
		if err != nil {
			return
		}
	}

	return
}

// CheckForPGNotifierEnvVars Checks if all the required Env Variables for PG Notifier are present
func CheckForPGNotifierEnvVars() bool {
	return config.IsSet("PGNOTIFIER_DB_HOST") &&
		config.IsSet("PGNOTIFIER_DB_USER") &&
		config.IsSet("PGNOTIFIER_DB_NAME") &&
		config.IsSet("PGNOTIFIER_DB_PASSWORD")
}

// GetPGNotifierConnectionString Returns PG Notifier DB Connection Configuration
func GetPGNotifierConnectionString() string {
	pkgLogger.Debugf("WH: All Env variables required for separate PG Notifier are set... Check pg notifier says True...")
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s",
		pgNotifierDBHost, pgNotifierDBPort, pgNotifierDBUser,
		pgNotifierDBPassword, pgNotifierDBName, pgNotifierDBSSLMode)
}

// trackUploadBatch tracks the upload batches until they are complete and triggers output through channel of type ResponseT
func (notifier *PGNotifier) trackUploadBatch(ctx context.Context, batchID string, ch *chan []Response) {
	rruntime.GoForWarehouse(func() {
		for {
			time.Sleep(trackBatchInterval)
			// keep polling db for batch status
			// or subscribe to triggers
			stmt := fmt.Sprintf(`
				SELECT
				  count(*)
				FROM
				  %s
				WHERE
				  batch_id = '%s'
				  AND status != '%s'
				  AND status != '%s';
`,
				queueName,
				batchID,
				SucceededState,
				AbortedState,
			)
			var count int
			err := notifier.db.QueryRowContext(ctx, stmt).Scan(&count)
			if err != nil {
				pkgLogger.Errorf("PgNotifier: Failed to query for tracking jobs by batch_id: %s, connInfo: %s", stmt, notifier.URI)
				panic(err)
			}

			if count == 0 {
				stmt = fmt.Sprintf(`
					SELECT
					  payload -> 'StagingFileID',
					  payload -> 'Output',
					  status,
					  error
					FROM
					  %s
					WHERE
					  batch_id = '%s';
`,
					queueName,
					batchID,
				)
				rows, err := notifier.db.QueryContext(ctx, stmt)
				if err != nil {
					panic(err)
				}
				var responses []Response
				for rows.Next() {
					var status, jobError, output sql.NullString
					var jobID int64
					err = rows.Scan(&jobID, &output, &status, &jobError)
					if err != nil {
						panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", stmt, err))
					}
					responses = append(responses, Response{
						JobID:  jobID,
						Output: []byte(output.String),
						Status: status.String,
						Error:  jobError.String,
					})
				}
				if rows.Err() != nil {
					panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", stmt, rows.Err()))
				}
				_ = rows.Close()
				*ch <- responses
				pkgLogger.Infof("PgNotifier: Completed processing all files  in batch: %s", batchID)
				stmt = fmt.Sprintf(`
					DELETE FROM
					  %s
					WHERE
					  batch_id = '%s';
`,
					queueName,
					batchID,
				)
				_, err = notifier.db.ExecContext(ctx, stmt)
				if err != nil {
					pkgLogger.Errorf("PgNotifier: Error deleting from %s for batch_id:%s : %v", queueName, batchID, err)
				}
				break
			}
			pkgLogger.Debugf("PgNotifier: Pending %d files to process in batch: %s", count, batchID)
		}
	})
}

// trackAsyncBatch tracks the upload batches until they are complete and triggers output through channel of type ResponseT
func (notifier *PGNotifier) trackAsyncBatch(ctx context.Context, batchID string, ch *chan []Response) {
	rruntime.GoForWarehouse(func() {
		// retry := 0
		var responses []Response
		for {
			time.Sleep(trackBatchInterval)
			// keep polling db for batch status
			// or subscribe to triggers
			stmt := fmt.Sprintf(`SELECT count(*) FROM %s WHERE batch_id=$1 AND status!=$2 AND status!=$3`, queueName)
			var count int
			err := notifier.db.QueryRowContext(ctx, stmt, batchID, SucceededState, AbortedState).Scan(&count)
			if err != nil {
				*ch <- responses
				pkgLogger.Errorf("PgNotifier: Failed to query for tracking jobs by batch_id: %s, connInfo: %s, error : %s", stmt, notifier.URI, err.Error())
				break
			}

			if count == 0 {
				stmt = fmt.Sprintf(`SELECT payload, status, error FROM %s WHERE batch_id = $1`, queueName)
				rows, err := notifier.db.QueryContext(ctx, stmt, batchID)
				if err != nil {
					*ch <- responses
					pkgLogger.Errorf("PgNotifier: Failed to query for getting jobs for payload, status & error: %s, connInfo: %s, error : %s", stmt, notifier.URI, err.Error())
					break
				}
				for rows.Next() {
					var status, jobError sql.NullString
					var payload json.RawMessage
					err = rows.Scan(&payload, &status, &jobError)
					if err != nil {
						continue
					}
					responses = append(responses, Response{
						JobID:  0, // Not required for this as there is no concept of BatchFileId
						Output: payload,
						Status: status.String,
						Error:  jobError.String,
					})
				}
				if err := rows.Err(); err != nil {
					*ch <- responses
					pkgLogger.Errorf("PgNotifier: Failed to query for getting jobs for payload with rows error, status & error: %s, connInfo: %s, error : %v", stmt, notifier.URI, err)
					break
				}
				_ = rows.Close()
				*ch <- responses
				pkgLogger.Infof("PgNotifier: Completed processing asyncjobs in batch: %s", batchID)
				stmt = fmt.Sprintf(`DELETE FROM %s WHERE batch_id = $1`, queueName)
				pkgLogger.Infof("Query for deleting pgnotifier rows is %s for batchId : %s in queueName: %s", stmt, batchID, queueName)
				_, err = notifier.db.ExecContext(ctx, stmt, batchID)
				if err != nil {
					pkgLogger.Errorf("PgNotifier: Error deleting from %s for batch_id:%s : %v", queueName, batchID, err)
				}
				break
			}
			pkgLogger.Debugf("PgNotifier: Pending %d files to process in batch: %s", count, batchID)
		}
	})
}

func (notifier *PGNotifier) UpdateClaimedEvent(claim *Claim, response *ClaimResponse) {
	var err error
	if response.Err != nil {
		pkgLogger.Error(response.Err.Error())
		stmt := fmt.Sprintf(`
			UPDATE
			  %[1]s
			SET
			  status =(
				CASE WHEN attempt > %[2]d THEN CAST (
				  '%[3]s' AS pg_notifier_status_type
				) ELSE CAST(
				  '%[4]s' AS pg_notifier_status_type
				) END
			  ),
			  attempt = attempt + 1,
			  updated_at = '%[5]s',
			  error = %[6]s
			WHERE
			  id = %[7]v;
`,
			queueName,
			maxAttempt,
			AbortedState,
			FailedState,
			GetCurrentSQLTimestamp(),
			misc.QuoteLiteral(response.Err.Error()),
			claim.ID,
		)
		_, err = notifier.db.Exec(stmt)

		// Sending stats when we mark pg_notifier status as aborted.
		if claim.Attempt > maxAttempt {
			stats.Default.NewTaggedStat("pg_notifier_aborted_records", stats.CountType, map[string]string{
				"queueName": queueName,
				"workspace": claim.Workspace,
				"module":    "pg_notifier",
			}).Increment()
		}
	} else {
		stmt := fmt.Sprintf(`
			UPDATE
			  %[1]s
			SET
			  status = '%[2]s',
			  updated_at = '%[3]s',
			  payload = $1
			WHERE
			  id = %[4]v;
`,
			queueName,
			SucceededState,
			GetCurrentSQLTimestamp(),
			claim.ID,
		)
		_, err = notifier.db.Exec(stmt, response.Payload)
	}

	if err != nil {
		pgNotifierClaimUpdateFailed.Increment()
		pkgLogger.Errorf("PgNotifier: Failed to update claimed event: %v", err)
	}
}

func (notifier *PGNotifier) claim(workerID string) (claim Claim, err error) {
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
	var jobType sql.NullString
	var payload json.RawMessage
	stmt := fmt.Sprintf(`
		UPDATE
		  %[1]s
		SET
		  status = '%[2]s',
		  updated_at = '%[3]s',
		  last_exec_time = '%[3]s',
		  worker_id = '%[4]v'
		WHERE
		  id = (
			SELECT
			  id
			FROM
			  %[1]s
			WHERE
			  status = '%[5]s'
			  OR status = '%[6]s'
			ORDER BY
			  priority ASC,
			  id ASC FOR
			UPDATE
			  SKIP LOCKED
			LIMIT
			  1
		  ) RETURNING id,
		  batch_id,
		  status,
		  payload,
		  workspace,
		  attempt,
		  job_type;
`,
		queueName,
		ExecutingState,
		GetCurrentSQLTimestamp(),
		workerID,
		WaitingState,
		FailedState,
	)

	tx, err := notifier.db.Begin()
	if err != nil {
		return
	}
	err = tx.QueryRow(stmt).Scan(&claimedID, &batchID, &status, &payload, &workspace, &attempt, &jobType)
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				err = fmt.Errorf("%v: %w", err, rollbackErr)
			}
		}
	}()
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		pkgLogger.Errorf("PgNotifier: Claim failed: %v, query: %s, connInfo: %s", err, stmt, notifier.URI)
		return
	}

	err = tx.Commit()
	if err != nil {
		pkgLogger.Errorf("PgNotifier: Error committing claim txn: %v", err)
		return
	}

	// fallback to upload if jobType is not valid
	if !jobType.Valid {
		jobType = sql.NullString{String: "upload", Valid: true}
	}

	claim = Claim{
		ID:        claimedID,
		BatchID:   batchID,
		Status:    status,
		Payload:   payload,
		Attempt:   attempt,
		Workspace: workspace,
		JobType:   jobType.String,
	}
	return claim, nil
}

func (notifier *PGNotifier) Publish(ctx context.Context, payload MessagePayload, schema *whUtils.Schema, priority int) (ch chan []Response, err error) {
	publishStartTime := time.Now()
	jobs := payload.Jobs
	defer func() {
		if err == nil {
			pgNotifierPublishTime.Since(publishStartTime)
			pgNotifierPublish.Increment()
		}
	}()

	ch = make(chan []Response)

	// Using transactions for bulk copying
	txn, err := notifier.db.Begin()
	if err != nil {
		err = fmt.Errorf("PgNotifier: Failed creating transaction for publishing with error: %w", err)
		return
	}
	defer func() {
		if err != nil {
			if rollbackErr := txn.Rollback(); rollbackErr != nil {
				pkgLogger.Errorf("PgNotifier: Failed rollback transaction for publishing with error: %s", rollbackErr.Error())
			}
		}
	}()

	stmt, err := txn.Prepare(pq.CopyIn(queueName, "batch_id", "status", "payload", "workspace", "priority", "job_type"))
	if err != nil {
		err = fmt.Errorf("PgNotifier: Failed creating prepared statement for publishing with error: %w", err)
		return
	}
	defer stmt.Close()

	batchID := misc.FastUUID().String()
	pkgLogger.Infof("PgNotifier: Inserting %d records into %s as batch: %s", len(jobs), queueName, batchID)
	for _, job := range jobs {
		_, err = stmt.ExecContext(ctx, batchID, WaitingState, string(job), notifier.workspaceIdentifier, priority, payload.JobType)
		if err != nil {
			err = fmt.Errorf("PgNotifier: Failed executing prepared statement for publishing with error: %w", err)
			return
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		err = fmt.Errorf("PgNotifier: Failed publishing prepared statement for publishing with error: %w", err)
		return
	}

	uploadSchemaJSON, err := json.Marshal(struct {
		UploadSchema whUtils.Schema
	}{
		UploadSchema: *schema,
	})
	if err != nil {
		err = fmt.Errorf("PgNotifier: Failed unmarshalling uploadschema for publishing with error: %w", err)
		return
	}

	sqlStatement := `
		UPDATE
		  pg_notifier_queue
		SET
		  payload = payload || $1
		WHERE
		  batch_id = $2;`
	_, err = txn.ExecContext(ctx, sqlStatement, uploadSchemaJSON, batchID)
	if err != nil {
		err = fmt.Errorf("PgNotifier: Failed updating uploadschema for publishing with error: %w", err)
		return
	}

	err = txn.Commit()
	if err != nil {
		err = fmt.Errorf("PgNotifier: Failed committing transaction for publishing with error: %w", err)
		return
	}

	pkgLogger.Infof("PgNotifier: Inserted %d records into %s as batch: %s", len(jobs), queueName, batchID)
	stats.Default.NewTaggedStat("pg_notifier_insert_records", stats.CountType, map[string]string{
		"queueName": queueName,
		"module":    "pg_notifier",
	}).Count(len(jobs))
	if payload.JobType == AsyncJobType {
		notifier.trackAsyncBatch(ctx, batchID, &ch)
		return
	}
	notifier.trackUploadBatch(ctx, batchID, &ch)
	return
}

func (notifier *PGNotifier) Subscribe(ctx context.Context, workerId string, jobsBufferSize int) chan Claim {
	jobs := make(chan Claim, jobsBufferSize)
	rruntime.GoForWarehouse(func() {
		pollSleep := time.Duration(0)
		defer close(jobs)
		for {
			claimedJob, err := notifier.claim(workerId)
			if err == nil {
				jobs <- claimedJob
				pollSleep = time.Duration(0)
			} else {
				pollSleep = 2*pollSleep + time.Duration(rand.Intn(100))*time.Millisecond
				if pollSleep > maxPollSleep.Load() {
					pollSleep = maxPollSleep.Load()
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

func (notifier *PGNotifier) setupQueue() (err error) {
	pkgLogger.Infof("PgNotifier: Creating Job Queue Tables ")

	m := &migrator.Migrator{
		Handle:                     notifier.GetDBHandle(),
		MigrationsTable:            "pg_notifier_queue_migrations",
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}
	err = m.Migrate("pg_notifier_queue")
	if err != nil {
		panic(fmt.Errorf("could not run pg_notifier_queue migrations: %w", err))
	}

	return
}

// GetCurrentSQLTimestamp to get sql complaint current datetime string
func GetCurrentSQLTimestamp() string {
	const SQLTimeFormat = "2006-01-02 15:04:05"
	return time.Now().Format(SQLTimeFormat)
}

// RunMaintenanceWorker (blocking - to be called from go routine) re-triggers zombie jobs
// which were left behind by dead workers in executing state
func (notifier *PGNotifier) RunMaintenanceWorker(ctx context.Context) error {
	maintenanceWorkerLockID := murmur3.Sum64([]byte(queueName))
	maintenanceWorkerLock, err := pglock.NewLock(ctx, int64(maintenanceWorkerLockID), notifier.GetDBHandle())
	if err != nil {
		return err
	}

	var locked bool
	defer func() {
		if locked {
			if err := maintenanceWorkerLock.Unlock(ctx); err != nil {
				pkgLogger.Errorf("Error while unlocking maintenance worker lock: %v", err)
			}
		}
	}()
	for {
		locked, err = maintenanceWorkerLock.Lock(ctx)
		if err != nil {
			pkgLogger.Errorf("Received error trying to acquire maintenance worker lock: %v", err)
		}
		if locked {
			break
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(jobOrphanTimeout.Load() / 5):
		}
	}
	for {
		stmt := fmt.Sprintf(`
			UPDATE
			  %[1]s
			SET
			  status = '%[3]s',
			  updated_at = '%[2]s'
			WHERE
			  id IN (
				SELECT
				  id
				FROM
				  %[1]s
				WHERE
				  status = '%[4]s'
				  AND last_exec_time <= NOW() - INTERVAL '%[5]v seconds' FOR
				UPDATE
				  SKIP LOCKED
			  ) RETURNING id;
`,
			queueName,
			GetCurrentSQLTimestamp(),
			WaitingState,
			ExecutingState,
			int(jobOrphanTimeout.Load()/time.Second),
		)
		pkgLogger.Debugf("PgNotifier: re-triggering zombie jobs: %v", stmt)
		rows, err := notifier.db.Query(stmt)
		if err != nil {
			panic(err)
		}
		var ids []int64
		for rows.Next() {
			var id int64
			err := rows.Scan(&id)
			if err != nil {
				pkgLogger.Errorf("PgNotifier: Error scanning returned id from re-triggered jobs: %v", err)
				continue
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			panic(err)
		}

		_ = rows.Close()
		pkgLogger.Debugf("PgNotifier: Re-triggered job ids: %v", ids)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(jobOrphanTimeout.Load() / 5):
		}
	}
}
