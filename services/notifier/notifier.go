package notifier

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-server/services/notifier/model"
	"github.com/rudderlabs/rudder-server/services/notifier/repo"

	"github.com/allisson/go-pglock/v2"
	"github.com/spaolacci/murmur3"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/rruntime"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
)

const (
	waiting   = "waiting"
	executing = "executing"
	succeeded = "succeeded"
	failed    = "failed"
	aborted   = "aborted"

	queueName = "pg_notifier_queue"

	asyncJob = "async_job"
)

type PGNotifier struct {
	dsn                 string
	db                  *sqlmw.DB
	workspaceIdentifier string
}

type Notifier struct {
	conf                *config.Config
	logger              logger.Logger
	statsFactory        stats.Stats
	db                  *sqlmw.DB
	repo                *repo.Notifier
	dsn                 string
	workspaceIdentifier string
	batchIDGenerator    func() uuid.UUID

	config struct {
		host                       string
		port                       int
		user                       string
		password                   string
		database                   string
		sslMode                    string
		maxAttempt                 int
		maxOpenConnections         int
		shouldForceSetLowerVersion bool
		trackBatchInterval         time.Duration
		maxPollSleep               time.Duration
		jobOrphanTimeout           time.Duration
		queryTimeout               time.Duration
	}
	stats struct {
		insertRecords      stats.Measurement
		publish            stats.Measurement
		publishTime        stats.Measurement
		claimSucceeded     stats.Measurement
		claimSucceededTime stats.Measurement
		claimFailed        stats.Measurement
		claimFailedTime    stats.Measurement
		claimUpdateFailed  stats.Measurement
	}
}

// New Given default connection info return pg notifier object from it
func New(workspaceIdentifier, fallbackConnectionInfo string) (notifier PGNotifier, err error) {
	return
}

func NewNotifier(
	ctx context.Context,
	conf *config.Config,
	log logger.Logger,
	statsFactory stats.Stats,
	workspaceIdentifier string,
	fallbackDSN string,
) (*Notifier, error) {
	n := &Notifier{
		conf:                conf,
		logger:              log.Child("pgnotifier"),
		statsFactory:        statsFactory,
		workspaceIdentifier: workspaceIdentifier,
		batchIDGenerator:    misc.FastUUID,
	}

	n.logger.Infof("Initializing Notifier...")

	n.config.host = config.GetString("PGNOTIFIER_DB_HOST", "localhost")
	n.config.user = config.GetString("PGNOTIFIER_DB_USER", "ubuntu")
	n.config.database = config.GetString("PGNOTIFIER_DB_NAME", "ubuntu")
	n.config.port = config.GetInt("PGNOTIFIER_DB_PORT", 5432)
	n.config.password = config.GetString("PGNOTIFIER_DB_PASSWORD", "ubuntu") // Reading secrets from
	n.config.sslMode = config.GetString("PGNOTIFIER_DB_SSL_MODE", "disable")
	n.config.maxAttempt = config.GetInt("PgNotifier.maxAttempt", 3)
	n.config.maxAttempt = config.GetInt("PgNotifier.maxOpenConnections", 20)
	n.config.shouldForceSetLowerVersion = config.GetBool("SQLMigrator.forceSetLowerVersion", true)
	n.config.trackBatchInterval = config.GetDuration("PgNotifier.trackBatchIntervalInS", 2, time.Second)
	n.config.queryTimeout = config.GetDuration("Warehouse.pgNotifierQueryTimeout", 5, time.Minute)

	n.conf.RegisterDurationConfigVariable(5000, &n.config.maxPollSleep, true, time.Millisecond, "PgNotifier.maxPollSleep")
	n.conf.RegisterDurationConfigVariable(120, &n.config.jobOrphanTimeout, true, time.Second, "PgNotifier.jobOrphanTimeout")

	n.stats.insertRecords = n.statsFactory.NewTaggedStat("pg_notifier_insert_records", stats.CountType, stats.Tags{
		"module":    "pgnotifier",
		"queueName": "pg_notifier_queue",
	})
	n.stats.publish = n.statsFactory.NewTaggedStat("pgnotifier.publish", stats.CountType, stats.Tags{
		"module": "pgnotifier",
	})
	n.stats.claimSucceeded = n.statsFactory.NewTaggedStat("pgnotifier.claim", stats.CountType, stats.Tags{
		"module": "pgnotifier",
		"status": "succeeded",
	})
	n.stats.claimFailed = n.statsFactory.NewTaggedStat("pgnotifier.claim", stats.CountType, stats.Tags{
		"module": "pgnotifier",
		"status": "failed",
	})
	n.stats.claimUpdateFailed = n.statsFactory.NewStat("pgnotifier.claimUpdateFailed", stats.CountType)
	n.stats.publishTime = n.statsFactory.NewTaggedStat("pgnotifier.publishTime", stats.TimerType, stats.Tags{
		"module": "pgnotifier",
	})
	n.stats.claimSucceededTime = n.statsFactory.NewTaggedStat("pgnotifier.claimTime", stats.TimerType, stats.Tags{
		"module": "pgnotifier",
		"status": "succeeded",
	})
	n.stats.claimFailedTime = n.statsFactory.NewTaggedStat("pgnotifier.claimTime", stats.TimerType, stats.Tags{
		"module": "pgnotifier",
		"status": "failed",
	})

	dsn := fallbackDSN
	if n.checkForNotifierEnvVars() {
		dsn = n.connectionString()
	}

	if err := n.setupDB(ctx, dsn); err != nil {
		return nil, fmt.Errorf("could not setup db: %w", err)
	}

	n.repo = repo.NewNotifier(n.db)

	return n, nil
}

func (n *Notifier) checkForNotifierEnvVars() bool {
	return n.conf.IsSet("PGNOTIFIER_DB_HOST") &&
		n.conf.IsSet("PGNOTIFIER_DB_USER") &&
		n.conf.IsSet("PGNOTIFIER_DB_NAME") &&
		n.conf.IsSet("PGNOTIFIER_DB_PASSWORD")
}

func (n *Notifier) connectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		n.config.host,
		n.config.port,
		n.config.user,
		n.config.password,
		n.config.database,
		n.config.sslMode,
	)
}

func (n *Notifier) setupDB(ctx context.Context, dsn string) error {
	database, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("could not open: %w", err)
	}
	database.SetMaxOpenConns(n.config.maxOpenConnections)

	if err := database.PingContext(ctx); err != nil {
		return fmt.Errorf("could not ping: %w", err)
	}

	n.db = sqlmw.New(
		database,
		sqlmw.WithLogger(n.logger.Child("notifier-db")),
		sqlmw.WithQueryTimeout(n.config.queryTimeout),
		sqlmw.WithStats(n.statsFactory),
	)

	err = n.setupTables()
	if err != nil {
		return fmt.Errorf("could not setup tables: %w", err)
	}

	return nil
}

func (n *Notifier) setupTables() error {
	m := &migrator.Migrator{
		Handle:                     n.db.DB,
		MigrationsTable:            "pg_notifier_queue_migrations",
		ShouldForceSetLowerVersion: n.config.shouldForceSetLowerVersion,
	}

	operation := func() error {
		return m.Migrate("pg_notifier_queue")
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)

	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		n.logger.Warnf("retrying warehouse database migration in %s: %v", t, err)
	})
	if err != nil {
		return fmt.Errorf("could not migrate pg_notifier_queue: %w", err)
	}

	return nil
}

// ClearJobs deletes all jobs from notifier if workspaceIdentifier is set
func (n *Notifier) ClearJobs(ctx context.Context) error {
	if n.workspaceIdentifier == "" {
		return nil
	}

	n.logger.Infof("Deleting all jobs for workspace: %s", n.workspaceIdentifier)

	err := n.repo.ResetForWorkspace(ctx, n.workspaceIdentifier)
	if err != nil {
		return fmt.Errorf("could not reset notifier for workspace: %s: %w", n.workspaceIdentifier, err)
	}
	return nil
}

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
	Jobs    []model.Payload
	JobType string
}

func (n *Notifier) GetDBHandle() *sql.DB {
	return n.db.DB
}

// trackUploadBatch tracks the upload batches until they are complete and triggers output through channel of type ResponseT
func (n *Notifier) trackUploadBatch(ctx context.Context, batchID string, ch *chan []Response) {
	rruntime.GoForWarehouse(func() {
		for {
			time.Sleep(n.config.trackBatchInterval)
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
				succeeded,
				aborted,
			)
			var count int
			err := n.db.QueryRowContext(ctx, stmt).Scan(&count)
			if err != nil {
				n.logger.Errorf("PgNotifier: Failed to query for tracking jobs by batch_id: %s, connInfo: %s", stmt, n.dsn)
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
				rows, err := n.db.QueryContext(ctx, stmt)
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
				n.logger.Infof("PgNotifier: Completed processing all files  in batch: %s", batchID)
				stmt = fmt.Sprintf(`
					DELETE FROM
					  %s
					WHERE
					  batch_id = '%s';
`,
					queueName,
					batchID,
				)
				_, err = n.db.ExecContext(ctx, stmt)
				if err != nil {
					n.logger.Errorf("PgNotifier: Error deleting from %s for batch_id:%s : %v", queueName, batchID, err)
				}
				break
			}
			n.logger.Debugf("PgNotifier: Pending %d files to process in batch: %s", count, batchID)
		}
	})
}

// trackAsyncBatch tracks the upload batches until they are complete and triggers output through channel of type ResponseT
func (n *Notifier) trackAsyncBatch(ctx context.Context, batchID string, ch *chan []Response) {
	rruntime.GoForWarehouse(func() {
		// retry := 0
		var responses []Response
		for {
			time.Sleep(n.config.trackBatchInterval)
			// keep polling db for batch status
			// or subscribe to triggers
			stmt := fmt.Sprintf(`SELECT count(*) FROM %s WHERE batch_id=$1 AND status!=$2 AND status!=$3`, queueName)
			var count int
			err := n.db.QueryRowContext(ctx, stmt, batchID, succeeded, aborted).Scan(&count)
			if err != nil {
				*ch <- responses
				n.logger.Errorf("PgNotifier: Failed to query for tracking jobs by batch_id: %s, connInfo: %s, error : %s", stmt, n.dsn, err.Error())
				break
			}

			if count == 0 {
				stmt = fmt.Sprintf(`SELECT payload, status, error FROM %s WHERE batch_id = $1`, queueName)
				rows, err := n.db.QueryContext(ctx, stmt, batchID)
				if err != nil {
					*ch <- responses
					n.logger.Errorf("PgNotifier: Failed to query for getting jobs for payload, status & error: %s, connInfo: %s, error : %s", stmt, n.dsn, err.Error())
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
					n.logger.Errorf("PgNotifier: Failed to query for getting jobs for payload with rows error, status & error: %s, connInfo: %s, error : %v", stmt, n.dsn, err)
					break
				}
				_ = rows.Close()
				*ch <- responses
				n.logger.Infof("PgNotifier: Completed processing asyncjobs in batch: %s", batchID)
				stmt = fmt.Sprintf(`DELETE FROM %s WHERE batch_id = $1`, queueName)
				n.logger.Infof("Query for deleting pgnotifier rows is %s for batchId : %s in queueName: %s", stmt, batchID, queueName)
				_, err = n.db.ExecContext(ctx, stmt, batchID)
				if err != nil {
					n.logger.Errorf("PgNotifier: Error deleting from %s for batch_id:%s : %v", queueName, batchID, err)
				}
				break
			}
			n.logger.Debugf("PgNotifier: Pending %d files to process in batch: %s", count, batchID)
		}
	})
}

func (n *Notifier) UpdateClaimedEvent(claim *Claim, response *ClaimResponse) {
	var err error
	if response.Err != nil {
		n.logger.Error(response.Err.Error())
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
			n.config.maxAttempt,
			aborted,
			failed,
			GetCurrentSQLTimestamp(),
			misc.QuoteLiteral(response.Err.Error()),
			claim.ID,
		)
		_, err = n.db.Exec(stmt)

		// Sending stats when we mark pg_notifier status as aborted.
		if claim.Attempt > n.config.maxAttempt {
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
			succeeded,
			GetCurrentSQLTimestamp(),
			claim.ID,
		)
		_, err = n.db.Exec(stmt, response.Payload)
	}

	if err != nil {
		n.stats.claimUpdateFailed.Increment()
		n.logger.Errorf("PgNotifier: Failed to update claimed event: %v", err)
	}
}

func (n *Notifier) claim(ctx context.Context, workerID string) (*model.Notifier, error) {
	claimStartTime := time.Now()

	claimedJob, err := n.repo.Claim(ctx, workerID)
	if err != nil {
		n.stats.claimFailedTime.Since(claimStartTime)
		n.stats.claimFailedTime.Increment()

		return nil, fmt.Errorf("claiming job: %w", err)
	}

	n.stats.claimSucceededTime.Since(claimStartTime)
	n.stats.claimSucceeded.Increment()

	return claimedJob, nil
}

func (n *Notifier) Publish(ctx context.Context, payload *model.PublishPayload) (ch chan []Response, err error) {
	publishStartTime := time.Now()

	defer func() {
		if err == nil {
			n.stats.publishTime.Since(publishStartTime)
			n.stats.publish.Increment()
		}
	}()

	ch = make(chan []Response)

	batchID := n.batchIDGenerator().String()

	if err := n.repo.Insert(ctx, payload, n.workspaceIdentifier, batchID); err != nil {
		return nil, fmt.Errorf("inserting jobs: %w", err)
	}

	n.logger.Infof("Inserted %d records into %s with batch length: %s", len(payload.Jobs), queueName, batchID)

	n.stats.insertRecords.Count(len(payload.Jobs))

	switch payload.Type {
	case asyncJob:
		n.trackAsyncBatch(ctx, batchID, &ch)
	default:
		n.trackUploadBatch(ctx, batchID, &ch)
	}
	return
}

func (n *Notifier) Subscribe(ctx context.Context, workerId string, jobsBufferSize int) chan *model.Notifier {
	jobsCh := make(chan *model.Notifier, jobsBufferSize)

	nextPollInterval := func(pollSleep time.Duration) time.Duration {
		pollSleep = 2*pollSleep + time.Duration(rand.Intn(100))*time.Millisecond

		if pollSleep < n.config.maxPollSleep {
			return pollSleep
		}

		return n.config.maxPollSleep
	}

	rruntime.GoForWarehouse(func() {
		defer close(jobsCh)

		pollSleep := time.Duration(0)

		for {
			claimedJob, err := n.claim(ctx, workerId)
			if err != nil {
				pollSleep = nextPollInterval(pollSleep)
				continue
			}

			jobsCh <- claimedJob
			pollSleep = time.Duration(0)

			select {
			case <-ctx.Done():
				return
			case <-time.After(pollSleep):
			}
		}
	})
	return jobsCh
}

// GetCurrentSQLTimestamp to get sql complaint current datetime string
func GetCurrentSQLTimestamp() string {
	const SQLTimeFormat = "2006-01-02 15:04:05"
	return time.Now().Format(SQLTimeFormat)
}

// RunMaintenanceWorker re-triggers zombie jobs which were left behind by dead workers in executing state
// Since it's a blocking call, it should be run in a separate goroutine
func (n *Notifier) RunMaintenanceWorker(ctx context.Context) error {
	maintenanceWorkerLockID := murmur3.Sum64([]byte(queueName))
	maintenanceWorkerLock, err := pglock.NewLock(ctx, int64(maintenanceWorkerLockID), n.db.DB)
	if err != nil {
		return fmt.Errorf("creating maintenance worker lock: %w", err)
	}

	var locked bool
	defer func() {
		if locked {
			if err := maintenanceWorkerLock.Unlock(ctx); err != nil {
				n.logger.Errorf("unlocking maintenance worker lock: %v", err)
			}
		}
	}()

	wait := func() {
		select {
		case <-ctx.Done():
		case <-time.After(n.config.jobOrphanTimeout / 5):
		}
	}

	for {
		if locked, err = maintenanceWorkerLock.Lock(ctx); err != nil {
			n.logger.Errorf("acquiring maintenance worker lock: %v", err)
		} else if locked {
			break
		}

		wait()
	}

	for {
		orphanJobIDs, err := n.repo.OrphanJobIDs(ctx)
		if err != nil {
			return fmt.Errorf("fetching orphan job ids: %w", err)
		}

		n.logger.Debugf("PgNotifier: Re-triggered job ids: %v", orphanJobIDs)

		wait()
	}
}
