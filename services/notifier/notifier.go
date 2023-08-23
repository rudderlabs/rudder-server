package notifier

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/cenkalti/backoff"
	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-server/services/notifier/model"
	"github.com/rudderlabs/rudder-server/services/notifier/repo"

	"github.com/allisson/go-pglock/v2"
	"github.com/spaolacci/murmur3"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/misc"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
)

const (
	queueName = "pg_notifier_queue"
	module    = "pgnotifier"
)

type Notifier struct {
	conf                *config.Config
	logger              logger.Logger
	statsFactory        stats.Stats
	db                  *sqlmw.DB
	repo                *repo.Notifier
	workspaceIdentifier string
	batchIDGenerator    func() uuid.UUID
	now                 func() time.Time
	background          struct {
		group  *errgroup.Group
		ctx    context.Context
		cancel context.CancelFunc
		wait   func() error
	}

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
		claimLag           stats.Measurement
		trackBatchLag      stats.Measurement
		claimSucceeded     stats.Measurement
		claimSucceededTime stats.Measurement
		claimFailed        stats.Measurement
		claimFailedTime    stats.Measurement
		claimUpdateFailed  stats.Measurement
		abortedRecords     stats.Measurement
	}
}

func New(
	ctx context.Context,
	conf *config.Config,
	log logger.Logger,
	statsFactory stats.Stats,
	workspaceIdentifier string,
	fallbackDSN string,
) (*Notifier, error) {
	n := &Notifier{
		conf:                conf,
		logger:              log.Child("notifier"),
		statsFactory:        statsFactory,
		workspaceIdentifier: workspaceIdentifier,
		batchIDGenerator:    misc.FastUUID,
		now:                 time.Now,
	}

	n.logger.Infof("Initializing Notifier...")

	n.config.host = n.conf.GetString("PGNOTIFIER_DB_HOST", "localhost")
	n.config.user = n.conf.GetString("PGNOTIFIER_DB_USER", "ubuntu")
	n.config.database = n.conf.GetString("PGNOTIFIER_DB_NAME", "ubuntu")
	n.config.port = n.conf.GetInt("PGNOTIFIER_DB_PORT", 5432)
	n.config.password = n.conf.GetString("PGNOTIFIER_DB_PASSWORD", "ubuntu") // Reading secrets from
	n.config.sslMode = n.conf.GetString("PGNOTIFIER_DB_SSL_MODE", "disable")
	n.config.maxAttempt = n.conf.GetInt("PgNotifier.maxAttempt", 3)
	n.config.maxOpenConnections = n.conf.GetInt("PgNotifier.maxOpenConnections", 20)
	n.config.shouldForceSetLowerVersion = n.conf.GetBool("SQLMigrator.forceSetLowerVersion", true)
	n.config.trackBatchInterval = n.conf.GetDuration("PgNotifier.trackBatchIntervalInS", 2, time.Second)
	n.config.queryTimeout = n.conf.GetDuration("Warehouse.pgNotifierQueryTimeout", 5, time.Minute)

	n.conf.RegisterDurationConfigVariable(5000, &n.config.maxPollSleep, true, time.Millisecond, "PgNotifier.maxPollSleep")
	n.conf.RegisterDurationConfigVariable(120, &n.config.jobOrphanTimeout, true, time.Second, "PgNotifier.jobOrphanTimeout")

	n.stats.insertRecords = n.statsFactory.NewTaggedStat("pg_notifier_insert_records", stats.CountType, stats.Tags{
		"module":    module,
		"queueName": queueName,
	})
	n.stats.publish = n.statsFactory.NewTaggedStat("pgnotifier.publish", stats.CountType, stats.Tags{
		"module": module,
	})
	n.stats.claimSucceeded = n.statsFactory.NewTaggedStat("pgnotifier.claim", stats.CountType, stats.Tags{
		"module": module,
		"status": model.Succeeded,
	})
	n.stats.claimFailed = n.statsFactory.NewTaggedStat("pgnotifier.claim", stats.CountType, stats.Tags{
		"module": module,
		"status": model.Failed,
	})
	n.stats.claimUpdateFailed = n.statsFactory.NewStat("pgnotifier.claimUpdateFailed", stats.CountType)
	n.stats.publishTime = n.statsFactory.NewTaggedStat("pgnotifier.publishTime", stats.TimerType, stats.Tags{
		"module": module,
	})
	n.stats.claimLag = n.statsFactory.NewTaggedStat("pgnotifier.claimLag", stats.TimerType, stats.Tags{
		"module": module,
	})
	n.stats.trackBatchLag = n.statsFactory.NewTaggedStat("pgnotifier.trackBatchLag", stats.TimerType, stats.Tags{
		"module": module,
	})
	n.stats.claimSucceededTime = n.statsFactory.NewTaggedStat("pgnotifier.claimTime", stats.TimerType, stats.Tags{
		"module": module,
		"status": model.Succeeded,
	})
	n.stats.claimFailedTime = n.statsFactory.NewTaggedStat("pgnotifier.claimTime", stats.TimerType, stats.Tags{
		"module": module,
		"status": model.Failed,
	})
	n.stats.abortedRecords = n.statsFactory.NewTaggedStat("pg_notifier_aborted_records", stats.CountType, stats.Tags{
		"workspace": n.workspaceIdentifier,
		"module":    "pg_notifier",
		"queueName": queueName,
	})

	ctx, cancel := context.WithCancel(ctx)
	n.background.group, n.background.ctx = errgroup.WithContext(ctx)
	n.background.cancel = cancel
	n.background.wait = n.background.group.Wait

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

func (n *Notifier) GetDBHandle() *sql.DB {
	return n.db.DB
}

func (n *Notifier) Publish(ctx context.Context, payload *model.PublishRequest) (<-chan *model.PublishResponse, error) {
	publishStartTime := n.now()

	batchID := n.batchIDGenerator().String()

	if err := n.repo.Insert(ctx, payload, n.workspaceIdentifier, batchID); err != nil {
		return nil, fmt.Errorf("inserting jobs: %w", err)
	}

	n.logger.Infof("Inserted %d records into %s with batch length: %s", len(payload.Payloads), queueName, batchID)

	defer func() {
		n.stats.insertRecords.Count(len(payload.Payloads))
		n.stats.publishTime.Since(publishStartTime)
		n.stats.publish.Increment()
	}()

	return n.trackBatch(ctx, batchID), nil
}

// trackBatch tracks the upload batches until they are complete and triggers output through channel of type ResponseT
func (n *Notifier) trackBatch(ctx context.Context, batchID string) <-chan *model.PublishResponse {
	ch := make(chan *model.PublishResponse)

	n.background.group.Go(func() error {
		defer close(ch)

		onUpdate := func(response *model.PublishResponse) {
			select {
			case <-ctx.Done():
				return
			case <-n.background.ctx.Done():
				return
			case ch <- response:
			}
		}

		for {
			trackBatchStartTime := n.now()

			select {
			case <-ctx.Done():
				return nil
			case <-n.background.ctx.Done():
				return nil
			case <-time.After(n.config.trackBatchInterval):
			}

			count, err := n.repo.PendingByBatchID(ctx, batchID)
			if err != nil {
				onUpdate(&model.PublishResponse{
					Err: fmt.Errorf("could not get pending count for batch: %s: %w", batchID, err),
				})
				return nil
			} else if count != 0 {
				n.stats.trackBatchLag.Since(trackBatchStartTime)
				continue
			}

			notifiers, err := n.repo.GetByBatchID(ctx, batchID)
			if err != nil {
				onUpdate(&model.PublishResponse{
					Err: fmt.Errorf("could not get notifiers for batch: %s: %w", batchID, err),
				})
				return nil
			}

			_, err = n.repo.DeleteByBatchID(ctx, batchID)
			if err != nil {
				onUpdate(&model.PublishResponse{
					Err: fmt.Errorf("could not delete notifiers for batch: %s: %w", batchID, err),
				})
				return nil
			}

			n.logger.Infof("Completed processing all files in batch: %s", batchID)

			onUpdate(&model.PublishResponse{
				Notifiers: notifiers,
			})
			return nil
		}
	})

	return ch
}

func (n *Notifier) Subscribe(ctx context.Context, workerId string, bufferSize int) <-chan *model.Job {
	jobsCh := make(chan *model.Job, bufferSize)

	nextPollInterval := func(pollSleep time.Duration) time.Duration {
		pollSleep = 2*pollSleep + time.Duration(rand.Intn(100))*time.Millisecond

		if pollSleep < n.config.maxPollSleep {
			return pollSleep
		}

		return n.config.maxPollSleep
	}

	n.background.group.Go(func() error {
		defer close(jobsCh)

		pollSleep := time.Duration(0)

		for {
			claimedJob, err := n.claim(ctx, workerId)
			if err == nil {
				jobsCh <- claimedJob
				pollSleep = time.Duration(0)
			} else {
				pollSleep = nextPollInterval(pollSleep)
			}

			select {
			case <-ctx.Done():
				return nil
			case <-n.background.ctx.Done():
				return nil
			case <-time.After(pollSleep):
			}
		}
	})
	return jobsCh
}

// Claim claims a job from the notifier queue
func (n *Notifier) claim(ctx context.Context, workerID string) (*model.Job, error) {
	claimStartTime := n.now()

	claimedJob, err := n.repo.Claim(ctx, workerID)
	if err != nil {
		n.stats.claimFailedTime.Since(claimStartTime)
		n.stats.claimFailed.Increment()

		return nil, fmt.Errorf("claiming job: %w", err)
	}

	n.stats.claimLag.SendTiming(n.now().Sub(claimedJob.CreatedAt))
	n.stats.claimSucceededTime.Since(claimStartTime)
	n.stats.claimSucceeded.Increment()

	return claimedJob, nil
}

// UpdateClaim updates the notifier with the claimResponse
// In case if we are not able to update the claim, we are just logging it,
// maintenance workers can again mark the status as waiting, and it will be again claimed by somebody else.
// Although, there is a case that it is being picked up, but never getting updated. We can monitor it using claim lag.
// claim lag also helps us to make sure that even the maintenance workers are able to monitor the jobs correctly.
func (n *Notifier) UpdateClaim(ctx context.Context, notifier *model.Job, response *model.ClaimResponse) {
	if response.Err != nil {
		n.logger.Error(response.Err.Error())

		if err := n.repo.OnFailed(ctx, notifier, response.Err, n.config.maxAttempt); err != nil {
			n.stats.claimUpdateFailed.Increment()
			n.logger.Errorf("claim update failed: %v", err)
		}

		if notifier.Attempt > n.config.maxAttempt {
			n.stats.abortedRecords.Increment()
		}
		return
	}

	if err := n.repo.OnSuccess(ctx, notifier, response.Payload); err != nil {
		n.stats.claimUpdateFailed.Increment()
		n.logger.Errorf("claim update failed: %v", err)
	}
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
				n.logger.Warnf("unlocking maintenance worker lock: %v", err)
			}
		}
	}()

	for {
		if locked, err = maintenanceWorkerLock.Lock(ctx); err != nil {
			n.logger.Warnf("acquiring maintenance worker lock: %v", err)
		} else if locked {
			break
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(n.config.jobOrphanTimeout / 5):
		}
	}

	for {
		orphanJobIDs, err := n.repo.OrphanJobIDs(ctx, int(n.config.jobOrphanTimeout/time.Second))
		if errors.Is(err, context.Canceled) {
			return nil
		} else if err != nil {
			return fmt.Errorf("fetching orphan job ids: %w", err)
		}

		n.logger.Debugf("Re-triggered job ids: %v", orphanJobIDs)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(n.config.jobOrphanTimeout / 5):
		}
	}
}

func (n *Notifier) Wait(ctx context.Context) error {
	<-ctx.Done()

	n.logger.Infof("Shutting down")
	n.background.cancel()
	return n.background.group.Wait()
}
