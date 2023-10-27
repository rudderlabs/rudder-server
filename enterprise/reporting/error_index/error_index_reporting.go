package error_index

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/workerpool"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
)

type ErrorIndexReporter struct {
	ctx              context.Context
	cancel           context.CancelFunc
	g                *errgroup.Group
	log              logger.Logger
	conf             *config.Config
	configSubscriber configSubscriber
	now              func() time.Time
	dbsMu            sync.RWMutex
	dbs              map[string]*handleWithSqlDB

	trigger func() <-chan time.Time

	statsFactory stats.Stats
	stats        struct {
		partitionTime stats.Timer
		partitions    stats.Gauge
	}
}

type handleWithSqlDB struct {
	*jobsdb.Handle
	sqlDB *sql.DB
}

func NewErrorIndexReporter(ctx context.Context, log logger.Logger, configSubscriber configSubscriber, conf *config.Config, statsFactory stats.Stats) *ErrorIndexReporter {
	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	eir := &ErrorIndexReporter{
		ctx:          ctx,
		cancel:       cancel,
		g:            g,
		log:          log.Child("error-index-reporter"),
		conf:         conf,
		statsFactory: statsFactory,

		configSubscriber: configSubscriber,
		now:              time.Now,
		dbs:              map[string]*handleWithSqlDB{},
	}

	eir.trigger = func() <-chan time.Time {
		return time.After(conf.GetDuration("Reporting.errorIndexReporting.SleepDuration", 30, time.Second))
	}

	eir.stats.partitionTime = eir.statsFactory.NewStat("erridx_partition_time", stats.TimerType)
	eir.stats.partitions = eir.statsFactory.NewStat("erridx_active_partitions", stats.GaugeType)

	return eir
}

// Report reports the metrics to the errorIndex JobsDB
func (eir *ErrorIndexReporter) Report(metrics []*types.PUReportedMetric, tx *Tx) error {
	failedAt := eir.now()

	var jobs []*jobsdb.JobT
	for _, metric := range metrics {
		if metric.StatusDetail == nil {
			continue
		}

		for _, failedMessage := range metric.StatusDetail.FailedMessages {
			workspaceID := eir.configSubscriber.WorkspaceIDFromSource(metric.SourceID)

			payload := payload{
				MessageID:        failedMessage.MessageID,
				SourceID:         metric.SourceID,
				DestinationID:    metric.DestinationID,
				TransformationID: metric.TransformationID,
				TrackingPlanID:   metric.TrackingPlanID,
				FailedStage:      metric.PUDetails.PU,
				EventName:        metric.StatusDetail.EventName,
				EventType:        metric.StatusDetail.EventType,
			}
			payload.SetReceivedAt(failedMessage.ReceivedAt)
			payload.SetFailedAt(failedAt)

			payloadJSON, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("unable to marshal payload: %v", err)
			}

			params := struct {
				WorkspaceID string `json:"workspaceId"`
				SourceID    string `json:"source_id"`
			}{
				WorkspaceID: workspaceID,
				SourceID:    metric.SourceID,
			}
			paramsJSON, err := json.Marshal(params)
			if err != nil {
				return fmt.Errorf("unable to marshal params: %v", err)
			}

			jobs = append(jobs, &jobsdb.JobT{
				UUID:         uuid.New(),
				Parameters:   paramsJSON,
				EventPayload: payloadJSON,
				EventCount:   1,
				WorkspaceId:  workspaceID,
			})
		}
	}

	if len(jobs) == 0 {
		return nil
	}
	db, err := eir.resolveJobsDB(tx)
	if err != nil {
		return fmt.Errorf("failed to resolve jobsdb: %w", err)
	}
	if err := db.WithStoreSafeTxFromTx(eir.ctx, tx, func(tx jobsdb.StoreSafeTx) error {
		return db.StoreInTx(eir.ctx, tx, jobs)
	}); err != nil {
		return fmt.Errorf("failed to store jobs: %w", err)
	}

	return nil
}

func (eir *ErrorIndexReporter) DatabaseSyncer(c types.SyncerConfig) types.ReportingSyncer {
	eir.dbsMu.Lock()
	defer eir.dbsMu.Unlock()

	if _, ok := eir.dbs[c.ConnInfo]; ok {
		return func() {} // returning a no-op syncer since another go routine has already started syncing
	}

	dbHandle, err := sql.Open("postgres", c.ConnInfo)
	if err != nil {
		panic(fmt.Errorf("failed to open error index db: %w", err))
	}
	errIndexDB := jobsdb.NewForReadWrite(
		"err_idx",
		jobsdb.WithDBHandle(dbHandle),
		jobsdb.WithDSLimit(eir.conf.GetReloadableIntVar(0, 1, "Reporting.errorIndexReporting.dsLimit")),
		jobsdb.WithConfig(eir.conf),
		jobsdb.WithSkipMaintenanceErr(eir.conf.GetBool("Reporting.errorIndexReporting.skipMaintenanceError", false)),
		jobsdb.WithJobMaxAge(
			func() time.Duration {
				return eir.conf.GetDurationVar(24, time.Hour, "Reporting.errorIndexReporting.jobRetention")
			},
		),
	)
	if err := errIndexDB.Start(); err != nil {
		panic(fmt.Errorf("failed to start error index db: %w", err))
	}
	eir.dbs[c.ConnInfo] = &handleWithSqlDB{
		Handle: errIndexDB,
		sqlDB:  dbHandle,
	}

	if !eir.conf.GetBool("Reporting.errorIndexReporting.syncer.enabled", true) {
		return func() {
			<-eir.ctx.Done()
			errIndexDB.Stop()
		}
	}

	return func() {
		eir.g.Go(func() error {
			defer errIndexDB.Stop()
			return eir.mainLoop(eir.ctx, errIndexDB)
		})
	}
}

func (eir *ErrorIndexReporter) mainLoop(ctx context.Context, errIndexDB *jobsdb.Handle) error {
	eir.log.Infow("Starting main loop for error index reporting")

	var (
		bucket           = eir.conf.GetStringVar("rudder-failed-messages", "ErrorIndex.storage.Bucket")
		regionHint       = eir.conf.GetStringVar("us-east-1", "ErrorIndex.storage.RegionHint", "AWS_S3_REGION_HINT")
		endpoint         = eir.conf.GetStringVar("", "ErrorIndex.storage.Endpoint")
		accessKeyID      = eir.conf.GetStringVar("", "ErrorIndex.storage.AccessKey", "AWS_ACCESS_KEY_ID")
		secretAccessKey  = eir.conf.GetStringVar("", "ErrorIndex.storage.SecretAccessKey", "AWS_SECRET_ACCESS_KEY")
		s3ForcePathStyle = eir.conf.GetBoolVar(false, "ErrorIndex.storage.S3ForcePathStyle")
		disableSSL       = eir.conf.GetBoolVar(false, "ErrorIndex.storage.DisableSSL")
		enableSSE        = eir.conf.GetBoolVar(false, "ErrorIndex.storage.EnableSSE", "AWS_ENABLE_SSE")
	)

	s3Config := map[string]interface{}{
		"bucketName":       bucket,
		"regionHint":       regionHint,
		"endpoint":         endpoint,
		"accessKeyID":      accessKeyID,
		"secretAccessKey":  secretAccessKey,
		"s3ForcePathStyle": s3ForcePathStyle,
		"disableSSL":       disableSSL,
		"enableSSE":        enableSSE,
	}
	fm, err := filemanager.NewS3Manager(s3Config, eir.log, func() time.Duration {
		return eir.conf.GetDuration("ErrorIndex.Uploader.Timeout", 120, time.Second)
	})
	if err != nil {
		return fmt.Errorf("creating file manager: %w", err)
	}

	workerPool := workerpool.New(
		ctx,
		func(sourceID string) workerpool.Worker {
			return newWorker(
				sourceID,
				eir.conf,
				eir.log,
				eir.statsFactory,
				errIndexDB,
				eir.configSubscriber,
				fm,
			)
		},
		eir.log,
		workerpool.WithIdleTimeout(2*eir.conf.GetDuration("Reporting.errorIndexReporting.uploadFrequency", 5, time.Minute)),
	)
	defer workerPool.Shutdown()

	for {
		start := time.Now()
		sources, err := errIndexDB.GetDistinctParameterValues(ctx, "source_id")
		if err != nil && ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("getting distinct parameter values: %w", err)
		}

		eir.stats.partitionTime.Since(start)
		eir.stats.partitions.Gauge(len(sources))

		for _, source := range sources {
			workerPool.PingWorker(source)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-eir.trigger():
		}
	}
}

func (eir *ErrorIndexReporter) Stop() {
	eir.cancel()
	_ = eir.g.Wait()
}

// resolveJobsDB returns the jobsdb that matches the current transaction (using system information functions)
// https://www.postgresql.org/docs/11/functions-info.html
func (eir *ErrorIndexReporter) resolveJobsDB(tx *Tx) (jobsdb.JobsDB, error) {
	eir.dbsMu.RLock()
	defer eir.dbsMu.RUnlock()

	if len(eir.dbs) == 1 { // optimisation, if there is only one jobsdb, return this. If it is the wrong one, it will fail anyway
		for i := range eir.dbs {
			return eir.dbs[i].Handle, nil
		}
	}

	dbIdentityQuery := `select inet_server_addr()::text || ':' || inet_server_port()::text || ':' || current_user || ':' || current_database() || ':' || current_schema || ':' || pg_postmaster_start_time()::text || ':' || version()`
	var txDatabaseIdentity string
	if err := tx.QueryRow(dbIdentityQuery).Scan(&txDatabaseIdentity); err != nil {
		return nil, fmt.Errorf("failed to get current tx's db identity: %w", err)
	}

	for key := range eir.dbs {
		var databaseIdentity string
		if err := eir.dbs[key].sqlDB.QueryRow(dbIdentityQuery).Scan(&databaseIdentity); err != nil {
			return nil, fmt.Errorf("failed to get db identity for %q: %w", key, err)
		}
		if databaseIdentity == txDatabaseIdentity {
			return eir.dbs[key].Handle, nil
		}
	}
	return nil, fmt.Errorf("no jobsdb found matching the current transaction")
}
