package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	errNotImplemented         = errors.New(warehouseutils.NotImplementedErrorCode)
	errAppendingCACertificate = errors.New("appending ca certificate to pool")
)

type ClickhouseV2 struct {
	DB *sqlmw.DB

	Namespace          string
	ObjectStorage      string
	Warehouse          model.Warehouse
	Uploader           warehouseutils.Uploader
	connectTimeout     time.Duration
	LoadFileDownloader downloader.Downloader

	conf   *config.Config
	logger logger.Logger
	stats  stats.Stats

	config struct {
		queryDebugLogs              bool
		commitEvery                 int
		poolSize                    int
		readTimeout                 time.Duration
		compress                    bool
		disableNullable             bool
		numWorkersDownloadLoadFiles int
		s3EngineEnabledWorkspaceIDs []string
		slowQueryThreshold          time.Duration
		randomLoadDelay             func(string) time.Duration
		disableLoadTableStats       func(string) bool
	}
}

func NewV2(conf *config.Config, log logger.Logger, stat stats.Stats) *ClickhouseV2 {
	ch := &ClickhouseV2{}

	ch.conf = conf
	ch.logger = log.Child("integrations").Child("clickhouse").Child("v2")
	ch.stats = stat

	ch.config.queryDebugLogs = conf.GetBoolVar(false, "Warehouse.clickhouse.v2.queryDebugLogs")
	// commitEvery is the number of rows between commits, which is what bounds
	// how much of an upload is held client-side at once.
	// Floored at 1: at zero or below, the block loop never runs a single row, so
	// insertBlock returns nothing, the caller never sees a short block, and the
	// load spins forever committing empty batches. One is enough to rule that
	// out, and keeping the floor there leaves small values usable in tests.
	ch.config.commitEvery = max(conf.GetIntVar(1000000, 1, "Warehouse.clickhouse.v2.commitEvery"), 1)
	ch.config.poolSize = conf.GetIntVar(100, 1, "Warehouse.clickhouse.v2.poolSize")
	ch.config.readTimeout = conf.GetDurationVar(300, time.Second, "Warehouse.clickhouse.v2.readTimeout")
	ch.config.compress = conf.GetBoolVar(false, "Warehouse.clickhouse.v2.compress")
	ch.config.disableNullable = conf.GetBoolVar(false, "Warehouse.clickhouse.v2.disableNullable")
	ch.config.numWorkersDownloadLoadFiles = conf.GetIntVar(8, 1, "Warehouse.clickhouse.v2.numWorkersDownloadLoadFiles")
	ch.config.s3EngineEnabledWorkspaceIDs = conf.GetStringSliceVar(nil, "Warehouse.clickhouse.v2.s3EngineEnabledWorkspaceIDs")
	ch.config.slowQueryThreshold = conf.GetDurationVar(5, time.Minute, "Warehouse.clickhouse.v2.slowQueryThreshold")
	ch.config.disableLoadTableStats = func(workspaceID string) bool {
		return conf.GetBoolVar(
			false,
			fmt.Sprintf("Warehouse.clickhouse.v2.%s.disableLoadTableStats", workspaceID),
			"Warehouse.clickhouse.v2.disableLoadTableStats",
		)
	}
	ch.config.randomLoadDelay = func(workspaceID string) time.Duration {
		maxDelay := conf.GetDurationVar(
			0,
			time.Second,
			fmt.Sprintf("Warehouse.clickhouse.v2.%s.maxLoadDelay", workspaceID),
			"Warehouse.clickhouse.v2.maxLoadDelay",
		)
		return time.Duration(float64(maxDelay) * (1 - rand.Float64()))
	}

	return ch
}

func (ch *ClickhouseV2) Setup(_ context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) (err error) {
	ch.Warehouse = warehouse
	ch.Namespace = warehouse.Namespace
	ch.Uploader = uploader
	ch.ObjectStorage = warehouseutils.ObjectStorageType(warehouseutils.CLICKHOUSE, warehouse.Destination.Config, ch.Uploader.UseRudderStorage())
	ch.LoadFileDownloader = downloader.NewDownloader(&warehouse, uploader, ch.config.numWorkersDownloadLoadFiles)

	if ch.DB, err = ch.connect(true); err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	return nil
}

func (ch *ClickhouseV2) Cleanup(_ context.Context) {
	if ch.DB != nil {
		_ = ch.DB.Close()
	}
}

func (ch *ClickhouseV2) Connect(_ context.Context, warehouse model.Warehouse) (client.Client, error) {
	ch.Warehouse = warehouse
	ch.Namespace = warehouse.Namespace
	ch.ObjectStorage = warehouseutils.ObjectStorageType(
		warehouseutils.CLICKHOUSE,
		warehouse.Destination.Config,
		misc.IsConfiguredToUseRudderObjectStorage(ch.Warehouse.Destination.Config),
	)

	db, err := ch.connect(true)
	if err != nil {
		return client.Client{}, fmt.Errorf("connecting: %w", err)
	}

	return client.Client{Type: client.SQLClient, SQL: db.DB}, nil
}

// TestConnection is used destination connection tester to test the clickhouse connection
func (ch *ClickhouseV2) TestConnection(ctx context.Context, _ model.Warehouse) error {
	err := ch.DB.PingContext(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("connection timeout: %w", err)
	}
	if err != nil {
		return fmt.Errorf("pinging: %w", err)
	}

	return nil
}

func (ch *ClickhouseV2) CreateSchema(ctx context.Context) error {
	if !ch.Uploader.IsWarehouseSchemaEmpty() {
		return nil
	}

	if schemaExists, err := ch.schemaExists(ctx, ch.Namespace); err != nil {
		return fmt.Errorf("checking if database %s exists: %w", ch.Namespace, err)
	} else if schemaExists {
		return nil
	}

	db, err := ch.connect(false)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	ch.logger.Infon("Creating schema",
		logger.NewStringField("clusterClause", ch.clusterClause()),
	)

	query := fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %q %s`, ch.Namespace, ch.clusterClause())
	if _, err = db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("creating database: %w", err)
	}
	return nil
}

func (ch *ClickhouseV2) schemaExists(ctx context.Context, schemaName string) (exists bool, err error) {
	var count int64
	sqlStatement := "SELECT count(*) FROM system.databases WHERE name = ?"
	err = ch.DB.QueryRowContext(ctx, sqlStatement, schemaName).Scan(&count)
	// ignore err if no results for query
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	exists = count > 0
	return exists, err
}

func (ch *ClickhouseV2) clusterClause() string {
	if cluster := ch.Warehouse.GetStringDestinationConfig(ch.conf, model.ClusterSetting); len(strings.TrimSpace(cluster)) > 0 {
		return fmt.Sprintf(`ON CLUSTER %q`, cluster)
	}
	return ""
}

func (*ClickhouseV2) DeleteBy(context.Context, []string, warehouseutils.DeleteByParams) error {
	return errNotImplemented
}

func (*ClickhouseV2) LoadIdentityMergeRulesTable(_ context.Context) error {
	return nil
}

func (*ClickhouseV2) LoadIdentityMappingsTable(_ context.Context) error {
	return nil
}

func (*ClickhouseV2) DownloadIdentityRules(context.Context, *misc.GZipWriter) error {
	return nil
}

func (*ClickhouseV2) IsEmpty(_ context.Context, _ model.Warehouse) (bool, error) {
	return false, nil
}

func (ch *ClickhouseV2) SetConnectionTimeout(timeout time.Duration) {
	ch.connectTimeout = timeout
}

func (*ClickhouseV2) ErrorMappings() []model.JobError {
	return errorsMappings
}
