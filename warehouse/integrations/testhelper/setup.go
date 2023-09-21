package testhelper

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/spf13/cast"

	"github.com/rudderlabs/rudder-go-kit/filemanager"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/rudder-server/utils/timeutil"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
)

const (
	WaitFor2Minute         = 2 * time.Minute
	WaitFor10Minute        = 10 * time.Minute
	DefaultQueryFrequency  = 100 * time.Millisecond
	AsyncJOBQueryFrequency = 1000 * time.Millisecond
)

const (
	jobsDBHost     = "localhost"
	jobsDBDatabase = "jobsdb"
	jobsDBUser     = "rudder"
	jobsDBPassword = "password"
)

type EventsCountMap map[string]int

type TestConfig struct {
	WriteKey                     string
	Schema                       string
	UserID                       string
	WorkspaceID                  string
	JobRunID                     string
	TaskRunID                    string
	SourceID                     string
	DestinationID                string
	DestinationType              string
	Tables                       []string
	Client                       *warehouseclient.Client
	TimestampBeforeSendingEvents time.Time
	Config                       map[string]interface{}
	StagingFilePath              string
	StagingFilesEventsMap        EventsCountMap
	LoadFilesEventsMap           EventsCountMap
	TableUploadsEventsMap        EventsCountMap
	WarehouseEventsMap           EventsCountMap
	JobsDB                       *sql.DB
	AsyncJob                     bool
	SkipWarehouse                bool
	HTTPPort                     int
}

func (w *TestConfig) VerifyEvents(t testing.TB) {
	t.Helper()

	w.reset()

	createStagingFile(t, w)

	verifyEventsInStagingFiles(t, w)
	verifyEventsInLoadFiles(t, w)
	verifyEventsInTableUploads(t, w)

	if w.AsyncJob {
		verifyAsyncJob(t, w)
	}
	if !w.SkipWarehouse {
		verifyEventsInWareHouse(t, w)
	}
}

func (w *TestConfig) reset() {
	w.TimestampBeforeSendingEvents = timeutil.Now()

	if len(w.StagingFilesEventsMap) == 0 {
		w.StagingFilesEventsMap = defaultStagingFilesEventsMap()
	}
	if len(w.LoadFilesEventsMap) == 0 {
		w.LoadFilesEventsMap = defaultLoadFilesEventsMap()
	}
	if len(w.TableUploadsEventsMap) == 0 {
		w.TableUploadsEventsMap = defaultTableUploadsEventsMap()
	}
	if len(w.WarehouseEventsMap) == 0 {
		w.WarehouseEventsMap = defaultWarehouseEventsMap()
	}
}

func defaultStagingFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"wh_staging_files": 32,
	}
}

func defaultLoadFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"identifies":    4,
		"users":         4,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        4,
	}
}

func defaultTableUploadsEventsMap() EventsCountMap {
	return EventsCountMap{
		"identifies":    4,
		"users":         4,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        4,
	}
}

func defaultWarehouseEventsMap() EventsCountMap {
	return EventsCountMap{
		"identifies":    4,
		"users":         1,
		"tracks":        4,
		"product_track": 4,
		"pages":         4,
		"screens":       4,
		"aliases":       4,
		"groups":        4,
	}
}

func SourcesStagingFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"wh_staging_files": 8,
	}
}

func SourcesLoadFilesEventsMap() EventsCountMap {
	return EventsCountMap{
		"tracks":       4,
		"google_sheet": 4,
	}
}

func SourcesTableUploadsEventsMap() EventsCountMap {
	return EventsCountMap{
		"tracks":       4,
		"google_sheet": 4,
	}
}

func SourcesWarehouseEventsMap() EventsCountMap {
	return EventsCountMap{
		"google_sheet": 4,
		"tracks":       4,
	}
}

func GetUserId(provider string) string {
	return fmt.Sprintf("userId_%s_%s", strings.ToLower(provider), warehouseutils.RandHex())
}

func RandSchema(provider string) string {
	hex := strings.ToLower(rand.String(12))
	namespace := fmt.Sprintf("test_%s_%d", hex, time.Now().Unix())
	return warehouseutils.ToProviderCase(provider, warehouseutils.ToSafeNamespace(provider,
		namespace,
	))
}

func JobsDB(t testing.TB, port int) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		jobsDBUser,
		jobsDBPassword,
		jobsDBHost,
		strconv.Itoa(port),
		jobsDBDatabase,
	)
	jobsDB, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	require.NoError(t, jobsDB.Ping())

	return jobsDB
}

func WithConstantRetries(operation func() error) error {
	var err error
	for i := 0; i < 6; i++ {
		if err = operation(); err == nil {
			return nil
		}
		time.Sleep(time.Duration(1+i) * time.Second)
	}
	return err
}

func EnhanceWithDefaultEnvs(t testing.TB) {
	t.Setenv("JOBS_DB_HOST", jobsDBHost)
	t.Setenv("JOBS_DB_NAME", jobsDBDatabase)
	t.Setenv("JOBS_DB_DB_NAME", jobsDBDatabase)
	t.Setenv("JOBS_DB_USER", jobsDBUser)
	t.Setenv("JOBS_DB_PASSWORD", jobsDBPassword)
	t.Setenv("JOBS_DB_SSL_MODE", "disable")
	t.Setenv("WAREHOUSE_JOBS_DB_HOST", jobsDBHost)
	t.Setenv("WAREHOUSE_JOBS_DB_NAME", jobsDBDatabase)
	t.Setenv("WAREHOUSE_JOBS_DB_DB_NAME", jobsDBDatabase)
	t.Setenv("WAREHOUSE_JOBS_DB_USER", jobsDBUser)
	t.Setenv("WAREHOUSE_JOBS_DB_PASSWORD", jobsDBPassword)
	t.Setenv("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	t.Setenv("GO_ENV", "production")
	t.Setenv("LOG_LEVEL", "INFO")
	t.Setenv("INSTANCE_ID", "1")
	t.Setenv("ALERT_PROVIDER", "pagerduty")
	t.Setenv("CONFIG_PATH", "../../../config/config.yaml")
	t.Setenv("RSERVER_WAREHOUSE_WAREHOUSE_SYNC_FREQ_IGNORE", "true")
	t.Setenv("RSERVER_WAREHOUSE_UPLOAD_FREQ_IN_S", "10")
	t.Setenv("RSERVER_WAREHOUSE_ENABLE_JITTER_FOR_SYNCS", "false")
	t.Setenv("RSERVER_WAREHOUSE_ENABLE_IDRESOLUTION", "true")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")
	t.Setenv("RUDDER_ADMIN_PASSWORD", "password")
	t.Setenv("RUDDER_GRACEFUL_SHUTDOWN_TIMEOUT_EXIT", "false")
	t.Setenv("RSERVER_LOGGER_CONSOLE_JSON_FORMAT", "true")
	t.Setenv("RSERVER_WAREHOUSE_MODE", "master_and_slave")
	t.Setenv("RSERVER_ENABLE_STATS", "false")
	t.Setenv("RUDDER_TMPDIR", t.TempDir())
	if testing.Verbose() {
		t.Setenv("LOG_LEVEL", "DEBUG")
	}
}

func Upload(
	t testing.TB,
	fm filemanager.FileManager,
	fileName string,
	tableName string,
) filemanager.UploadedFile {
	t.Helper()

	f, err := os.Open(fileName)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	loadObjectFolder := "rudder-warehouse-load-objects"
	sourceID := "test_source-id"

	uploadOutput, err := fm.Upload(
		context.Background(), f, loadObjectFolder,
		tableName, sourceID, uuid.New().String()+"-"+tableName,
	)
	require.NoError(t, err)

	return uploadOutput
}

func RecordsFromWarehouse(
	t testing.TB,
	db *sql.DB,
	query string,
) [][]string {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), query)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	_ = rows.Err()

	columns, err := rows.Columns()
	require.NoError(t, err)

	var records [][]string
	for rows.Next() {
		resultSet := make([]any, len(columns))
		resultSetPtrs := make([]any, len(columns))
		for i := 0; i < len(columns); i++ {
			resultSetPtrs[i] = &resultSet[i]
		}
		require.NoError(t, rows.Scan(resultSetPtrs...))

		records = append(records, lo.Map(resultSet, func(item any, index int) string {
			return cast.ToString(item)
		}))
	}
	return records
}
