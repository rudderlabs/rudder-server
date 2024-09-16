package testhelper

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	WaitFor2Minute          = 2 * time.Minute
	WaitFor10Minute         = 10 * time.Minute
	DefaultQueryFrequency   = 100 * time.Millisecond
	SourceJobQueryFrequency = 1000 * time.Millisecond
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
	SourceJob                    bool
	SkipWarehouse                bool
	HTTPPort                     int
}

func (w *TestConfig) VerifyEvents(t testing.TB) {
	t.Helper()

	w.reset()

	createStagingFile(t, w)

	verifyEventsInStagingFiles(t, w)
	verifyEventsInTableUploads(t, w)

	if w.SourceJob {
		verifySourceJob(t, w)
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

	t.Cleanup(func() {
		_ = jobsDB.Close()
	})

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

func UploadLoadFile(
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
	sourceID := "test_source_id"

	uploadOutput, err := fm.Upload(
		context.Background(), f, loadObjectFolder,
		tableName, sourceID, uuid.New().String()+"-"+tableName,
	)
	require.NoError(t, err)

	return uploadOutput
}

// RetrieveRecordsFromWarehouse retrieves records from the warehouse based on the given query.
// It returns a slice of slices, where each inner slice represents a record's values.
func RetrieveRecordsFromWarehouse(
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

		err = rows.Scan(resultSetPtrs...)
		require.NoError(t, err)

		records = append(records, lo.Map(resultSet, func(item any, index int) string {
			switch item := item.(type) {
			case time.Time:
				return item.Format(time.RFC3339)
			case string:
				if t, err := time.Parse(time.RFC3339Nano, item); err == nil {
					return t.Format(time.RFC3339)
				}
				return item
			default:
				return cast.ToString(item)
			}
		}))
	}
	return records
}

// SampleTestRecords returns a set of records for testing default loading scenarios.
// It uses testdata/load.* as the source of data.
func SampleTestRecords() [][]string {
	return [][]string{
		{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "126.75", "126", "hello-world"},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "125.75", "125", "hello-world"},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
	}
}

// AppendTestRecords returns a set of records for testing append scenarios.
// It uses testdata/load.* twice as the source of data.
func AppendTestRecords() [][]string {
	return [][]string{
		{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "126.75", "126", "hello-world"},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "126.75", "126", "hello-world"},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "125.75", "125", "hello-world"},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "125.75", "125", "hello-world"},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "125", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
	}
}

// DiscardTestRecords returns a set of records for testing rudder discards.
// It uses testdata/discards.* as the source of data.
func DiscardTestRecords() [][]string {
	return [][]string{
		{"context_screen_density", "125.75", "2022-12-15T06:53:49Z", "1", "test_table", "2022-12-15T06:53:49Z", "dummy reason"},
		{"context_screen_density", "125", "2022-12-15T06:53:49Z", "2", "test_table", "2022-12-15T06:53:49Z", "dummy reason"},
		{"context_screen_density", "true", "2022-12-15T06:53:49Z", "3", "test_table", "2022-12-15T06:53:49Z", "dummy reason"},
		{"context_screen_density", "7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "4", "test_table", "2022-12-15T06:53:49Z", "dummy reason"},
		{"context_screen_density", "hello-world", "2022-12-15T06:53:49Z", "5", "test_table", "2022-12-15T06:53:49Z", "dummy reason"},
		{"context_screen_density", "2022-12-15T06:53:49Z", "2022-12-15T06:53:49Z", "6", "test_table", "2022-12-15T06:53:49Z", "dummy reason"},
	}
}

// DedupTestRecords returns a set of records for testing deduplication scenarios.
// It uses testdata/dedup.* as the source of data.
func DedupTestRecords() [][]string {
	return [][]string{
		{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "521", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "75.125", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "75.125", "521", "world-hello"},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "world-hello"},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "75.125", "521", "world-hello"},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "521", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "75.125", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "world-hello"},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
	}
}

// DedupTwiceTestRecords returns a set of records for testing deduplication scenarios.
// It uses testdata/dedup.* as the source of data.
func DedupTwiceTestRecords() [][]string {
	return [][]string{
		{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "521", ""},
		{"6734e5db-f918-4efe-1421-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "521", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "75.125", "", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "75.125", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "75.125", "521", "world-hello"},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "75.125", "521", "world-hello"},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "world-hello"},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "world-hello"},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "75.125", "521", "world-hello"},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "75.125", "521", "world-hello"},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "521", ""},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "521", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "75.125", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "75.125", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "world-hello"},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "world-hello"},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
	}
}

// MismatchSchemaTestRecords returns a set of records for testing schema mismatch scenarios.
// It uses testdata/mismatch-schema.* as the source of data.
func MismatchSchemaTestRecords() [][]string {
	return [][]string{
		{"6734e5db-f918-4efe-1421-872f66e235c5", "", "", "", "", "", ""},
		{"6734e5db-f918-4efe-2314-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"6734e5db-f918-4efe-2352-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
		{"6734e5db-f918-4efe-2414-872f66e235c5", "2022-12-15T06:53:49Z", "false", "2022-12-15T06:53:49Z", "126.75", "126", "hello-world"},
		{"6734e5db-f918-4efe-3555-872f66e235c5", "2022-12-15T06:53:49Z", "false", "", "", "", ""},
		{"6734e5db-f918-4efe-5152-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"6734e5db-f918-4efe-5323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1212-872f66e235c5", "2022-12-15T06:53:49Z", "true", "2022-12-15T06:53:49Z", "125.75", "125", "hello-world"},
		{"7274e5db-f918-4efe-1454-872f66e235c5", "", "", "", "", "", ""},
		{"7274e5db-f918-4efe-1511-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", ""},
		{"7274e5db-f918-4efe-2323-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "125.75", "", ""},
		{"7274e5db-f918-4efe-4524-872f66e235c5", "2022-12-15T06:53:49Z", "true", "", "", "", ""},
		{"7274e5db-f918-4efe-5151-872f66e235c5", "2022-12-15T06:53:49Z", "", "", "", "", "hello-world"},
		{"7274e5db-f918-4efe-5322-872f66e235c5", "2022-12-15T06:53:49Z", "", "2022-12-15T06:53:49Z", "", "", ""},
	}
}

func CreateDiscardFileCSV(t testing.TB) (*os.File, error) {
	t.Helper()

	data := DiscardTestRecords()
	outfile, err := os.Create(filepath.Join(t.TempDir() + "discard.csv.gz"))
	if err != nil {
		return nil, err
	}
	defer outfile.Close()

	gzipWriter := gzip.NewWriter(outfile)
	defer gzipWriter.Close()

	csvWriter := csv.NewWriter(gzipWriter)

	for _, row := range data {
		if err := csvWriter.Write(row); err != nil {
			return nil, err
		}
	}
	csvWriter.Flush()

	if err := csvWriter.Error(); err != nil {
		return nil, err
	}
	return outfile, nil
}

func CreateDiscardFileJSON(t testing.TB) (*os.File, error) {
	t.Helper()

	data := DiscardTestRecords()
	outfile, err := os.Create(filepath.Join(t.TempDir() + "discard.json.gz"))
	if err != nil {
		return nil, err
	}
	defer outfile.Close()

	gzipWriter := gzip.NewWriter(outfile)
	defer gzipWriter.Close()

	jsonEncoder := json.NewEncoder(gzipWriter)

	if err := jsonEncoder.Encode(data); err != nil {
		return nil, err
	}

	return outfile, nil
}
