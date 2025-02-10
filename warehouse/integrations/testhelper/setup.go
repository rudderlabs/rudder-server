package testhelper

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/spf13/cast"

	"github.com/rudderlabs/rudder-go-kit/filemanager"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
)

const (
	WaitFor2Minute                 = 2 * time.Minute
	WaitFor10Minute                = 10 * time.Minute
	DefaultQueryFrequency          = 100 * time.Millisecond
	DefaultWarehouseQueryFrequency = 500 * time.Millisecond
	SourceJobQueryFrequency        = 1000 * time.Millisecond
)

const (
	jobsDBHost     = "localhost"
	jobsDBDatabase = "jobsdb"
	jobsDBUser     = "rudder"
	jobsDBPassword = "password"
)

type TestConfig struct {
	WriteKey                     string
	Schema                       string
	UserID                       string
	WorkspaceID                  string
	JobRunID                     string
	TaskRunID                    string
	SourceID                     string
	Destination                  backendconfig.DestinationT
	DestinationID                string
	DestinationType              string
	Tables                       []string
	Client                       *warehouseclient.Client
	TimestampBeforeSendingEvents time.Time
	Config                       map[string]interface{}
	StagingFilePath              string
	EventsFilePath               string
	StagingFilesEventsMap        EventsCountMap
	TableUploadsEventsMap        EventsCountMap
	WarehouseEventsMap           EventsCountMap
	JobsDB                       *sql.DB
	SourceJob                    bool
	SkipWarehouse                bool
	HTTPPort                     int
	TransformerURL               string
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
		if w.SourceJob {
			if slices.Contains(whutils.IdentityEnabledWarehouses, w.DestinationType) {
				w.StagingFilesEventsMap = defaultSourcesStagingFilesWithIDResolutionEventsMap()
			} else {
				w.StagingFilesEventsMap = defaultSourcesStagingFilesEventsMap()
			}
		} else {
			if slices.Contains(whutils.IdentityEnabledWarehouses, w.DestinationType) {
				w.StagingFilesEventsMap = defaultStagingFilesWithIDResolutionEventsMap()
			} else {
				w.StagingFilesEventsMap = defaultStagingFilesEventsMap()
			}
		}
	}
	if len(w.TableUploadsEventsMap) == 0 {
		if w.SourceJob {
			w.TableUploadsEventsMap = defaultSourcesTableUploadsEventsMap()
		} else {
			w.TableUploadsEventsMap = defaultTableUploadsEventsMap(w.DestinationType)
		}
	}
	if len(w.WarehouseEventsMap) == 0 {
		if w.SourceJob {
			w.WarehouseEventsMap = defaultSourcesWarehouseEventsMap()
		} else {
			w.WarehouseEventsMap = defaultWarehouseEventsMap(w.DestinationType)
		}
	}
}

func GetUserId(provider string) string {
	return fmt.Sprintf("userId_%s_%s", strings.ToLower(provider), whutils.RandHex())
}

func RandSchema(provider string) string {
	hex := strings.ToLower(rand.String(12))
	namespace := fmt.Sprintf("test_%s_%d", hex, time.Now().Unix())
	return whutils.ToProviderCase(provider, whutils.ToSafeNamespace(provider,
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

func UploadSampleTestRecordsTemplateLoadFile(
	t testing.TB,
	fm filemanager.FileManager,
	prefixes []string,
	recordSetIndex int,
) filemanager.UploadedFile {
	t.Helper()

	tmpl, err := template.ParseFiles("../testdata/load.template")
	require.NoError(t, err)

	tempFile, err := os.CreateTemp("", "clone_*.csv.gz")
	require.NoError(t, err)

	gzipWriter := gzip.NewWriter(tempFile)
	require.NoError(t, tmpl.Execute(gzipWriter, SampleTestRecordsTemplate(recordSetIndex)))
	require.NoError(t, gzipWriter.Close())

	f, err := os.Open(tempFile.Name())
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	uploadOutput, err := fm.Upload(context.Background(), f, prefixes...)
	require.NoError(t, err)

	return uploadOutput
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

func UploadLoad(
	t testing.TB,
	fm filemanager.FileManager,
	tableName string,
	content [][]string,
) filemanager.UploadedFile {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "upload_load_*.csv.gz")
	require.NoError(t, err)
	defer func() { _ = tmpFile.Close() }()
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	gzipWriter := gzip.NewWriter(tmpFile)
	defer gzipWriter.Close()

	writer := csv.NewWriter(gzipWriter)
	defer writer.Flush()

	for _, record := range content {
		err := writer.Write(record)
		require.NoError(t, err)
	}

	// Ensure all data is written and compressed
	writer.Flush()
	err = gzipWriter.Close()
	require.NoError(t, err)

	return UploadLoadFile(t, fm, tmpFile.Name(), tableName)
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

func ConvertRecordsToSchema(input [][]string) model.Schema {
	return lo.MapValues(lo.GroupBy(input, func(row []string) string {
		return row[0]
	}), func(columns [][]string, _ string) model.TableSchema {
		return lo.SliceToMap(columns, func(col []string) (string, string) {
			return col[1], col[2]
		})
	})
}
