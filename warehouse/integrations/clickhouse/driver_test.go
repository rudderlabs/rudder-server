package clickhouse

import (
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestSqlDB(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	wrapped := sqlmw.New(db)
	require.Same(t, db, wrapped.SqlDB())
}

func TestNewDriverSelection(t *testing.T) {
	originalV1 := openClickhouseV1
	originalV2 := openClickhouseV2
	t.Cleanup(func() {
		openClickhouseV1 = originalV1
		openClickhouseV2 = originalV2
	})

	newMockDB := func(t *testing.T) *sql.DB {
		t.Helper()
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		mock.ExpectClose()
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	openClickhouseV1 = func(Config) (*sql.DB, error) { return newMockDB(t), nil }
	openClickhouseV2 = func(Config) (*sql.DB, error) { return newMockDB(t), nil }

	t.Run("default uses v1", func(t *testing.T) {
		driver, driverName, commitEvery, err := newDriver(Config{logger: logger.NOP}, model.Warehouse{}, config.New())
		require.NoError(t, err)
		require.NotNil(t, driver)
		require.Equal(t, "v1", driverName)
		require.Zero(t, commitEvery)
	})

	t.Run("workspace v2 uses block size", func(t *testing.T) {
		cfg := config.New()
		cfg.Set("Warehouse.clickhouse.workspace-1.useV2Driver", true)

		driver, driverName, commitEvery, err := newDriver(Config{blockSize: "2", logger: logger.NOP}, model.Warehouse{WorkspaceID: "workspace-1"}, cfg)
		require.NoError(t, err)
		require.NotNil(t, driver)
		require.Equal(t, "v2", driverName)
		require.Equal(t, 2, commitEvery)
	})
}

func TestConnectUsesNewDriverSelection(t *testing.T) {
	originalV1 := openClickhouseV1
	originalV2 := openClickhouseV2
	t.Cleanup(func() {
		openClickhouseV1 = originalV1
		openClickhouseV2 = originalV2
	})

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectClose()
	t.Cleanup(func() { _ = db.Close() })

	openClickhouseV1 = func(Config) (*sql.DB, error) {
		require.FailNow(t, "v1 opener should not be used")
		return nil, fmt.Errorf("v1 opener should not be used")
	}
	openClickhouseV2 = func(conf Config) (*sql.DB, error) {
		require.True(t, conf.includeDBInConn)
		return db, nil
	}

	cfg := config.New()
	cfg.Set("Warehouse.clickhouse.useV2Driver", true)
	cfg.Set("Warehouse.clickhouse.blockSize", "2")
	ch := New(cfg, logger.NOP, stats.NOP)

	client, err := ch.Connect(context.Background(), model.Warehouse{
		WorkspaceID: "workspace-1",
		Namespace:   "namespace",
		Destination: backendconfig.DestinationT{Config: map[string]any{}},
	})
	require.NoError(t, err)
	require.Equal(t, "v2", ch.driverName)
	require.Equal(t, 2, ch.commitEvery)
	require.Same(t, db, client.SQL)
}

func TestClickHouseDriverTags(t *testing.T) {
	store, err := memstats.New()
	require.NoError(t, err)

	ch := New(config.New(), logger.NOP, store)
	ch.driverName = "v2"
	ch.Warehouse = model.Warehouse{
		WorkspaceID: "workspace-1",
		Type:        warehouseutils.CLICKHOUSE,
		Identifier:  "identifier",
		Source: backendconfig.SourceT{
			ID: "source-1",
		},
		Destination: backendconfig.DestinationT{
			ID: "destination-1",
		},
	}
	ch.Namespace = "namespace"

	fields := ch.defaultLogFields()
	require.Contains(t, fields, "driver")
	require.Contains(t, fields, "v2")

	chStats := ch.newClickHouseStat("tracks")
	chStats.numRowsLoadFile.Count(1)

	measurement := store.Get("warehouse.clickhouse.numRowsLoadFile", stats.Tags{
		"workspaceId": "workspace-1",
		"destination": "destination-1",
		"destType":    warehouseutils.CLICKHOUSE,
		"source":      "source-1",
		"identifier":  "identifier",
		"tableName":   warehouseutils.TableNameForStats("tracks"),
		"driver":      "v2",
	})
	require.NotNil(t, measurement)
}

func TestLoadTablesCommitEvery(t *testing.T) {
	testCases := []struct {
		name        string
		commitEvery int
		batchSizes  []int
	}{
		{
			name:        "v1 commits once per table",
			commitEvery: 0,
			batchSizes:  []int{4},
		},
		{
			name:        "v2 commits each full batch and no empty batch for exact multiple",
			commitEvery: 2,
			batchSizes:  []int{2, 2},
		},
		{
			name:        "v2 commits final partial batch",
			commitEvery: 3,
			batchSizes:  []int{3, 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() { _ = db.Close() }()

			ch := New(config.New(), logger.NOP, stats.NOP)
			ch.DB = sqlmw.New(db)
			ch.Namespace = "namespace"
			ch.config.execTimeout = time.Second
			ch.config.commitTimeout = time.Second
			ch.Warehouse = model.Warehouse{
				Namespace: "namespace",
				Type:      warehouseutils.CLICKHOUSE,
				Source:    backendconfig.SourceT{ID: "source-1"},
				Destination: backendconfig.DestinationT{
					ID: "destination-1",
				},
			}

			insertSQL := regexp.QuoteMeta(`INSERT INTO "namespace"."tracks" ("id","received_at") VALUES (?,?)`)
			rowIDs := []string{"id-1", "id-2", "id-1", "id-2"}
			rowIndex := 0
			for _, batchSize := range tc.batchSizes {
				mock.ExpectBegin()
				expectedPrepare := mock.ExpectPrepare(insertSQL)
				for range batchSize {
					expectedPrepare.ExpectExec().WithArgs(rowIDs[rowIndex], sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
					rowIndex++
				}
				mock.ExpectCommit()
			}

			fileName := writeLoadFile(t, []string{
				"id-1,2024-01-01T00:00:00Z",
				"id-2,2024-01-02T00:00:00Z",
				"id-1,2024-01-03T00:00:00Z",
				"id-2,2024-01-04T00:00:00Z",
			})

			terr := ch.loadTablesFromFilesNamesWithRetry(
				context.Background(),
				"tracks",
				model.TableSchema{"id": "string", "received_at": "datetime"},
				[]string{fileName},
				ch.newClickHouseStat("tracks"),
				tc.commitEvery,
			)
			require.NoError(t, terr.err)
			require.False(t, terr.enableRetry)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func writeLoadFile(t *testing.T, rows []string) string {
	t.Helper()

	file, err := os.CreateTemp(t.TempDir(), "load-*.csv.gz")
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	writer := gzip.NewWriter(file)
	for _, row := range rows {
		_, err = fmt.Fprintln(writer, row)
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	return file.Name()
}
