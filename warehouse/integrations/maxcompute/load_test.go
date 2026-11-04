package maxcompute

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func testManager() *MaxCompute {
	o := odps.NewOdps(nil, "https://service.cn-hangzhou.maxcompute.aliyun.com/api")
	o.SetDefaultProjectName("project")
	m := &MaxCompute{odps: o, warehouse: model.Warehouse{WorkspaceID: "workspace"}, conf: config.New()}
	m.warehouse.Destination.Config = map[string]any{"bucketName": "my-bucket", "region": "cn-hangzhou", "accessKeyId": "id", "accessKeySecret": "secret", "ossRoleArn": "read-role", "endpoint": "https://service.cn-hangzhou.maxcompute.aliyun.com/api", "skipViews": true}
	return m
}

func TestOSSLoadChecksMetadataBeforeSQL(t *testing.T) {
	m := testManager()
	original := metadataSize
	t.Cleanup(func() { metadataSize = original })
	location := "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/path/file.gz"
	called := 0
	m.exec = func(_ context.Context, sql string) error {
		called++
		require.Contains(t, sql, "LOAD DATA INPATH 'oss://my-bucket/path/file.gz'")
		return nil
	}
	metadataSize = func(_ context.Context, _ *config.Config, conf map[string]any, key string) (int64, error) {
		require.Equal(t, "path/file.gz", key)
		require.Equal(t, "my-bucket", conf["bucketName"])
		return maxOSSLoadBytes + 1, nil
	}
	_, err := m.loadTable(context.Background(), "events", location)
	require.ErrorContains(t, err, "3GB")
	require.Zero(t, called)
	metadataSize = func(context.Context, *config.Config, map[string]any, string) (int64, error) {
		return 0, errors.New("unavailable")
	}
	_, err = m.loadTable(context.Background(), "events", location)
	require.Error(t, err)
	require.Zero(t, called)
	metadataSize = func(context.Context, *config.Config, map[string]any, string) (int64, error) { return 0, nil }
	_, err = m.loadTable(context.Background(), "events", location)
	require.ErrorContains(t, err, "length")
	require.Zero(t, called)
	metadataSize = func(context.Context, *config.Config, map[string]any, string) (int64, error) {
		return maxOSSLoadBytes, nil
	}
	stats, err := m.loadTable(context.Background(), "events", location)
	require.NoError(t, err)
	require.EqualValues(t, 0, stats.RowsInserted) // test SQL executor has no task summary
	require.Equal(t, 1, called)
}

type loadUploader struct {
	warehouseutils.Uploader
	files []warehouseutils.LoadFile
}

func (u loadUploader) GetLoadFileType() string { return warehouseutils.LoadFileTypeJson }
func (u loadUploader) GetLoadFilesMetadata(_ context.Context, _ warehouseutils.GetLoadFilesOptions) ([]warehouseutils.LoadFile, error) {
	return u.files, nil
}

func TestPrecheckAllFilesAndCatalogViewBeforeLoad(t *testing.T) {
	m := testManager()
	m.warehouse.Destination.Config["skipViews"] = false
	prefix := "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/"
	m.uploader = loadUploader{files: []warehouseutils.LoadFile{{Location: prefix + "one.gz"}, {Location: prefix + "two.gz"}}}
	original := metadataSize
	t.Cleanup(func() { metadataSize = original })
	var statements []string
	m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
	catalogCalls := 0
	m.catalog = func(context.Context) (model.Schema, error) {
		catalogCalls++
		return model.Schema{"events": {"id": "string", "received_at": "datetime", "current_column": "string"}}, nil
	}
	metadataSize = func(_ context.Context, _ *config.Config, _ map[string]any, key string) (int64, error) {
		if key == "two.gz" {
			return 0, nil
		}
		return 10, nil
	}
	_, err := m.LoadTable(context.Background(), "events")
	require.ErrorContains(t, err, "length")
	require.Empty(t, statements)
	require.Zero(t, catalogCalls)
	metadataSize = func(context.Context, *config.Config, map[string]any, string) (int64, error) { return 10, nil }
	m.catalog = func(context.Context) (model.Schema, error) {
		return nil, errors.New("catalog unavailable")
	}
	_, err = m.LoadTable(context.Background(), "events")
	require.ErrorContains(t, err, "catalog unavailable")
	require.Empty(t, statements)
	m.catalog = func(context.Context) (model.Schema, error) {
		catalogCalls++
		return model.Schema{"events": {"id": "string", "received_at": "datetime", "current_column": "string"}}, nil
	}
	_, err = m.LoadTable(context.Background(), "events")
	require.NoError(t, err)
	require.Equal(t, 1, catalogCalls)
	require.Len(t, statements, 3)
	require.Contains(t, statements[0], "CREATE OR REPLACE VIEW")
	require.Contains(t, statements[0], "`current_column`")
	require.Contains(t, statements[0], "`pt` >=")
	require.Contains(t, statements[0], "`pt` < TO_CHAR(DATEADD(GETDATE(), 2, 'dd')")
	require.Contains(t, statements[1], "one.gz")
	require.Contains(t, statements[2], "two.gz")
}

func TestSkipUsersTable(t *testing.T) {
	m := testManager()
	m.warehouse.Destination.Config["skipUsersTable"] = true
	m.uploader = loadUploader{files: nil}
	result := m.LoadUserTables(context.Background())
	require.Equal(t, map[string]error{warehouseutils.IdentifiesTable: nil}, result)
}

func TestMaxComputeSummary(t *testing.T) {
	for _, test := range []struct {
		input string
		rows  int64
		valid bool
	}{
		{`{"tasks":[{"inputRecords":42}]}`, 42, true},
		{`{"rowsInserted":0}`, 0, true},
		{`{"inputRecords":-1}`, 0, false},
		{`{"inputRecords":2.5}`, 0, false},
		{`{"bytes":1024}`, 0, false},
		{`secret`, 0, false},
	} {
		rows, err := loadSummaryRows(test.input)
		if test.valid {
			require.NoError(t, err)
			require.Equal(t, test.rows, rows)
		} else {
			require.Error(t, err)
			require.NotContains(t, err.Error(), "secret")
		}
	}
}

func TestFixedViewsAndNames(t *testing.T) {
	m := testManager()
	m.warehouse.Destination.Config["skipViews"] = false
	m.SetPlannedPartitions(map[string]model.PartitionPlan{"event_source": {Source: "_partitiontime", Granularity: "hour"}})
	statements := []string{}
	m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
	require.NoError(t, m.CreateTable(context.Background(), "event_source", model.TableSchema{"id": "string", "_partitiontime": "datetime"}))
	require.Contains(t, statements[0], "'hour'")
	require.Contains(t, statements[1], "`pt` >=")
	require.Contains(t, statements[1], "`pt` < TO_CHAR(DATEADD(GETDATE(), 2, 'dd')")
	require.Contains(t, statements[1], "PARTITION BY `id`")
	first := viewName(strings.Repeat("a", 120) + "b")
	second := viewName(strings.Repeat("a", 120) + "c")
	require.LessOrEqual(t, len(first), 128)
	require.NotEqual(t, first, second)
	m.warehouse.Destination.Config["skipViews"] = true
	statements = nil
	require.NoError(t, m.CreateTable(context.Background(), "events", model.TableSchema{"id": "string", "received_at": "datetime"}))
	require.Contains(t, statements[0], "TRUNC_TIME(`_partitiontime`, 'day') AS `pt`")
	require.Contains(t, statements[0], "'table.format.version'='2'")
	require.Len(t, statements, 1)
}

func TestMaxComputeConfiguredEndpoint(t *testing.T) {
	m := testManager()
	m.warehouse.Destination.Config["project"] = "project"
	m.warehouse.Destination.Config["endpoint"] = "https://evil.example/api"
	require.ErrorContains(t, m.init(m.warehouse), "endpoint")
	m.warehouse.Destination.Config["endpoint"] = "https://service.cn-hangzhou.maxcompute.aliyun.com/api"
	require.NoError(t, m.init(m.warehouse))
	m.warehouse.Destination.Config["region"] = "us-west-1"
	require.ErrorContains(t, m.init(m.warehouse), "endpoint")
	m.warehouse.Destination.Config["region"] = "cn-hangzhou"
	delete(m.warehouse.Destination.Config, "endpoint")
	require.ErrorContains(t, m.init(m.warehouse), "endpoint is required")
}

func TestPreDDLAndTimeoutAndDeleteBy(t *testing.T) {
	m := testManager()
	called := false
	m.exec = func(context.Context, string) error { called = true; return nil }
	schema := model.TableSchema{}
	for i := range 1201 {
		schema["c"+strings.Repeat("a", i)] = "string"
	}
	require.ErrorContains(t, m.CreateTable(context.Background(), "events", schema), "1200")
	require.False(t, called)
	m.SetConnectionTimeout(0)
	require.Equal(t, 30*time.Second, m.connectTimeout)
	m.SetConnectionTimeout(24 * time.Hour)
	require.Equal(t, 2*time.Minute, m.connectTimeout)
	require.NoError(t, m.DeleteBy(context.Background(), nil, warehouseutils.DeleteByParams{}))
}
