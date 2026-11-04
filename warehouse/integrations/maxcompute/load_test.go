package maxcompute

import (
	"context"
	"errors"
	"net/url"
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
	location := "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/path/file.json.gz"
	var statements []string
	m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
	for _, metadata := range []string{`{}`, `{"content_length":0}`, `{"content_length":3221225473}`, `{"content_length":"10"}`, `{"content_length":1.5}`} {
		_, err := m.loadTable(context.Background(), "events", []warehouseutils.LoadFile{{Location: location, Metadata: []byte(metadata)}})
		require.ErrorContains(t, err, "length", metadata)
		require.Empty(t, statements)
	}
	files := []warehouseutils.LoadFile{{Location: location, Metadata: []byte(`{"content_length":3221225472}`)}}
	stats, err := m.loadTable(context.Background(), "events", files)
	require.NoError(t, err)
	require.EqualValues(t, 0, stats.RowsInserted)
	require.Equal(t, []string{"LOAD INTO TABLE `project`.`events` FROM LOCATION 'oss://oss-cn-hangzhou-internal.aliyuncs.com/my-bucket/path/file.json.gz' ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' WITH SERDEPROPERTIES ('odps.properties.rolearn'='read-role') STORED AS TEXTFILE"}, statements)
}

func TestOSSLocationAcceptsEncodedObjectKeys(t *testing.T) {
	m := testManager()
	m.exec = func(context.Context, string) error { return nil }
	for _, key := range []string{"a%/b+ c/日本.json.gz", "it's+safe.json.gz"} {
		location := "https://oss-cn-hangzhou.aliyuncs.com/my-bucket/" + url.PathEscape(key)
		paths, err := m.preparedLoadFiles(context.Background(), []warehouseutils.LoadFile{{Location: location, Metadata: []byte(`{"content_length":10}`)}})
		require.NoError(t, err)
		require.Equal(t, []string{"oss://oss-cn-hangzhou-internal.aliyuncs.com/my-bucket/" + key}, paths)
		statement, err := m.loadStatement("`project`.`events`", paths[0])
		require.NoError(t, err)
		require.Contains(t, statement, "odps.properties.rolearn")
		require.NotContains(t, statement, "read-role/"+key)
	}
}

func TestMaxComputeValidationReadsLoadedRowWithinPartition(t *testing.T) {
	m := testManager()
	var loads int
	m.exec = func(_ context.Context, statement string) error {
		require.Contains(t, statement, "LOAD INTO TABLE")
		loads++
		return nil
	}
	m.query = func(_ context.Context, statement string) (warehouseutils.QueryResult, error) {
		require.Equal(t, "SELECT `id`, `val` FROM `project`.`events` WHERE `pt` >= '2024-01-02' AND `pt` < '2024-01-03' AND `id` = 1 LIMIT 1", statement)
		return warehouseutils.QueryResult{Values: [][]string{{"1", "RudderStack"}}}, nil
	}
	payload := map[string]any{"id": 1, "val": "RudderStack", "uuid_ts": "2024-01-02 00:00:00.123456", "__content_length": int64(10)}
	location := "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/test.json.gz"
	require.NoError(t, m.TestLoadTable(context.Background(), location, "events", payload, warehouseutils.LoadFileTypeJson))
	require.Equal(t, 1, loads)
	m.query = func(context.Context, string) (warehouseutils.QueryResult, error) {
		return warehouseutils.QueryResult{}, nil
	}
	require.ErrorContains(t, m.TestLoadTable(context.Background(), location, "events", payload, warehouseutils.LoadFileTypeJson), "row was not found")
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
	m.uploader = loadUploader{files: []warehouseutils.LoadFile{{Location: prefix + "one.gz", Metadata: []byte(`{"content_length":10}`)}, {Location: prefix + "two.gz"}}}
	var statements []string
	m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
	catalogCalls := 0
	m.catalog = func(context.Context) (model.Schema, error) {
		catalogCalls++
		return model.Schema{"events": {"id": "string", "received_at": "datetime", "current_column": "string"}}, nil
	}
	_, err := m.LoadTable(context.Background(), "events")
	require.ErrorContains(t, err, "length")
	require.Empty(t, statements)
	require.Zero(t, catalogCalls)
	m.uploader = loadUploader{files: []warehouseutils.LoadFile{{Location: prefix + "one.gz", Metadata: []byte(`{"content_length":10}`)}, {Location: prefix + "two.gz", Metadata: []byte(`{"content_length":10}`)}}}
	m.catalog = func(context.Context) (model.Schema, error) { return nil, errors.New("catalog unavailable") }
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
		{`{"tasks":[{"Outputs":[{"OutputRecordCounts":{"pt=2024-01-01":20,"pt=2024-01-02":22}}],"inputRecords":100}]}`, 42, true},
		{`{"Outputs":[{"OutputRecordCounts":[0,3]},{"OutputRecordCounts":[4]}]}`, 7, true},
		{`{"inputRecords":42}`, 0, false},
		{`{"Outputs":[{"OutputRecordCounts":-1}]}`, 0, false},
		{`{"Outputs":[{"OutputRecordCounts":2.5}]}`, 0, false},
		{`{"Outputs":[]}`, 0, false},
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
	m.warehouse.Destination.Config["partitionType"] = "hour"
	statements := []string{}
	m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
	require.NoError(t, m.CreateTable(context.Background(), "event_source", model.TableSchema{"id": "string", "uuid_ts": "datetime"}))
	require.Contains(t, statements[0], "'hour'")
	require.Contains(t, statements[1], "`pt` >=")
	require.Contains(t, statements[1], "`pt` < TO_CHAR(DATEADD(GETDATE(), 2, 'dd')")
	require.Contains(t, statements[1], "PARTITION BY `id`")
	first := viewName(strings.Repeat("a", 120) + "b")
	second := viewName(strings.Repeat("a", 120) + "c")
	require.LessOrEqual(t, len(first), 128)
	require.NotEqual(t, first, second)
	m.warehouse.Destination.Config["skipViews"] = true
	m.warehouse.Destination.Config["partitionType"] = "day"
	statements = nil
	require.NoError(t, m.CreateTable(context.Background(), "events", model.TableSchema{"id": "string", "received_at": "datetime"}))
	require.Contains(t, statements[0], "TRUNC_TIME(`uuid_ts`, 'day') AS `pt`")
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
