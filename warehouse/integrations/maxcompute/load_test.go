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
	m.warehouse.Destination.Config = map[string]any{"bucketName": "my-bucket", "region": "cn-hangzhou", "accessKeyId": "id", "accessKeySecret": "secret", "ossRoleArn": "acs:ram::123456789:role/read-role", "endpoint": "https://service.cn-hangzhou.maxcompute.aliyun.com/api", "skipViews": true}
	return m
}

func TestOSSLoadChecksMetadataBeforeSQL(t *testing.T) {
	m := testManager()
	folder := "11111111-1111-1111-1111-111111111111-events"
	prefix := "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/" + folder + "/"
	var statements []string
	m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
	for _, metadata := range []string{`{}`, `{"content_length":0}`, `{"content_length":3221225473}`, `{"content_length":"10"}`, `{"content_length":1.5}`} {
		files := []warehouseutils.LoadFile{
			{Location: prefix + "one.json.gz", Metadata: []byte(`{"content_length":10}`)},
			{Location: prefix + "two.json.gz", Metadata: []byte(metadata)},
		}
		_, err := m.loadTable(context.Background(), "events", files)
		require.ErrorContains(t, err, "length", metadata)
		require.Empty(t, statements)
	}
	files := []warehouseutils.LoadFile{
		{Location: prefix + "one.json.gz", Metadata: []byte(`{"content_length":3221225472}`)},
		{Location: prefix + "two.json.gz", Metadata: []byte(`{"content_length":10}`)},
	}
	stats, err := m.loadTable(context.Background(), "events", files)
	require.NoError(t, err)
	require.EqualValues(t, 0, stats.RowsInserted)
	require.Equal(t, []string{"LOAD INTO TABLE `project`.`events` FROM LOCATION 'oss://oss-cn-hangzhou-internal.aliyuncs.com/my-bucket/" + folder + "/' ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' WITH SERDEPROPERTIES ('odps.properties.rolearn'='acs:ram::123456789:role/read-role') STORED AS TEXTFILE"}, statements)
}

func TestOSSLocationAcceptsEncodedObjectKeys(t *testing.T) {
	m := testManager()
	folder := "11111111-1111-1111-1111-111111111111-events"
	key := folder + "/a%+ c-日本.json.gz"
	location := "https://oss-cn-hangzhou.aliyuncs.com/my-bucket/" + url.PathEscape(key)
	folders, err := m.preparedLoadFiles(context.Background(), []warehouseutils.LoadFile{{Location: location, Metadata: []byte(`{"content_length":10}`)}})
	require.NoError(t, err)
	require.Equal(t, []loadFolder{{location: "oss://oss-cn-hangzhou-internal.aliyuncs.com/my-bucket/" + folder + "/", size: 10}}, folders)
}

func TestOSSLoadRejectsUnsafeSQLLiterals(t *testing.T) {
	m := testManager()
	safeFolder := "11111111-1111-1111-1111-111111111111-events"
	var statements []string
	m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
	for _, bad := range []string{"'", "\\", "\x01", "\n", "\x7f"} {
		key := safeFolder + "/file" + bad + ".json.gz"
		location := "https://oss-cn-hangzhou.aliyuncs.com/my-bucket/" + url.PathEscape(key)
		_, err := m.loadTable(context.Background(), "events", []warehouseutils.LoadFile{{Location: location, Metadata: []byte(`{"content_length":10}`)}})
		require.Error(t, err, bad)
		require.Empty(t, statements)
		key = safeFolder + bad + "/file.json.gz"
		location = "https://oss-cn-hangzhou.aliyuncs.com/my-bucket/" + url.PathEscape(key)
		_, err = m.loadTable(context.Background(), "events", []warehouseutils.LoadFile{{Location: location, Metadata: []byte(`{"content_length":10}`)}})
		require.Error(t, err, bad)
		require.Empty(t, statements)
		m.warehouse.Destination.Config["bucketName"] = "my-bucket" + bad
		_, err = m.loadTable(context.Background(), "events", []warehouseutils.LoadFile{{Location: "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/" + safeFolder + "/file.json.gz", Metadata: []byte(`{"content_length":10}`)}})
		require.Error(t, err, bad)
		require.Empty(t, statements)
		m.warehouse.Destination.Config["bucketName"] = "my-bucket"
		m.warehouse.Destination.Config["ossRoleArn"] = "acs:ram::123456789:role/read" + bad
		_, err = m.loadTable(context.Background(), "events", []warehouseutils.LoadFile{{Location: "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/" + safeFolder + "/file.json.gz", Metadata: []byte(`{"content_length":10}`)}})
		require.Error(t, err, bad)
		require.Empty(t, statements)
		m.warehouse.Destination.Config["ossRoleArn"] = "acs:ram::123456789:role/read-role"
	}
}

func TestMaxComputeValidationReadsLoadedRowWithinPartition(t *testing.T) {
	m := testManager()
	type contextKey struct{}
	var loads int
	m.exec = func(_ context.Context, statement string) error {
		require.Contains(t, statement, "LOAD INTO TABLE")
		loads++
		return nil
	}
	m.query = func(queryCtx context.Context, statement string) (warehouseutils.QueryResult, error) {
		require.Equal(t, true, queryCtx.Value(contextKey{}))
		require.Equal(t, "SELECT `id`, `val` FROM `project`.`events` WHERE `pt` >= '2024-01-02' AND `pt` < '2024-01-03' AND `id` = 1 LIMIT 1", statement)
		return warehouseutils.QueryResult{Values: [][]string{{"1", "RudderStack"}}}, nil
	}
	payload := map[string]any{"id": 1, "val": "RudderStack", "uuid_ts": "2024-01-02 00:00:00"}
	location := "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/11111111-1111-1111-1111-111111111111-validation/test.json.gz"
	ctx := warehouseutils.WithMaxComputeValidationFileSize(context.WithValue(context.Background(), contextKey{}, true), 10)
	require.NoError(t, m.TestLoadTable(ctx, location, "events", payload, warehouseutils.LoadFileTypeJson))
	require.Equal(t, 1, loads)
	m.query = func(context.Context, string) (warehouseutils.QueryResult, error) {
		return warehouseutils.QueryResult{}, nil
	}
	require.ErrorContains(t, m.TestLoadTable(ctx, location, "events", payload, warehouseutils.LoadFileTypeJson), "row was not found")
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
	prefix := "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/11111111-1111-1111-1111-111111111111-events/"
	prefix2 := "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/22222222-2222-2222-2222-222222222222-events/"
	m.uploader = loadUploader{files: []warehouseutils.LoadFile{{Location: prefix + "one.json.gz", Metadata: []byte(`{"content_length":10}`)}, {Location: prefix2 + "two.json.gz"}}}
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
	m.uploader = loadUploader{files: []warehouseutils.LoadFile{{Location: prefix + "one.json.gz", Metadata: []byte(`{"content_length":10}`)}, {Location: prefix2 + "two.json.gz", Metadata: []byte(`{"content_length":10}`)}}}
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
	require.Contains(t, statements[1], "11111111-1111-1111-1111-111111111111-events/")
	require.Contains(t, statements[2], "22222222-2222-2222-2222-222222222222-events/")
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
