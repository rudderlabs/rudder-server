package maxcompute

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestValidateLocation(t *testing.T) {
	settings := map[string]any{"bucketName": "my-bucket", "region": "cn-hangzhou"}
	require.NoError(t, validateLocation("https://my-bucket.oss-cn-hangzhou.aliyuncs.com/path/file.gz", settings))
	require.NoError(t, validateLocation("https://oss-cn-hangzhou.aliyuncs.com/my-bucket/path/a%25+b%20c.gz", settings))
	for _, location := range []string{
		"http://my-bucket.oss-cn-hangzhou.aliyuncs.com/path", "https://evil.example/path",
		"https://my-bucket.oss-cn-hangzhou.aliyuncs.com.evil.example/path",
		"https://my-bucket.oss-cn-hangzhou.aliyuncs.com:443/path",
		"https://my-bucket.oss-cn-hangzhou.aliyuncs.com/path?signature=x",
	} {
		require.Error(t, validateLocation(location, settings), location)
	}
	withPath := misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{Provider: warehouseutils.AlibabaOSS, Config: map[string]any{"bucketName": "my-bucket/rudder", "region": "cn-hangzhou"}})
	require.NoError(t, validateLocation("https://my-bucket.oss-cn-hangzhou.aliyuncs.com/rudder/file.gz", withPath))
}

func TestTypesAndConversion(t *testing.T) {
	for logical, physical := range map[string]string{
		"string": "STRING", "text": "STRING", "int": "BIGINT",
		"float": "DOUBLE", "boolean": "BOOLEAN", "datetime": "TIMESTAMP",
	} {
		actual, err := columnType(logical)
		require.NoError(t, err)
		require.Equal(t, physical, actual)
	}
	for _, unsupported := range []string{"json", "array(int)", "int); DROP TABLE foo"} {
		_, err := columnType(unsupported)
		require.Error(t, err)
	}
	for physical, logical := range map[datatype.TypeID]string{
		datatype.BIGINT: "int", datatype.INT: "int", datatype.SMALLINT: "int", datatype.TINYINT: "int",
		datatype.DOUBLE: "float", datatype.FLOAT: "float", datatype.DECIMAL: "float",
		datatype.BOOLEAN: "boolean", datatype.TIMESTAMP: "datetime", datatype.DATETIME: "datetime",
		datatype.DATE: "datetime", datatype.STRING: "string", datatype.VARCHAR: "string", datatype.CHAR: "string",
	} {
		actual, ok := sdkType(datatype.NewPrimitiveType(physical))
		require.True(t, ok)
		require.Equal(t, logical, actual)
	}
	for _, unsupported := range []datatype.TypeID{datatype.BINARY, datatype.JSON, datatype.TIMESTAMP_NTZ} {
		_, ok := sdkType(datatype.NewPrimitiveType(unsupported))
		require.False(t, ok)
	}
	_, ok := sdkType(datatype.NewArrayType(datatype.NewPrimitiveType(datatype.BIGINT)))
	require.False(t, ok)
}

func TestRoleSettingsAreExclusiveAndRequireWorkspace(t *testing.T) {
	settings := map[string]any{"bucketName": "my-bucket", "region": "cn-hangzhou", "iamRoleARN": "acs:ram::123:role/test", "ossRoleArn": "acs:ram::123456789:role/read-role", "endpoint": "https://service.cn-hangzhou.maxcompute.aliyun.com/api", "project": "project"}
	require.NoError(t, validateSettings(settings, "workspace"))
	require.ErrorContains(t, validateSettings(settings, ""), "workspace ID")
	settings["accessKeyId"] = "destination-key"
	require.ErrorContains(t, validateSettings(settings, "workspace"), "cannot be combined")
	delete(settings, "accessKeyId")
	settings["accessKeySecret"] = "destination-secret"
	require.ErrorContains(t, validateSettings(settings, "workspace"), "cannot be combined")
	delete(settings, "accessKeySecret")
	m := testManager()
	m.warehouse.Destination.Config = settings
	m.warehouse.Destination.WorkspaceID = "workspace"
	m.conf = config.New()
	require.ErrorContains(t, m.init(m.warehouse), "caller credentials unavailable")
	m.conf.Set("FileManager.ALIBABA_OSS.callerAccessKeyID", "server-id")
	m.conf.Set("FileManager.ALIBABA_OSS.callerAccessKeySecret", "server-secret")
	require.NoError(t, m.init(m.warehouse))
}

func TestCreateTableStatements(t *testing.T) {
	instance := odps.NewOdps(nil, "https://service.cn-hangzhou.maxcompute.aliyun.com/api")
	instance.SetDefaultProjectName("project")
	m := &MaxCompute{odps: instance, warehouse: model.Warehouse{}, conf: config.New()}
	m.warehouse.Destination.Config = map[string]any{"skipViews": true}
	m.warehouse.Destination.Config["partitionType"] = "hour"
	var statements []string
	m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
	require.NoError(t, m.CreateTable(context.Background(), "events", model.TableSchema{"id": "string"}))
	require.Len(t, statements, 1)
	require.Contains(t, statements[0], "TRUNC_TIME(\x60uuid_ts\x60, 'hour') AS \x60pt\x60")
	require.Contains(t, statements[0], "\x60uuid_ts\x60 TIMESTAMP")
	require.Contains(t, statements[0], "'table.format.version'='2'")
	require.NotContains(t, statements[0], "transactional")
	m.warehouse.Destination.Config["partitionType"] = "hour'); DROP TABLE"
	statements = nil
	require.Error(t, m.CreateTable(context.Background(), "events", model.TableSchema{"id": "string"}))
	require.Empty(t, statements)
}

func TestCreateTablePartitionSource(t *testing.T) {
	columns := model.TableSchema{
		"id": "string", "received_at": "datetime", "loaded_at": "datetime", "uuid_ts": "datetime",
		"bad": "string", "_timestamp": "datetime",
	}
	for _, tc := range []struct {
		name, source, granularity, want, failure string
	}{
		{name: "event source", source: "received_at", granularity: "hour", want: "TRUNC_TIME(\x60received_at\x60, 'hour')"},
		{name: "sentinel", source: "_PARTITIONTIME", granularity: "hour", want: "TRUNC_TIME(\x60uuid_ts\x60, 'hour')"},
		{name: "default ingestion", want: "TRUNC_TIME(\x60uuid_ts\x60, 'day')"},
		{name: "missing source", source: "sent_at", granularity: "hour", want: "TRUNC_TIME(\x60uuid_ts\x60, 'day')"},
		{name: "normalized source", source: "Received_At", want: "TRUNC_TIME(\x60received_at\x60, 'day')"},
		{name: "reserved source", source: "timestamp", want: "TRUNC_TIME(\x60_timestamp\x60, 'day')"},
		{name: "invalid source", source: "bad; DROP TABLE", failure: "unsupported MaxCompute partition column"},
		{name: "wrong type", source: "timestamp", failure: "datetime column"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caseColumns := make(model.TableSchema, len(columns))
			maps.Copy(caseColumns, columns)
			if tc.failure == "datetime column" {
				caseColumns["_timestamp"] = "string"
			}
			_, err := PlanPartition(caseColumns, tc.source, tc.granularity)
			if tc.failure != "" {
				require.ErrorContains(t, err, tc.failure)
				return
			}
			require.NoError(t, err)
			m := testManager()
			m.warehouse.Destination.Config["skipViews"] = true
			m.warehouse.Destination.Config["partitionColumn"] = tc.source
			m.warehouse.Destination.Config["partitionType"] = tc.granularity
			var statements []string
			m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
			require.NoError(t, m.CreateTable(context.Background(), "events", caseColumns))
			require.Len(t, statements, 1)
			require.Contains(t, statements[0], tc.want+" AS \x60pt\x60")
		})
	}
}

func TestNamespaceOwnerBeforeWrites(t *testing.T) {
	for _, tc := range []struct {
		name       string
		owner      string
		principal  string
		schemaCode int
		wantError  string
		wantWrites int
	}{
		{name: "existing populated owned schema", owner: "owner", principal: "owner"},
		{name: "existing populated foreign schema", owner: "other", principal: "owner", wantError: "not owned"},
		{name: "missing owner fails closed", principal: "owner", wantError: "owner unavailable"},
		{name: "missing schema can be created", schemaCode: http.StatusNotFound, wantWrites: 1},
		{name: "catalog error fails closed", schemaCode: http.StatusForbidden, wantError: "loading MaxCompute schema"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, http.MethodGet, r.Method)
				require.Equal(t, "/api/projects/project/schemas/schema", r.URL.Path)
				if tc.schemaCode != 0 {
					w.WriteHeader(tc.schemaCode)
					return
				}
				w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
				_, _ = fmt.Fprintf(w, "<Schema><Name>schema</Name><Owner>%s</Owner></Schema>", tc.owner)
			}))
			defer catalog.Close()
			instance := odps.NewOdps(account.NewAliyunAccount("id", "secret"), catalog.URL+"/api")
			instance.SetDefaultProjectName("project")
			m := &MaxCompute{odps: instance, warehouse: model.Warehouse{Namespace: "schema"}}
			called := 0
			m.principal = func(context.Context) (string, error) { called++; return tc.principal, nil }
			var statements []string
			m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
			err := m.CreateSchema(context.Background())
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				require.Empty(t, statements)
			} else {
				require.NoError(t, err)
				require.Len(t, statements, tc.wantWrites)
				if tc.wantWrites > 0 {
					require.Equal(t, "CREATE SCHEMA `schema`", statements[0])
				}
			}
			if tc.owner != "" && tc.schemaCode == 0 {
				require.Equal(t, 1, called)
			} else {
				require.Zero(t, called)
			}
		})
	}
}

func TestNamespaceOwnerPrincipalFromSDK(t *testing.T) {
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/projects/project/schemas/schema":
			w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
			_, _ = fmt.Fprint(w, "<Schema><Owner>ALIYUN$123@aliyun.com</Owner></Schema>")
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/authorization"):
			_, _ = fmt.Fprint(w, `<Authorization><Result>{"DisplayName":"ALIYUN$123@aliyun.com","ID":"p4_123","SourceIP":"127.0.0.1"}</Result></Authorization>`)
		default:
			t.Errorf("unexpected SDK request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer catalog.Close()
	instance := odps.NewOdps(account.NewAliyunAccount("id", "secret"), catalog.URL+"/api")
	instance.SetDefaultProjectName("project")
	m := &MaxCompute{odps: instance, warehouse: model.Warehouse{Namespace: "schema"}}
	require.NoError(t, m.checkNamespaceOwner(context.Background()))
}

func TestCurrentPrincipalRejectsInvalidWhoamiResult(t *testing.T) {
	for _, result := range []string{`not JSON`, `{}`, `{"DisplayName":""}`} {
		t.Run(result, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = fmt.Fprintf(w, "<Authorization><Result>%s</Result></Authorization>", result)
			}))
			defer server.Close()
			instance := odps.NewOdps(account.NewAliyunAccount("id", "secret"), server.URL+"/api")
			instance.SetDefaultProjectName("project")
			m := &MaxCompute{odps: instance}
			_, err := m.currentPrincipal(context.Background())
			require.ErrorContains(t, err, "principal")
			require.NotContains(t, err.Error(), result)
		})
	}
}

func TestRunSQLSharedRunnerAndSummary(t *testing.T) {
	for _, namespace := range []string{"", "schema"} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			var submitted bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost:
					submitted = true
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					for _, hint := range []string{`odps.namespace.schema`, `odps.sql.type.system.odps2`, `odps.sql.timezone`} {
						require.Contains(t, string(body), hint)
					}
					w.Header().Set("Location", "/api/projects/project/instances/test")
					w.WriteHeader(http.StatusCreated)
				case r.URL.Query().Has("instancesummary"):
					_, _ = fmt.Fprint(w, `{"Instance":{"JsonSummary":"{\"Outputs\":[{\"OutputRecordCounts\":[42]}]}"}}`)
				case r.URL.Query().Has("taskstatus"):
					_, _ = fmt.Fprint(w, `<Instance><Tasks><Task><Name>load</Name><Status>SUCCESS</Status></Task></Tasks></Instance>`)
				default:
					_, _ = fmt.Fprint(w, `<Instance><Status>RUNNING</Status></Instance>`)
				}
			}))
			defer server.Close()
			service := odps.NewOdps(account.NewAliyunAccount("id", "secret"), server.URL)
			service.SetDefaultProjectName("project")
			m := &MaxCompute{odps: service, warehouse: model.Warehouse{Namespace: namespace}, queryTimeout: time.Second, pollInterval: time.Millisecond}
			rows, err := m.runSQL(context.Background(), "LOAD INTO TABLE t FROM LOCATION 'oss://oss-cn-hangzhou-internal.aliyuncs.com/bucket/file' ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' WITH SERDEPROPERTIES ('odps.properties.rolearn'='acs:ram::123456789:role/read-role') STORED AS TEXTFILE", true)
			require.NoError(t, err)
			require.True(t, submitted)
			require.EqualValues(t, 42, rows)
		})
	}
}

func TestRunSQLSummaryFailureDoesNotFailCommittedLoad(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost:
			w.Header().Set("Location", "/api/projects/project/instances/test")
			w.WriteHeader(http.StatusCreated)
		case r.URL.Query().Has("instancesummary"):
			_, _ = fmt.Fprint(w, `{"Instance":{"JsonSummary":"not JSON"}}`)
		case r.URL.Query().Has("taskstatus"):
			_, _ = fmt.Fprint(w, `<Instance><Tasks><Task><Name>load</Name><Status>SUCCESS</Status></Task></Tasks></Instance>`)
		default:
			_, _ = fmt.Fprint(w, `<Instance><Status>RUNNING</Status></Instance>`)
		}
	}))
	defer server.Close()
	service := odps.NewOdps(account.NewAliyunAccount("id", "secret"), server.URL)
	service.SetDefaultProjectName("project")
	m := &MaxCompute{odps: service, queryTimeout: time.Second, pollInterval: time.Millisecond}
	rows, err := m.runSQL(context.Background(), "LOAD INTO TABLE t FROM LOCATION 'oss://bucket/folder/'", true)
	require.NoError(t, err)
	require.EqualValues(t, -1, rows)
}

func TestRunSQLTimeoutAndExecHook(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Location", "/api/projects/project/instances/test")
			w.WriteHeader(http.StatusCreated)
			return
		}
		if r.Method == http.MethodPut {
			return
		}
		_, _ = fmt.Fprint(w, `<Instance><Status>RUNNING</Status><Tasks><Task><Status>RUNNING</Status></Task></Tasks></Instance>`)
	}))
	defer server.Close()
	service := odps.NewOdps(account.NewAliyunAccount("id", "secret"), server.URL)
	service.SetDefaultProjectName("project")
	m := &MaxCompute{odps: service, queryTimeout: 60 * time.Millisecond, pollInterval: 5 * time.Millisecond}
	start := time.Now()
	_, err := m.runSQL(context.Background(), "SELECT 1", false)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second)

	called := false
	m.exec = func(ctx context.Context, sql string) error {
		called = true
		require.Equal(t, "SELECT 2", sql)
		return nil
	}
	_, err = m.runSQL(context.Background(), "SELECT 2", true)
	require.NoError(t, err)
	require.True(t, called)
}

func TestViewSQLBoundedAndQuoted(t *testing.T) {
	instance := odps.NewOdps(nil, "https://service.cn-hangzhou.maxcompute.aliyun.com/api")
	instance.SetDefaultProjectName("project")
	m := &MaxCompute{odps: instance, warehouse: model.Warehouse{}}
	_, err := m.viewSQL("bad;drop", model.TableSchema{"id": "string"})
	require.Error(t, err)
	m.warehouse.Destination.Config = map[string]any{}
	statement, err := m.viewSQL("events", model.TableSchema{"id": "string", "uuid_ts": "datetime"})
	require.NoError(t, err)
	require.Contains(t, statement, "`pt` >=")
	require.Contains(t, statement, "`project`.`events`")
	require.NotContains(t, statement, "SELECT *")
	m.warehouse.Namespace = "schema"
	statement, err = m.viewSQL("events", model.TableSchema{"id": "string", "uuid_ts": "datetime"})
	require.NoError(t, err)
	require.Contains(t, statement, "`project`.`schema`.`events`")
	for _, granularity := range []string{"day", "hour"} {
		t.Run(granularity, func(t *testing.T) {
			m.warehouse.Destination.Config["partitionType"] = granularity
			statement, err := m.viewSQL("events", model.TableSchema{"id": "string", "uuid_ts": "datetime"})
			require.NoError(t, err)
			require.Contains(t, statement, " WHERE `pt` >= TO_CHAR(DATEADD(GETDATE(), -60, 'dd'), 'yyyy-mm-dd') AND `pt` < TO_CHAR(DATEADD(GETDATE(), 2, 'dd'), 'yyyy-mm-dd')")
			require.NotContains(t, statement, "`pt` <=")
			require.NotContains(t, statement, "yyyy-mm-dd hh:00:00")
		})
	}
	statement, err = m.viewSQL(warehouseutils.DiscardsTable, model.TableSchema{"row_id": "string", "column_name": "string", "table_name": "string", "uuid_ts": "datetime"})
	require.NoError(t, err)
	require.Contains(t, statement, "PARTITION BY `row_id`, `column_name`, `table_name`")
}

func TestCreateTablePartitionPipelineOverride(t *testing.T) {
	m := testManager()
	m.warehouse.Source.ID = "source"
	m.warehouse.Destination.ID = "destination"
	m.warehouse.Destination.Config["partitionColumn"] = "received_at"
	m.warehouse.Destination.Config["partitionType"] = "day"
	m.warehouse.Destination.Config["skipViews"] = true
	m.conf.Set("Warehouse.pipeline.source.destination.partitionColumn", "_PARTITIONTIME")
	m.conf.Set("Warehouse.pipeline.source.destination.partitionType", "hour")
	var statement string
	m.exec = func(_ context.Context, sql string) error { statement = sql; return nil }
	require.NoError(t, m.CreateTable(context.Background(), "events", model.TableSchema{"id": "string", "uuid_ts": "datetime", "received_at": "datetime"}))
	require.Contains(t, statement, "TRUNC_TIME(`uuid_ts`, 'hour') AS `pt`")
}

func testManager() *MaxCompute {
	o := odps.NewOdps(nil, "https://service.cn-hangzhou.maxcompute.aliyun.com/api")
	o.SetDefaultProjectName("project")
	m := &MaxCompute{odps: o, warehouse: model.Warehouse{WorkspaceID: "workspace"}, conf: config.New()}
	m.warehouse.Destination.Config = map[string]any{"bucketName": "my-bucket", "region": "cn-hangzhou", "accessKeyId": "id", "accessKeySecret": "secret", "ossRoleArn": "acs:ram::123456789:role/read-role", "endpoint": "https://service.cn-hangzhou.maxcompute.aliyun.com/api", "skipViews": true}
	return m
}

func TestOSSLoadGroupsFilesByFolder(t *testing.T) {
	m := testManager()
	folder := "11111111-1111-1111-1111-111111111111-events"
	prefix := "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/" + folder + "/"
	var statements []string
	m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
	files := []warehouseutils.LoadFile{{Location: prefix + "one.json.gz"}, {Location: prefix + "two.json.gz"}}
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
	folders, err := m.preparedLoadFiles(context.Background(), []warehouseutils.LoadFile{{Location: location}})
	require.NoError(t, err)
	require.Equal(t, []loadFolder{{location: "oss://oss-cn-hangzhou-internal.aliyuncs.com/my-bucket/" + folder + "/"}}, folders)
}

func TestOSSLoadRejectsUnsafeSQLLiterals(t *testing.T) {
	m := testManager()
	safeFolder := "11111111-1111-1111-1111-111111111111-events"
	var statements []string
	m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
	for _, bad := range []string{"'", "\\", "\x01", "\n", "\x7f"} {
		key := safeFolder + "/file" + bad + ".json.gz"
		location := "https://oss-cn-hangzhou.aliyuncs.com/my-bucket/" + url.PathEscape(key)
		_, err := m.loadTable(context.Background(), "events", []warehouseutils.LoadFile{{Location: location}})
		require.Error(t, err, bad)
		require.Empty(t, statements)
		key = safeFolder + bad + "/file.json.gz"
		location = "https://oss-cn-hangzhou.aliyuncs.com/my-bucket/" + url.PathEscape(key)
		_, err = m.loadTable(context.Background(), "events", []warehouseutils.LoadFile{{Location: location}})
		require.Error(t, err, bad)
		require.Empty(t, statements)
		m.warehouse.Destination.Config["bucketName"] = "my-bucket" + bad
		_, err = m.loadTable(context.Background(), "events", []warehouseutils.LoadFile{{Location: "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/" + safeFolder + "/file.json.gz"}})
		require.Error(t, err, bad)
		require.Empty(t, statements)
		m.warehouse.Destination.Config["bucketName"] = "my-bucket"
		m.warehouse.Destination.Config["ossRoleArn"] = "acs:ram::123456789:role/read" + bad
		_, err = m.loadTable(context.Background(), "events", []warehouseutils.LoadFile{{Location: "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/" + safeFolder + "/file.json.gz"}})
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
	ctx := context.WithValue(context.Background(), contextKey{}, true)
	require.NoError(t, m.TestLoadTable(ctx, location, "events", payload, warehouseutils.LoadFileTypeJson))
	require.Equal(t, 1, loads)
	m.query = func(context.Context, string) (warehouseutils.QueryResult, error) {
		return warehouseutils.QueryResult{}, nil
	}
	require.ErrorContains(t, m.TestLoadTable(ctx, location, "events", payload, warehouseutils.LoadFileTypeJson), "row was not found")
}

func TestMaxComputeValidationLoadSkipsViewRefresh(t *testing.T) {
	m := testManager()
	m.warehouse.Destination.Config["skipViews"] = false
	m.catalog = func(context.Context) (model.Schema, error) {
		t.Fatal("validation load must not read the catalog to refresh a view")
		return nil, errors.New("unexpected catalog lookup")
	}
	var statements []string
	m.exec = func(_ context.Context, statement string) error {
		statements = append(statements, statement)
		return nil
	}
	m.query = func(context.Context, string) (warehouseutils.QueryResult, error) {
		return warehouseutils.QueryResult{Values: [][]string{{"1", "RudderStack"}}}, nil
	}
	payload := map[string]any{"id": 1, "val": "RudderStack", "uuid_ts": "2024-01-02 00:00:00"}
	location := "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/11111111-1111-1111-1111-111111111111-validation/test.json.gz"
	ctx := context.Background()
	require.NoError(t, m.TestLoadTable(ctx, location, "events", payload, warehouseutils.LoadFileTypeJson))
	require.Len(t, statements, 1)
	require.Contains(t, statements[0], "LOAD INTO TABLE")
}

type loadUploader struct {
	warehouseutils.Uploader
	files       []warehouseutils.LoadFile
	tableSchema model.TableSchema
}

func (u loadUploader) GetLoadFileType() string { return warehouseutils.LoadFileTypeJson }

func (u loadUploader) GetTableSchemaInWarehouse(string) model.TableSchema { return u.tableSchema }

func (u loadUploader) GetLoadFilesMetadata(_ context.Context, _ warehouseutils.GetLoadFilesOptions) ([]warehouseutils.LoadFile, error) {
	return u.files, nil
}

func TestLoadViewUsesUploadSchemaBeforeCatalog(t *testing.T) {
	m := testManager()
	m.warehouse.Destination.Config["skipViews"] = false
	prefix := "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/11111111-1111-1111-1111-111111111111-events/"
	m.uploader = loadUploader{
		files:       []warehouseutils.LoadFile{{Location: prefix + "one.json.gz"}},
		tableSchema: model.TableSchema{"id": "string", "uuid_ts": "datetime", "added_this_sync": "string"},
	}
	// a table created moments ago may not be listed yet: the catalog must not be consulted
	m.catalog = func(context.Context) (model.Schema, error) {
		t.Fatal("view columns must come from the upload schema")
		return nil, errors.New("unexpected catalog lookup")
	}
	var statements []string
	m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
	_, err := m.LoadTable(context.Background(), "events")
	require.NoError(t, err)
	require.Len(t, statements, 2)
	require.Contains(t, statements[0], "CREATE OR REPLACE VIEW")
	require.Contains(t, statements[0], "`added_this_sync`")
	require.Contains(t, statements[1], "LOAD INTO TABLE")
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
	require.Contains(t, statements[1], "ORDER BY `uuid_ts` DESC")
	require.Equal(t, "event_source_view", viewName("event_source"))
	require.Equal(t, strings.Repeat("a", 123)+"_view", viewName(strings.Repeat("a", 123)))
	require.LessOrEqual(t, len(viewName(strings.Repeat("a", 124))), 128)
	first := viewName(strings.Repeat("a", 123) + "b")
	second := viewName(strings.Repeat("a", 123) + "c")
	require.LessOrEqual(t, len(first), 128)
	require.NotEqual(t, first, second)
	m.warehouse.Destination.Config["skipViews"] = true
	m.warehouse.Destination.Config["partitionType"] = "day"
	statements = nil
	require.NoError(t, m.CreateTable(context.Background(), "events", model.TableSchema{"id": "string", "received_at": "datetime"}))
	require.Contains(t, statements[0], "TRUNC_TIME(`uuid_ts`, 'day') AS `pt`")
	require.Contains(t, statements[0], "'table.format.version'='2'")
	require.Len(t, statements, 1)
	m.warehouse.Destination.Config["skipViews"] = false
	statements = nil
	require.NoError(t, m.createTableView(context.Background(), "keyless", model.TableSchema{"uuid_ts": "datetime"}))
	require.Empty(t, statements)
}

type usersUploader struct {
	loadUploader
	users  model.TableSchema
	err    error
	tables []string
}

func (u *usersUploader) GetTableSchemaInWarehouse(string) model.TableSchema { return u.users }

func (u *usersUploader) GetLoadFilesMetadata(_ context.Context, options warehouseutils.GetLoadFilesOptions) ([]warehouseutils.LoadFile, error) {
	u.tables = append(u.tables, options.Table)
	return u.files, u.err
}

func testUsersManager(t *testing.T, source, granularity string) (*MaxCompute, *usersUploader, *[]string) {
	t.Helper()
	m := testManager()
	expression := fmt.Sprintf(`[{"leafExprDesc":{"reference":{"name":"%s"}}},{"leafExprDesc":{"constant":"%s"}},{"functionCall":{"name":"trunc_time"}}]`, source, granularity)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		schema, _ := jsonrs.Marshal(map[string]any{"columns": []any{map[string]any{"name": "id", "type": "string"}, map[string]any{"name": source, "type": "timestamp"}}, "PartitionKeys": []any{map[string]any{"name": "pt", "type": "string", "hasGenerateExpression": true, "generateExpression": expression}}})
		_, _ = fmt.Fprintf(w, "<Table><Schema>%s</Schema></Table>", strings.ReplaceAll(string(schema), "&", "&amp;"))
	}))
	t.Cleanup(server.Close)
	m.odps = odps.NewOdps(account.NewAliyunAccount("id", "secret"), server.URL)
	m.odps.SetDefaultProjectName("project")
	u := &usersUploader{
		files: []warehouseutils.LoadFile{{Location: "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/11111111-1111-1111-1111-111111111111-users/one.json.gz"}},
		users: model.TableSchema{"id": "string", "received_at": "datetime", "loaded_at": "datetime", "uuid_ts": "datetime", "email": "string", "name": "string"},
	}
	m.uploader = u
	statements := &[]string{}
	m.exec = func(_ context.Context, sql string) error { *statements = append(*statements, sql); return nil }
	m.catalog = func(context.Context) (model.Schema, error) {
		return model.Schema{warehouseutils.UsersTable: u.users}, nil
	}
	return m, u, statements
}

func TestUsersCarryForwardUnpartitionedStage(t *testing.T) {
	for _, tc := range []struct{ source, granularity string }{
		{"uuid_ts", "day"}, {"uuid_ts", "hour"}, {"received_at", "day"},
	} {
		t.Run(tc.source+"_"+tc.granularity, func(t *testing.T) {
			m, u, statements := testUsersManager(t, tc.source, tc.granularity)
			m.warehouse.Destination.Config["skipViews"] = false
			require.NoError(t, m.loadUsersWithCarryForward(context.Background()))
			require.Equal(t, []string{warehouseutils.UsersTable}, u.tables)
			require.Len(t, *statements, 5)
			stage := strings.TrimPrefix(strings.SplitN((*statements)[0], " (", 2)[0], "CREATE TABLE ")
			require.Contains(t, (*statements)[0], "`loaded_at` TIMESTAMP")
			require.Contains(t, (*statements)[0], "TBLPROPERTIES ('table.format.version'='2')")
			require.NotContains(t, (*statements)[0], "AUTO PARTITIONED")
			require.Contains(t, (*statements)[1], "LOAD INTO TABLE "+stage+" FROM LOCATION")
			require.Contains(t, (*statements)[2], "CREATE OR REPLACE VIEW `project`.`users_view`")
			insert := (*statements)[3]
			require.Contains(t, insert, "INSERT INTO `project`.`users`")
			require.Contains(t, insert, "FROM "+stage+" WHERE `id` IS NOT NULL")
			require.NotContains(t, insert, "FROM "+stage+" WHERE `pt`")
			require.Contains(t, insert, "FROM `project`.`users` WHERE `pt` >= '"+time.Now().UTC().AddDate(0, 0, -60).Format("2006-01-02")+"' AND `pt` < '"+time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02")+"' AND `id` IN (SELECT `id` FROM "+stage+" WHERE `id` IS NOT NULL)")
			require.NotContains(t, insert, "FROM `project`.`identifies`")
			require.Contains(t, insert, " UNION ALL ")
			require.Contains(t, insert, "SELECT DISTINCT")
			require.Contains(t, insert, "FIRST_VALUE(`email`, true) OVER (PARTITION BY `id` ORDER BY `received_at` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)")
			require.Contains(t, insert, "FIRST_VALUE(`received_at`, true)")
			require.Contains(t, insert, "FIRST_VALUE(CASE WHEN `__current_upload` = 1 THEN `uuid_ts` END, true)")
			require.Contains(t, insert, "FIRST_VALUE(CASE WHEN `__current_upload` = 1 THEN `loaded_at` END, true)")
			require.Equal(t, "DROP TABLE IF EXISTS "+stage, (*statements)[4])
		})
	}
}

func TestUsersCarryForwardPreflightAndCleanup(t *testing.T) {
	m, u, statements := testUsersManager(t, "uuid_ts", "day")
	u.files = nil
	require.NoError(t, m.loadUsersWithCarryForward(context.Background()))
	require.Empty(t, *statements)
	u.files = []warehouseutils.LoadFile{{Location: "https://evil.example/bad.gz"}}
	require.ErrorContains(t, m.loadUsersWithCarryForward(context.Background()), "load file")
	require.Empty(t, *statements)
	u.err = errors.New("router failed with secret")
	require.ErrorContains(t, m.loadUsersWithCarryForward(context.Background()), "load files failed")
	require.NotContains(t, m.loadUsersWithCarryForward(context.Background()).Error(), "secret")
	require.Empty(t, *statements)
	u.err = nil
	u.files = []warehouseutils.LoadFile{{Location: "https://my-bucket.oss-cn-hangzhou.aliyuncs.com/11111111-1111-1111-1111-111111111111-users/one.json.gz"}}
	m.exec = func(_ context.Context, sql string) error {
		*statements = append(*statements, sql)
		if strings.HasPrefix(sql, "INSERT INTO") {
			return errors.New("insert failed")
		}
		return nil
	}
	require.ErrorContains(t, m.loadUsersWithCarryForward(context.Background()), "insert failed")
	require.Contains(t, (*statements)[len(*statements)-1], "DROP TABLE IF EXISTS")
}

func TestFetchSchemaKeepsTableNamesAndExcludesGeneratedPTAndViews(t *testing.T) {
	expression := "[{\"leafExprDesc\":{\"reference\":{\"name\":\"received_at\"}}},{\"leafExprDesc\":{\"constant\":\"hour\"}},{\"functionCall\":{\"name\":\"trunc_time\"}}]"
	catalogSchema, err := jsonrs.Marshal(map[string]any{
		"columns": []any{
			map[string]any{"name": "id", "type": "string"},
			map[string]any{"name": "received_at", "type": "timestamp"},
		},
		"PartitionKeys": []any{
			map[string]any{"name": "pt", "type": "string", "hasGenerateExpression": true, "generateExpression": expression},
		},
	})
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/tables") {
			_, _ = fmt.Fprint(w, "<Tables><Table><Name>product_view_clicked</Name><Owner>owner</Owner></Table><Table><Name>actual_view</Name><Owner>owner</Owner></Table></Tables>")
			return
		}
		kind := odps.ManagedTable.String()
		if strings.HasSuffix(r.URL.Path, "/actual_view") {
			kind = odps.VirtualView.String()
		}
		if strings.HasSuffix(r.URL.Path, "/without_partition") {
			_, _ = fmt.Fprintf(w, "<Table><Owner>owner</Owner><Type>%s</Type><Schema>%s</Schema></Table>", kind, html.EscapeString(`{"columns":[{"name":"id","type":"string"}]}`))
			return
		}
		_, _ = fmt.Fprintf(w, "<Table><Owner>owner</Owner><Type>%s</Type><Schema>%s</Schema></Table>", kind, html.EscapeString(string(catalogSchema)))
	}))
	defer server.Close()
	instance := odps.NewOdps(account.NewAliyunAccount("id", "secret"), server.URL)
	instance.SetDefaultProjectName("project")
	m := &MaxCompute{odps: instance}
	schema, err := m.FetchSchema(context.Background())
	require.NoError(t, err)
	require.Contains(t, schema, "product_view_clicked")
	require.NotContains(t, schema, "actual_view")
	require.Equal(t, "datetime", schema["product_view_clicked"]["received_at"])
	require.NotContains(t, schema["product_view_clicked"], "pt")

	plan, err := m.CatalogPartition(context.Background(), "product_view_clicked")
	require.NoError(t, err)
	require.Equal(t, "received_at", plan.Source)
	require.Equal(t, "hour", plan.Granularity)
	_, err = m.CatalogPartition(context.Background(), "without_partition")
	require.ErrorContains(t, err, "no supported pt partition expression")
}

func TestFetchSchemaMissingNamespaceIsEmpty(t *testing.T) {
	for _, tc := range []struct {
		name, code, message string
		missing             bool
	}{
		{
			name: "missing schema", code: "ODPS-0420095",
			message: "Access Denied: NO privilege 'odps:List' on /projects/project/schemas/missing. Schema project.missing does not exist Tips: Principal:RAM$123456789:987654321",
			missing: true,
		},
		{name: "other permission error", code: "ODPS-0420095", message: "Access Denied: NO privilege 'odps:List' on /projects/project/schemas/existing"},
		{name: "other error code", code: "ODPS-0420096", message: "Schema project.missing does not exist"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				_, _ = fmt.Fprintf(w, "<Error><Code>%s</Code><Message>%s</Message></Error>", tc.code, html.EscapeString(tc.message))
			}))
			defer server.Close()
			instance := odps.NewOdps(account.NewAliyunAccount("id", "secret"), server.URL)
			instance.SetDefaultProjectName("project")
			m := &MaxCompute{odps: instance}
			schema, err := m.FetchSchema(context.Background())
			if tc.missing {
				require.NoError(t, err)
				require.NotNil(t, schema)
				require.Empty(t, schema)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.code)
		})
	}
}

func TestErrorMappingsUseSanitizedDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, diagnostic string
		want             model.JobErrorType
	}{
		{"project membership", "ODPS-0420095: User does not exist in the project", model.PermissionError},
		{"RAM numeric ID", "ODPS-0030001: user name is not a valid account", model.PermissionError},
		{"schema edition", "ODPS-0110061: project doesn't support schema", model.PermissionError},
		{"full scan", "ODPS-0130071: full scan of all partitions is not allowed", model.UncategorizedError},
		{"identifier length", "ODPS-0130071: identifier too long, max length is 128", model.UncategorizedError},
		{"assume role", "NoPermission", model.PermissionError},
		{"invalid access key", "InvalidAccessKeyId", model.PermissionError},
		{"signature mismatch", "SignatureDoesNotMatch", model.PermissionError},
		{"caller keys", "MaxCompute role caller credentials unavailable", model.PermissionError},
		{"missing table", "TableNotFound", model.ResourceNotFoundError},
		{"cell limit", "ODPS-0121095: cell exceeds 8 MB", model.ColumnSizeError},
		{"column count", "MaxCompute table exceeds 1200 columns", model.ColumnCountError},
		{"generic", "ODPS-1234567: unexpected failure", model.UncategorizedError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			safe := errors.New(tc.diagnostic)
			if tc.name != "caller keys" && tc.name != "column count" {
				safe = client.ODPSSafeError(safe)
			}
			got := model.UncategorizedError
			for _, mapping := range (&MaxCompute{}).ErrorMappings() {
				if mapping.Format.MatchString(safe.Error()) {
					got = mapping.Type
					break
				}
			}
			require.Equal(t, tc.want, got, safe.Error())
		})
	}
}

func TestMaxComputeErrorReasons(t *testing.T) {
	for _, tc := range []struct {
		name, message, want string
		role                bool
	}{
		{"membership", "ODPS-0420095: User does not exist in the project", "project_membership", false},
		{"numeric RAM ID", "ODPS-0030001: user name is not a valid account", "project_membership", false},
		{"schema edition", "ODPS-0110061: project doesn't support schema", "schema_not_enabled", false},
		{"identifier length", "ODPS-0130071: too long, max length is 128", "identifier_length", false},
		{"full scan", "ODPS-0130071: full scan of all partitions is prohibited", "fullscan_guard", false},
		{"role trust", "NoPermission while assuming role", "role_assumption", true},
		{"OSS key", "InvalidAccessKeyId", "access_key_authentication", false},
		{"OSS signature", "SignatureDoesNotMatch", "access_key_authentication", false},
		{"ODPS key", "ODPS authentication or permission denied", "access_key_authentication", false},
		{"caller keys", "MaxCompute role caller credentials unavailable", "caller_credentials_missing", true},
		{"OSS policy", "oss: Error returned by Service. Http Status Code: 403. Error Code: AccessDenied.", "oss_access_denied", false},
		{"OSS policy in role mode", "oss: service returned error: StatusCode=403, ErrorCode=AccessDenied", "oss_access_denied", true},
		{"unknown", "ODPS-1111111: unknown", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason, guidance := errorReason(tc.message, tc.role)
			require.Equal(t, tc.want, reason)
			if reason != "" {
				require.NotEmpty(t, guidance)
				require.NotContains(t, guidance, tc.message)
			}
		})
	}
}

func TestMaxComputeAuthModeValidation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		config    map[string]any
		workspace string
		role      bool
		valid     bool
	}{
		{"role", map[string]any{"iamRoleARN": "odps-role", "ossRoleArn": "oss-role", "bucketName": "bucket", "region": "cn-hangzhou"}, "workspace", true, true},
		{"access key", map[string]any{"accessKeyId": "id", "accessKeySecret": "secret", "ossRoleArn": "oss-role", "bucketName": "bucket", "region": "cn-hangzhou"}, "", false, true},
		{"both", map[string]any{"iamRoleARN": "odps-role", "accessKeyId": "id", "accessKeySecret": "secret", "ossRoleArn": "oss-role", "bucketName": "bucket", "region": "cn-hangzhou"}, "workspace", false, false},
		{"neither", map[string]any{"ossRoleArn": "oss-role", "bucketName": "bucket", "region": "cn-hangzhou"}, "workspace", false, false},
		{"partial", map[string]any{"accessKeyId": "id", "ossRoleArn": "oss-role", "bucketName": "bucket", "region": "cn-hangzhou"}, "workspace", false, false},
		{"missing workspace", map[string]any{"iamRoleARN": "odps-role", "ossRoleArn": "oss-role", "bucketName": "bucket", "region": "cn-hangzhou"}, "", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			role, modeErr := roleMode(tc.config)
			if tc.valid {
				require.NoError(t, modeErr)
				require.Equal(t, tc.role, role)
				require.NoError(t, validateSettings(tc.config, tc.workspace))
			} else {
				require.Error(t, validateSettings(tc.config, tc.workspace))
			}
		})
	}
}

func TestValidationError(t *testing.T) {
	roleConfig := map[string]any{"iamRoleARN": "acs:ram::123:role/test"}
	got := ValidationError(model.VerifyingObjectStorage, roleConfig, errors.New("NoPermission while assuming role"))
	require.EqualError(t, got, "role_assumption: Check the role trust policy and workspace-bound ExternalId.")

	got = ValidationError(model.VerifyingObjectStorage, nil, errors.New("unexpected remote detail"))
	require.EqualError(t, got, "MaxCompute OSS validation failed")

	original := errors.New("unexpected catalog error")
	require.ErrorIs(t, ValidationError(model.VerifyingFetchSchema, nil, original), original)
}

func TestNormalizeDateTime(t *testing.T) {
	for input, want := range map[string]string{
		"2025-03-04T05:06:07.123456Z":          "2025-03-04 05:06:07.123456",
		"2025-03-04 06:06:07.123456 +0100 CET": "2025-03-04 05:06:07.123456",
		"2025-03-04 06:06:07.123456 +0100":     "2025-03-04 05:06:07.123456",
		"2025-03-04 05:06:07.123456":           "2025-03-04 05:06:07.123456",
		"2025-03-04 05:06:07":                  "2025-03-04 05:06:07",
	} {
		got, ok := NormalizeDateTime(input)
		require.True(t, ok, input)
		require.Equal(t, want, got)
		again, ok := NormalizeDateTime(got)
		require.True(t, ok)
		require.Equal(t, got, again)
	}
	for _, invalid := range []any{"not a date", 42, nil} {
		_, ok := NormalizeDateTime(invalid)
		require.False(t, ok)
	}
}
