package maxcompute

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/datatype"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

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
	require.NoError(t, warehouseutils.ValidateMaxComputeSettings(settings, "workspace"))
	require.ErrorContains(t, warehouseutils.ValidateMaxComputeSettings(settings, ""), "workspace ID")
	settings["accessKeyId"] = "destination-key"
	require.ErrorContains(t, warehouseutils.ValidateMaxComputeSettings(settings, "workspace"), "cannot be combined")
	delete(settings, "accessKeyId")
	settings["accessKeySecret"] = "destination-secret"
	require.ErrorContains(t, warehouseutils.ValidateMaxComputeSettings(settings, "workspace"), "cannot be combined")
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
	statement, err := m.viewSQL("events", model.TableSchema{"id": "string", "_partitiontime": "datetime"})
	require.NoError(t, err)
	require.Contains(t, statement, "`pt` >=")
	require.Contains(t, statement, "`project`.`events`")
	require.NotContains(t, statement, "SELECT *")
	m.warehouse.Namespace = "schema"
	statement, err = m.viewSQL("events", model.TableSchema{"id": "string"})
	require.NoError(t, err)
	require.Contains(t, statement, "`project`.`schema`.`events`")
	for _, granularity := range []string{"day", "hour"} {
		t.Run(granularity, func(t *testing.T) {
			m.warehouse.Destination.Config["partitionType"] = granularity
			statement, err := m.viewSQL("events", model.TableSchema{"id": "string"})
			require.NoError(t, err)
			require.Contains(t, statement, " WHERE `pt` >= TO_CHAR(DATEADD(GETDATE(), -60, 'dd'), 'yyyy-mm-dd') AND `pt` < TO_CHAR(DATEADD(GETDATE(), 2, 'dd'), 'yyyy-mm-dd')")
			require.NotContains(t, statement, "`pt` <=")
			require.NotContains(t, statement, "yyyy-mm-dd hh:00:00")
		})
	}
	statement, err = m.viewSQL(warehouseutils.DiscardsTable, model.TableSchema{"row_id": "string", "column_name": "string", "table_name": "string", "loaded_at": "datetime"})
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
