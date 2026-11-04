package maxcompute

import (
	"context"
	"fmt"
	"io"
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
	for _, location := range []string{
		"http://my-bucket.oss-cn-hangzhou.aliyuncs.com/path", "https://evil.example/path",
		"https://my-bucket.oss-cn-hangzhou.aliyuncs.com.evil.example/path",
		"https://my-bucket.oss-cn-hangzhou.aliyuncs.com:443/path",
		"https://my-bucket.oss-cn-hangzhou.aliyuncs.com/path?signature=x",
		"https://my-bucket.oss-cn-hangzhou.aliyuncs.com/%2e%2e/path",
	} {
		require.Error(t, validateLocation(location, settings), location)
	}
}

func TestTypesAndConversion(t *testing.T) {
	typ, err := columnType("array(int)")
	require.NoError(t, err)
	require.Equal(t, "ARRAY<BIGINT>", typ)
	_, err = columnType("int); DROP TABLE foo")
	require.Error(t, err)
	logical, ok := sdkType(datatype.NewArrayType(datatype.NewPrimitiveType(datatype.BIGINT)))
	require.True(t, ok)
	require.Equal(t, "array(int)", logical)
}

func TestRoleSettingsAreExclusiveAndRequireWorkspace(t *testing.T) {
	settings := map[string]any{"bucketName": "my-bucket", "region": "cn-hangzhou", "roleBasedAuth": true, "iamRoleARN": "acs:ram::123:role/test"}
	require.NoError(t, validateOSSSettings(settings, "workspace"))
	require.ErrorContains(t, validateOSSSettings(settings, ""), "workspace ID")
	settings["accessKeyID"] = "destination-key"
	require.ErrorContains(t, validateOSSSettings(settings, "workspace"), "cannot be combined")
	delete(settings, "accessKeyID")
	settings["accessKey"] = "destination-secret"
	require.ErrorContains(t, validateOSSSettings(settings, "workspace"), "cannot be combined")
	delete(settings, "accessKey")
	m := testManager()
	m.warehouse.Destination.Config = settings
	m.warehouse.Destination.Config["project"] = "project"
	m.warehouse.Destination.WorkspaceID = "workspace"
	config.Default.Set("FileManager.ALIBABA_OSS.callerAccessKeyID", "")
	config.Default.Set("FileManager.ALIBABA_OSS.callerAccessKeySecret", "")
	require.ErrorContains(t, m.init(m.warehouse), "caller credentials unavailable")
	config.Default.Set("FileManager.ALIBABA_OSS.callerAccessKeyID", "server-id")
	config.Default.Set("FileManager.ALIBABA_OSS.callerAccessKeySecret", "server-secret")
	t.Cleanup(func() {
		config.Default.Set("FileManager.ALIBABA_OSS.callerAccessKeyID", "")
		config.Default.Set("FileManager.ALIBABA_OSS.callerAccessKeySecret", "")
	})
	require.NoError(t, m.init(m.warehouse))
}

func TestCreateTableStatements(t *testing.T) {
	instance := odps.NewOdps(nil, "https://service.cn-hangzhou.maxcompute.aliyun.com/api")
	instance.SetDefaultProjectName("project")
	m := &MaxCompute{odps: instance, warehouse: model.Warehouse{}}
	m.warehouse.Destination.Config = map[string]any{"partitionMode": "fixed", "partitionType": "hour", "skipViews": true}
	var statements []string
	m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
	err := m.CreateTable(context.Background(), "events", model.TableSchema{"id": "string", "_partitiontime": "datetime"})
	require.NoError(t, err)
	require.Len(t, statements, 1)
	require.Contains(t, statements[0], "AUTO PARTITIONED BY (TRUNC_TIME(`_partitiontime`, 'hour') AS `pt`)")
	statements = nil
	m.warehouse.Destination.Config["partitionType"] = "hour'); DROP TABLE"
	err = m.CreateTable(context.Background(), "events", model.TableSchema{"id": "string", "_partitiontime": "datetime"})
	require.Error(t, err)
	require.Empty(t, statements)
}

func TestCreateTablePartitionSource(t *testing.T) {
	columns := model.TableSchema{
		"id": "string", "received_at": "datetime", "loaded_at": "datetime", "_partitiontime": "datetime", "bad": "string",
	}
	for _, tc := range []struct {
		name     string
		config   map[string]any
		override string
		want     string
		failure  string
	}{
		{name: "configured source over fixed default", config: map[string]any{"partitionMode": "fixed", "partitionColumn": "received_at", "partitionType": "hour"}, want: "TRUNC_TIME(`received_at`, 'hour')"},
		{name: "table source over destination default", config: map[string]any{"partitionColumn": "received_at", "partitionColumns": map[string]any{"events": "loaded_at"}}, want: "TRUNC_TIME(`loaded_at`, 'day')"},
		{name: "pipeline source", config: map[string]any{"partitionColumn": "received_at"}, override: "loaded_at", want: "TRUNC_TIME(`loaded_at`, 'day')"},
		{name: "sentinel", config: map[string]any{"partitionColumn": "_PARTITIONTIME", "partitionType": "hour"}, want: "TRUNC_TIME(GETDATE(), 'day')"},
		{name: "sentinel fixed", config: map[string]any{"partitionMode": "fixed", "partitionColumn": "_PARTITIONTIME"}, want: "TRUNC_TIME(GETDATE(), 'day')"},
		{name: "explicit empty fixed", config: map[string]any{"partitionMode": "fixed", "partitionColumn": ""}, want: "TRUNC_TIME(GETDATE(), 'day')"},
		{name: "dynamic default ingestion", config: map[string]any{"partitionMode": "dynamic"}, want: "TRUNC_TIME(GETDATE(), 'day')"},
		{name: "fixed default", config: map[string]any{"partitionMode": "fixed"}, want: "TRUNC_TIME(`_partitiontime`, 'day')"},
		{name: "invalid source", config: map[string]any{"partitionColumn": "bad; DROP TABLE"}, failure: "invalid MaxCompute identifier"},
		{name: "source missing", config: map[string]any{"partitionColumn": "missing"}, want: "TRUNC_TIME(GETDATE(), 'day')"},
		{name: "normalized source", config: map[string]any{"partitionColumn": "Received_At"}, want: "TRUNC_TIME(`received_at`, 'day')"},
		{name: "source wrong type", config: map[string]any{"partitionColumn": "bad"}, failure: "datetime column"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := testManager()
			m.warehouse.Destination.Config = tc.config
			m.warehouse.Destination.Config["skipViews"] = true
			m.warehouse.Source.ID = "source"
			m.warehouse.Destination.ID = "destination"
			if tc.override != "" {
				m.conf = config.New()
				m.conf.Set("Warehouse.pipeline.source.destination.partitionColumn.events", tc.override)
			}
			var statements []string
			m.exec = func(_ context.Context, sql string) error { statements = append(statements, sql); return nil }
			err := m.CreateTable(context.Background(), "events", columns)
			if tc.failure != "" {
				require.ErrorContains(t, err, tc.failure)
				require.Empty(t, statements)
				return
			}
			require.NoError(t, err)
			require.Len(t, statements, 1)
			require.Contains(t, statements[0], tc.want+" AS `pt`")
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
		{name: "existing populated owned schema", owner: "owner", principal: "owner", wantWrites: 1},
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
				require.Equal(t, "CREATE SCHEMA IF NOT EXISTS `project`.`schema`", statements[0])
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
			_, _ = fmt.Fprint(w, "<Authorization><Result>ALIYUN$123@aliyun.com</Result></Authorization>")
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
					_, _ = fmt.Fprint(w, `{"Instance":{"JsonSummary":"{\"inputRecords\":42}"}}`)
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
			rows, err := m.runSQL(context.Background(), "LOAD DATA INPATH 'oss://bucket/file'", true)
			require.NoError(t, err)
			require.True(t, submitted)
			require.EqualValues(t, 42, rows)
		})
	}
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
	m.warehouse.Destination.Config = map[string]any{"partitionMode": "fixed"}
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
