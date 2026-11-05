package maxcompute_test

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/compose-test/compose"
	"github.com/rudderlabs/compose-test/testcompose"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	maxcompute "github.com/rudderlabs/rudder-server/warehouse/integrations/maxcompute"
	whth "github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type integrationCredentials struct {
	AccessKeyID           string `json:"accessKeyId"`
	AccessKeySecret       string `json:"accessKeySecret"`
	IAMRoleARN            string `json:"iamRoleARN"`
	CallerAccessKeyID     string `json:"callerAccessKeyId"`
	CallerAccessKeySecret string `json:"callerAccessKeySecret"`
	WorkspaceID           string `json:"workspaceID"`
	OSSRoleARN            string `json:"ossRoleArn"`
	Project               string `json:"project"`
	Endpoint              string `json:"endpoint"`
	Region                string `json:"region"`
	BucketName            string `json:"bucketName"`
	Prefix                string `json:"prefix"`
}

func TestIntegration(t *testing.T) {
	if os.Getenv("SLOW") != "1" {
		t.Skip("set SLOW=1 to run MaxCompute integration tests")
	}
	misc.Init()
	validations.Init()
	warehouseutils.Init()
	for _, mode := range []struct {
		name, env string
		role      bool
	}{
		{"access_key", "MAXCOMPUTE_INTEGRATION_TEST_CREDENTIALS", false},
		{"role", "MAXCOMPUTE_ROLE_INTEGRATION_TEST_CREDENTIALS", true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			secret := os.Getenv(mode.env)
			if secret == "" {
				if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
					t.Fatalf("%s is not set", mode.env)
				}
				t.Skipf("%s is not set", mode.env)
			}
			var credentials integrationCredentials
			require.NoError(t, jsonrs.Unmarshal([]byte(secret), &credentials))
			require.NotEmpty(t, credentials.Prefix)
			require.NotEmpty(t, credentials.OSSRoleARN)
			if mode.role {
				require.NotEmpty(t, credentials.WorkspaceID)
				require.NotEmpty(t, credentials.IAMRoleARN)
				t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "FileManager.ALIBABA_OSS.callerAccessKeyID"), credentials.CallerAccessKeyID)
				t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "FileManager.ALIBABA_OSS.callerAccessKeySecret"), credentials.CallerAccessKeySecret)
			} else {
				require.NotEmpty(t, credentials.AccessKeyID)
				require.NotEmpty(t, credentials.AccessKeySecret)
			}
			if !t.Run("validation", func(t *testing.T) { testIntegrationValidation(t, credentials, mode.role) }) {
				t.FailNow() // The remaining cases depend on the same CREATE and LOAD path.
			}
			t.Run("events_and_schema", func(t *testing.T) { testIntegrationEvents(t, credentials, mode.role) })
			for _, partition := range []struct{ name, column, granularity string }{
				{"ingestion_hour", "", "hour"},
				{"received_at_day", "received_at", "day"},
			} {
				t.Run(partition.name, func(t *testing.T) {
					testIntegrationPartition(t, credentials, mode.role, partition.column, partition.granularity)
				})
			}
		})
	}
}

func integrationConfig(c integrationCredentials, namespace string, role bool) map[string]any {
	settings := map[string]any{
		"project": c.Project, "endpoint": c.Endpoint, "region": c.Region,
		"bucketName": c.BucketName, "prefix": c.Prefix, "ossRoleArn": c.OSSRoleARN,
		"namespace": namespace, "syncFrequency": "30", "skipUsersTable": false,
		"skipViews": false, "useRudderStorage": false, "cleanupObjectStorageFiles": true,
	}
	if role {
		settings["iamRoleARN"] = c.IAMRoleARN
	} else {
		settings["accessKeyId"] = c.AccessKeyID
		settings["accessKeySecret"] = c.AccessKeySecret
	}
	return settings
}

func integrationDestination(c integrationCredentials, namespace, workspaceID string, role bool) backendconfig.DestinationT {
	id := warehouseutils.RandHex()
	destination := backendconfigtest.NewDestinationBuilder(warehouseutils.MAXCOMPUTE).
		WithID(id).WithRevisionID(id)
	for key, value := range integrationConfig(c, namespace, role) {
		destination.WithConfigOption(key, value)
	}
	result := destination.Build()
	result.WorkspaceID = workspaceID
	return result
}

func TestIntegrationDestinationRevisionID(t *testing.T) {
	destination := integrationDestination(integrationCredentials{}, "test_schema", "test_workspace", false)
	require.Equal(t, destination.ID, destination.RevisionID)
}

type integrationRuntime struct {
	*maxcompute.MaxCompute
	service   *odps.Odps
	warehouse model.Warehouse
}

func integrationManager(t *testing.T, dest backendconfig.DestinationT, namespace string, uploader warehouseutils.Uploader) *integrationRuntime {
	t.Helper()
	m := maxcompute.New(config.Default, logger.NOP)
	m.SetConnectionTimeout(time.Minute)
	warehouse := model.Warehouse{Destination: dest, Namespace: namespace}
	require.NoError(t, m.Setup(context.Background(), warehouse, uploader))
	connection, err := m.Connect(context.Background(), warehouse)
	require.NoError(t, err)
	runtime := &integrationRuntime{MaxCompute: m, service: connection.ODPS, warehouse: warehouse}
	t.Cleanup(func() { cleanupIntegrationSchema(t, runtime, namespace) })
	return runtime
}

func testIntegrationValidation(t *testing.T, c integrationCredentials, role bool) {
	namespace := whth.RandSchema(warehouseutils.MAXCOMPUTE)
	workspaceID := warehouseutils.RandHex()
	if role {
		workspaceID = c.WorkspaceID
	}
	destination := integrationDestination(c, namespace, workspaceID, role)
	integrationManager(t, destination, namespace, nil)
	var response *validations.DestinationValidationResponse
	for attempt := 0; attempt < 3; attempt++ {
		response = validations.NewDestinationValidator().Validate(context.Background(), &destination)
		transient := strings.Contains(response.Error, "ODPS request timed out") ||
			(strings.Contains(response.Error, "ODPS-0420095") && strings.Contains(response.Error, "does not exist"))
		if response.Success || !transient {
			break
		}
		t.Log("retrying MaxCompute validation after a transient catalog or SQL timeout")
		time.Sleep(time.Second)
	}
	require.True(t, response.Success, response.Error)
	require.Len(t, response.Steps, 6)
	for _, step := range response.Steps {
		require.True(t, step.Success, "%s: %s", step.Name, step.Error)
	}
}

func testIntegrationEvents(t *testing.T, c integrationCredentials, role bool) {
	httpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	stack := testcompose.New(t, compose.FilePaths([]string{"../testdata/docker-compose.jobsdb.yml", "../testdata/docker-compose.transformer.yml"}))
	stack.Start(context.Background())

	namespace := whth.RandSchema(warehouseutils.MAXCOMPUTE)
	workspaceID := warehouseutils.RandHex()
	if role {
		workspaceID = c.WorkspaceID // the role trusts this ExternalId
	}
	sourceID, writeKey := warehouseutils.RandHex(), warehouseutils.RandHex()
	destination := integrationDestination(c, namespace, workspaceID, role)
	workspaceConfig := backendconfigtest.NewConfigBuilder().WithSource(
		backendconfigtest.NewSourceBuilder().WithID(sourceID).WithWriteKey(writeKey).
			WithWorkspaceID(workspaceID).WithConnection(destination).Build(),
	).WithWorkspaceID(workspaceID).Build()
	whth.BootstrapSvc(t, workspaceConfig, httpPort, stack.Port("jobsDb", 5432))
	m := integrationManager(t, destination, namespace, nil)
	require.NoError(t, m.CreateSchema(context.Background()), "SQL purpose: create event schema")
	require.NoError(t, m.TestConnection(context.Background(), m.warehouse), "verify event schema ownership")
	jobsDB := whth.JobsDB(t, stack.Port("jobsDb", 5432))
	tables := []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"}
	fixture := integrationEventsFixture(t, "../testdata/upload-job.events-1.json", false)
	firstUser := whth.GetUserId(warehouseutils.MAXCOMPUTE)
	first := whth.TestConfig{
		WriteKey: writeKey, Schema: namespace, Tables: tables, SourceID: sourceID,
		DestinationID: destination.ID, Config: destination.Config, WorkspaceID: workspaceID,
		DestinationType: warehouseutils.MAXCOMPUTE, JobsDB: jobsDB, HTTPPort: httpPort,
		EventsFilePath: fixture, UserID: firstUser, SkipWarehouse: true,
		TransformerURL: fmt.Sprintf("http://localhost:%d", stack.Port("transformer", 9090)),
		Destination:    destination,
	}
	first.VerifyEvents(t)
	assertIntegrationEventCounts(t, m, namespace, firstUser, tables)
	plan, err := m.CatalogPartition(context.Background(), "tracks")
	require.NoError(t, err)
	require.Equal(t, "uuid_ts", plan.Source)
	require.Equal(t, "day", plan.Granularity)

	second := first
	second.UserID = firstUser
	second.EventsFilePath = integrationEventsFixture(t, "../testdata/upload-job.events-2.json", true)
	second.VerifyEvents(t)
	for _, table := range tables {
		if table != warehouseutils.UsersTable {
			assertIntegrationCount(t, m, namespace, table, "user_id", firstUser, 8)
		}
	}
	assertIntegrationCount(t, m, namespace, warehouseutils.UsersTable, "id", firstUser, 2)
	schema, err := m.FetchSchema(context.Background())
	require.NoError(t, err)
	require.Equal(t, "string", schema["product_track"]["integration_added"])
	require.Equal(t, "string", schema["users"]["email"])
	var viewHasColumn bool
	odps.NewTables(m.service, c.Project, namespace).List(func(view *odps.Table, err error) {
		require.NoError(t, err)
		if view == nil || view.Name() != "product_track_view" {
			return
		}
		require.NoError(t, view.Load())
		require.Equal(t, odps.VirtualView, view.Type())
		for _, column := range view.Schema().Columns {
			viewHasColumn = viewHasColumn || column.Name == "integration_added"
		}
	})
	require.True(t, viewHasColumn, "product_track view did not refresh after ADD COLUMNS")
	assertIntegrationViewCount(t, m, namespace, "users_view", "id", firstUser, 1)
	productView := integrationQuery(t, m, fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`.`product_track_view` WHERE `integration_added` = 'yes'", c.Project, namespace))
	require.Len(t, productView.Values, 1)
	require.NotEqual(t, "0", productView.Values[0][0])
	userView := integrationQuery(t, m, fmt.Sprintf("SELECT `email`, `uuid_ts`, `loaded_at` FROM `%s`.`%s`.`users_view` WHERE `id` = '%s'", c.Project, namespace, firstUser))
	require.Len(t, userView.Values, 1)
	require.Len(t, userView.Values[0], 3)
	require.Equal(t, "rhedricks+1@example.com", userView.Values[0][0])
	newest := integrationQuery(t, m, fmt.Sprintf("SELECT `uuid_ts`, `loaded_at` FROM `%s`.`%s`.`users` WHERE %s AND `id` = '%s' ORDER BY `uuid_ts` DESC LIMIT 1", c.Project, namespace, integrationPTBounds(), firstUser))
	require.Len(t, newest.Values, 1)
	require.Equal(t, newest.Values[0], userView.Values[0][1:])
	testIntegrationDiscards(t, m, destination, namespace)
}

func integrationEventsFixture(t *testing.T, source string, addColumn bool) string {
	t.Helper()
	contents, err := os.ReadFile(source)
	require.NoError(t, err)
	text := strings.ReplaceAll(string(contents), "2023-05-12", time.Now().UTC().Format("2006-01-02"))
	if addColumn {
		oldEmail := `"email": "rhedricks+5@example.com"`
		require.Contains(t, text, oldEmail)
		text = strings.Replace(text, oldEmail, `"email": null`, 1)
		before := `"product_id": "5",`
		require.Contains(t, text, before)
		text = strings.Replace(text, before, before+` "integration_added": "yes",`, 1)
	}
	fixture, err := os.CreateTemp(t.TempDir(), "events-*.json")
	require.NoError(t, err)
	_, err = fixture.WriteString(text)
	require.NoError(t, err)
	require.NoError(t, fixture.Close())
	return fixture.Name()
}

func integrationQuery(t *testing.T, m *integrationRuntime, statement string) warehouseutils.QueryResult {
	t.Helper()
	result, err := client.QueryODPS(context.Background(), m.service, statement, 5*time.Minute, time.Second)
	require.NoError(t, err, "SQL purpose: MaxCompute integration readback")
	return result
}

func assertIntegrationViewCount(t *testing.T, m *integrationRuntime, namespace, view, column, value string, expected int64) {
	t.Helper()
	require.Regexp(t, `^[A-Za-z0-9_]+$`, value)
	statement := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`.`%s` WHERE `%s` = '%s'", m.service.DefaultProjectName(), namespace, view, column, value)
	result := integrationQuery(t, m, statement)
	require.Len(t, result.Values, 1)
	actual, err := strconv.ParseInt(result.Values[0][0], 10, 64)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func integrationPTBounds() string {
	now := time.Now().UTC()
	return fmt.Sprintf("`pt` >= '%s' AND `pt` < '%s'", now.AddDate(0, 0, -60).Format("2006-01-02"), now.AddDate(0, 0, 1).Format("2006-01-02"))
}

func assertIntegrationEventCounts(t *testing.T, m *integrationRuntime, namespace, userID string, tables []string) {
	t.Helper()
	for _, table := range tables {
		column, count := "user_id", int64(4)
		if table == "users" {
			column, count = "id", 1
		}
		assertIntegrationCount(t, m, namespace, table, column, userID, count)
	}
}

func assertIntegrationCount(t *testing.T, m *integrationRuntime, namespace, table, column, value string, expected int64) {
	t.Helper()
	// Values are generated by the test helper, never supplied by a destination.
	require.Regexp(t, `^[A-Za-z0-9_]+$`, value)
	statement := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`.`%s` WHERE %s AND `%s` = '%s'", m.service.DefaultProjectName(), namespace, table, integrationPTBounds(), column, value)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		result, err := client.QueryODPS(context.Background(), m.service, statement, 5*time.Minute, time.Second)
		if !assert.NoError(c, err, "SQL purpose: bounded table readback") || !assert.Len(c, result.Values, 1) || !assert.Len(c, result.Values[0], 1) {
			return
		}
		actual, err := strconv.ParseInt(result.Values[0][0], 10, 64)
		if assert.NoError(c, err) {
			assert.Equal(c, expected, actual, statement)
		}
	}, 10*time.Minute, 10*time.Second)
}

type integrationUploader struct {
	warehouseutils.Uploader
	files []warehouseutils.LoadFile
}

func (u integrationUploader) GetLoadFileType() string { return warehouseutils.LoadFileTypeJson }
func (u integrationUploader) GetLoadFilesMetadata(_ context.Context, _ warehouseutils.GetLoadFilesOptions) ([]warehouseutils.LoadFile, error) {
	return u.files, nil
}

func integrationUploadRow(t *testing.T, dest backendconfig.DestinationT, table string, row map[string]any) warehouseutils.LoadFile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "row.json.gz")
	writer, err := misc.CreateGZ(path)
	require.NoError(t, err)
	encoded, err := jsonrs.Marshal(row)
	require.NoError(t, err)
	require.NoError(t, writer.WriteGZ(string(encoded)+"\n"))
	require.NoError(t, writer.Close())
	size := writer.BytesWritten()
	require.Positive(t, size)
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	converted := misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
		Provider: warehouseutils.AlibabaOSS, Config: dest.Config, WorkspaceID: dest.WorkspaceID,
	})
	fm, err := filemanager.New(&filemanager.Settings{Provider: warehouseutils.AlibabaOSS, Config: converted, Conf: config.Default})
	require.NoError(t, err)
	folder := strings.ToLower(misc.FastUUID().String()) + "-" + table
	output, err := fm.Upload(context.Background(), f, "maxcompute-integration", folder)
	require.NoError(t, err)
	key, err := fm.GetObjectNameFromLocation(output.Location)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(key, strings.Trim(castPrefix(dest.Config), "/")+"/"), "fixture escaped credential prefix")
	t.Cleanup(func() {
		require.NoError(t, fm.Delete(context.Background(), []string{key}), "delete MaxCompute integration fixture")
	})
	return warehouseutils.LoadFile{Location: output.Location, Metadata: fmt.Appendf(nil, `{"content_length":%d}`, size)}
}

func castPrefix(settings map[string]any) string {
	value, _ := settings["prefix"].(string)
	return value
}

func testIntegrationPartition(t *testing.T, c integrationCredentials, role bool, partitionColumn, partitionType string) {
	namespace := whth.RandSchema(warehouseutils.MAXCOMPUTE)
	workspaceID := warehouseutils.RandHex()
	if role {
		workspaceID = c.WorkspaceID
	}
	dest := integrationDestination(c, namespace, workspaceID, role)
	dest.Config["partitionColumn"] = partitionColumn
	dest.Config["partitionType"] = partitionType
	m := integrationManager(t, dest, namespace, nil)
	ctx := context.Background()
	require.NoError(t, m.CreateSchema(ctx), "SQL purpose: create partition-probe schema")
	columns := model.TableSchema{
		"id": "string", "user_id": "string", "uuid_ts": "datetime",
		"received_at": "datetime", "loaded_at": "datetime",
	}
	require.NoError(t, m.CreateTable(ctx, "partition_probe", columns), "SQL purpose: create partition-probe table")
	plan, err := m.CatalogPartition(ctx, "partition_probe")
	require.NoError(t, err)
	wantSource := "uuid_ts"
	if partitionColumn != "" {
		wantSource = partitionColumn
	}
	require.Equal(t, wantSource, plan.Source)
	require.Equal(t, partitionType, plan.Granularity)
	stamp := time.Now().UTC()
	row := map[string]any{
		"id": "integration_row", "user_id": "integration_user",
		"uuid_ts":     stamp.Format("2006-01-02 15:04:05"),
		"received_at": stamp.Format("2006-01-02 15:04:05.999999"),
		"loaded_at":   stamp.Format("2006-01-02 15:04:05.999999"),
	}
	require.NoError(t, m.Setup(ctx, m.warehouse, integrationUploader{files: []warehouseutils.LoadFile{integrationUploadRow(t, dest, "partition_probe", row)}}))
	stats, err := m.LoadTable(ctx, "partition_probe")
	require.NoError(t, err, "SQL purpose: LOAD partition-probe folder")
	if stats.RowsInserted >= 0 {
		require.EqualValues(t, 1, stats.RowsInserted)
	}
	assertIntegrationCount(t, m, namespace, "partition_probe", "id", "integration_row", 1)
	partitionValue := stamp.Format("2006-01-02")
	if partitionType == "hour" {
		partitionValue = stamp.Truncate(time.Hour).Format("2006-01-02 15:04:05")
	}
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`.`partition_probe` WHERE %s AND `pt` = '%s'", c.Project, namespace, integrationPTBounds(), partitionValue)
	result, err := client.QueryODPS(ctx, m.service, query, 5*time.Minute, time.Second)
	require.NoError(t, err, "SQL purpose: read partition-probe pt")
	require.Len(t, result.Values, 1)
	require.Equal(t, "1", result.Values[0][0])
}

func testIntegrationDiscards(t *testing.T, m *integrationRuntime, dest backendconfig.DestinationT, namespace string) {
	t.Helper()
	columns := model.TableSchema{}
	maps.Copy(columns, warehouseutils.DiscardsSchema)
	columns["loaded_at"] = "datetime"
	schema, err := m.FetchSchema(context.Background())
	require.NoError(t, err)
	if len(schema[warehouseutils.DiscardsTable]) == 0 {
		require.NoError(t, m.CreateTable(context.Background(), warehouseutils.DiscardsTable, columns), "SQL purpose: create rudder_discards table")
	}
	stamp := time.Now().UTC()
	row := map[string]any{
		"table_name": "tracks", "row_id": "integration_discard", "column_name": "received_at",
		"column_value": "bad-date", "reason": "invalid datetime",
		"uuid_ts":     stamp.Format("2006-01-02 15:04:05"),
		"received_at": stamp.Format("2006-01-02 15:04:05.999999"),
		"loaded_at":   stamp.Format("2006-01-02 15:04:05.999999"),
	}
	require.NoError(t, m.Setup(context.Background(), m.warehouse, integrationUploader{files: []warehouseutils.LoadFile{integrationUploadRow(t, dest, warehouseutils.DiscardsTable, row)}}))
	stats, err := m.LoadTable(context.Background(), warehouseutils.DiscardsTable)
	require.NoError(t, err, "SQL purpose: LOAD rudder_discards folder")
	if stats.RowsInserted >= 0 {
		require.EqualValues(t, 1, stats.RowsInserted)
	}
	assertIntegrationCount(t, m, namespace, warehouseutils.DiscardsTable, "row_id", "integration_discard", 1)
	assertIntegrationViewCount(t, m, namespace, "rudder_discards_view", "row_id", "integration_discard", 1)
}

func cleanupIntegrationSchema(t *testing.T, m *integrationRuntime, namespace string) {
	t.Helper()
	if m.service == nil {
		return
	}
	var views, tables []string
	odps.NewTables(m.service, m.service.DefaultProjectName(), namespace).List(func(table *odps.Table, err error) {
		if err != nil {
			t.Logf("list MaxCompute cleanup tables: %v", client.ODPSSafeError(err))
			return
		}
		if table == nil || table.Load() != nil {
			return
		}
		if table.Type() == odps.VirtualView || table.Type() == odps.MaterializedView {
			views = append(views, table.Name())
		} else {
			tables = append(tables, table.Name())
		}
	})
	for _, item := range append(views, tables...) {
		name := fmt.Sprintf("`%s`.`%s`.`%s`", m.service.DefaultProjectName(), namespace, item)
		verb := "DROP TABLE IF EXISTS "
		if slices.Contains(views, item) {
			verb = "DROP VIEW IF EXISTS "
		}
		if _, err := client.RunODPSSQL(context.Background(), m.service, verb+name, 5*time.Minute, time.Second); err != nil {
			t.Logf("drop MaxCompute cleanup table: %v", err)
		}
	}
	if err := odps.NewSchemas(m.service, m.service.DefaultProjectName()).Delete(namespace); err != nil {
		t.Logf("drop MaxCompute integration schema: %v", client.ODPSSafeError(err))
	}
}
