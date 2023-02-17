package deltalake_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake/client"
	"google.golang.org/grpc"

	proto "github.com/rudderlabs/rudder-server/proto/databricks"

	"github.com/rudderlabs/rudder-server/warehouse/validations"

	"github.com/rudderlabs/rudder-server/utils/misc"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/testhelper"
)

func TestIntegrationDeltalake(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}
	if _, exists := os.LookupEnv(testhelper.DeltalakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.DeltalakeIntegrationTestCredentials)
	}

	t.Parallel()

	deltalake.Init()

	credentials, err := testhelper.DatabricksCredentials()
	require.NoError(t, err)

	dl := deltalake.NewDeltalake()
	deltalake.WithConfig(dl, config.Default)

	db, err := dl.NewClient(&credentials, 0)
	require.NoError(t, err)

	var (
		jobsDB   = testhelper.SetUpJobsDB(t)
		provider = warehouseutils.DELTALAKE
		schema   = testhelper.Schema(provider, testhelper.DeltalakeIntegrationTestSchema)
	)

	t.Cleanup(func() {
		require.NoError(t,
			testhelper.WithConstantBackoff(func() (err error) {
				dropSchemaResponse, err := db.Client.Execute(db.Context, &proto.ExecuteRequest{
					Config:       db.CredConfig,
					Identifier:   db.CredIdentifier,
					SqlStatement: fmt.Sprintf(`DROP SCHEMA %[1]s CASCADE;`, schema),
				})
				if err != nil {
					return fmt.Errorf("failed dropping schema %s for Deltalake, error: %s", schema, err.Error())
				}
				if dropSchemaResponse.GetErrorCode() != "" {
					return fmt.Errorf("failed dropping schema %s for Deltalake, errorCode: %s, errorMessage: %s", schema, dropSchemaResponse.GetErrorCode(), dropSchemaResponse.GetErrorMessage())
				}
				return
			}),
		)
	})

	testCases := []struct {
		name               string
		schema             string
		writeKey           string
		sourceID           string
		destinationID      string
		messageID          string
		warehouseEventsMap testhelper.EventsCountMap
		prerequisite       func(t testing.TB)
	}{
		{
			name:               "Merge Mode",
			writeKey:           "sToFgoilA0U1WxNeW1gdgUVDsEW",
			schema:             schema,
			sourceID:           "25H5EpYzojqQSepRSaGBrrPx3e4",
			destinationID:      "25IDjdnoEus6DDNrth3SWO1FOpu",
			warehouseEventsMap: mergeEventsMap(),
			prerequisite: func(t testing.TB) {
				t.Helper()
				testhelper.SetConfig(t, []warehouseutils.KeyValue{
					{
						Key:   "Warehouse.deltalake.loadTableStrategy",
						Value: "MERGE",
					},
				})
			},
		},
		{
			name:               "Append Mode",
			writeKey:           "sToFgoilA0U1WxNeW1gdgUVDsEW",
			schema:             schema,
			sourceID:           "25H5EpYzojqQSepRSaGBrrPx3e4",
			destinationID:      "25IDjdnoEus6DDNrth3SWO1FOpu",
			warehouseEventsMap: appendEventsMap(),
			prerequisite: func(t testing.TB) {
				t.Helper()
				testhelper.SetConfig(t, []warehouseutils.KeyValue{
					{
						Key:   "Warehouse.deltalake.loadTableStrategy",
						Value: "APPEND",
					},
				})
			},
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			ts := testhelper.WareHouseTest{
				Schema:        tc.schema,
				WriteKey:      tc.writeKey,
				SourceID:      tc.sourceID,
				DestinationID: tc.destinationID,
				Prerequisite:  tc.prerequisite,
				JobsDB:        jobsDB,
				Provider:      provider,
				UserID:        testhelper.GetUserId(provider),
				MessageID:     misc.FastUUID().String(),
				Tables:        []string{"identifies", "users", "tracks", "product_track", "pages", "screens", "aliases", "groups"},
				WarehouseEventsMap: testhelper.EventsCountMap{
					"identifies":    1,
					"users":         1,
					"tracks":        1,
					"product_track": 1,
					"pages":         1,
					"screens":       1,
					"aliases":       1,
					"groups":        1,
				},
				Client: &warehouseclient.Client{
					DeltalakeClient: db,
					Type:            warehouseclient.DeltalakeClient,
				},
				StatsToVerify: []string{
					"warehouse_deltalake_grpcExecTime",
					"warehouse_deltalake_healthTimeouts",
				},
			}
			ts.VerifyEvents(t)

			ts.WarehouseEventsMap = tc.warehouseEventsMap
			ts.VerifyModifiedEvents(t)
		})
	}
}

func TestConfigurationValidationDeltalake(t *testing.T) {
	if os.Getenv("SLOW") == "0" {
		t.Skip("Skipping tests. Remove 'SLOW=0' env var to run them.")
	}
	if _, exists := os.LookupEnv(testhelper.DeltalakeIntegrationTestCredentials); !exists {
		t.Skipf("Skipping %s as %s is not set", t.Name(), testhelper.DeltalakeIntegrationTestCredentials)
	}

	t.Parallel()

	misc.Init()
	validations.Init()
	warehouseutils.Init()
	deltalake.Init()

	configurations := testhelper.PopulateTemplateConfigurations()
	destination := backendconfig.DestinationT{
		ID: "25IDjdnoEus6DDNrth3SWO1FOpu",
		Config: map[string]interface{}{
			"host":            configurations["deltalakeHost"],
			"port":            configurations["deltalakePort"],
			"path":            configurations["deltalakePath"],
			"token":           configurations["deltalakeToken"],
			"namespace":       configurations["deltalakeNamespace"],
			"bucketProvider":  "AZURE_BLOB",
			"containerName":   configurations["deltalakeContainerName"],
			"prefix":          "",
			"useSTSTokens":    false,
			"enableSSE":       false,
			"accountName":     configurations["deltalakeAccountName"],
			"accountKey":      configurations["deltalakeAccountKey"],
			"syncFrequency":   "30",
			"eventDelivery":   false,
			"eventDeliveryTS": 1648195480174,
		},
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:          "23HLpnDJnIg7DsBvDWGU6DQzFEo",
			Name:        "DELTALAKE",
			DisplayName: "Databricks (Delta Lake)",
		},
		Name:       "deltalake-demo",
		Enabled:    true,
		RevisionID: "29eClxJQQlaWzMWyqnQctFDP5T2",
	}
	testhelper.VerifyConfigurationTest(t, destination)
}

func mergeEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"identifies":    1,
		"users":         1,
		"tracks":        1,
		"product_track": 1,
		"pages":         1,
		"screens":       1,
		"aliases":       1,
		"groups":        1,
	}
}

func appendEventsMap() testhelper.EventsCountMap {
	return testhelper.EventsCountMap{
		"identifies":    2,
		"users":         2,
		"tracks":        2,
		"product_track": 2,
		"pages":         2,
		"screens":       2,
		"aliases":       2,
		"groups":        2,
	}
}

type MockClient struct {
	executeSQLRegex      func(query string) (bool, error)
	connectRes           *proto.ConnectResponse
	executeRes           *proto.ExecuteResponse
	executeQueryRes      *proto.ExecuteQueryResponse
	schemasRes           *proto.FetchSchemasResponse
	tableRes             *proto.FetchTablesResponse
	tableAttributesRes   *proto.FetchTableAttributesResponse
	totalCountInTableRes *proto.FetchTotalCountInTableResponse
	partitionRes         *proto.FetchPartitionColumnsResponse
	closeRes             *proto.CloseResponse
	mockError            error
}

func (m *MockClient) Connect(context.Context, *proto.ConnectRequest, ...grpc.CallOption) (*proto.ConnectResponse, error) {
	return m.connectRes, m.mockError
}

func (m *MockClient) Execute(_ context.Context, r *proto.ExecuteRequest, _ ...grpc.CallOption) (*proto.ExecuteResponse, error) {
	if m.executeSQLRegex != nil {
		if matched, err := m.executeSQLRegex(r.GetSqlStatement()); matched {
			return nil, err
		}
	}
	return m.executeRes, m.mockError
}

func (m *MockClient) ExecuteQuery(context.Context, *proto.ExecuteQueryRequest, ...grpc.CallOption) (*proto.ExecuteQueryResponse, error) {
	return m.executeQueryRes, m.mockError
}

func (m *MockClient) FetchSchemas(context.Context, *proto.FetchSchemasRequest, ...grpc.CallOption) (*proto.FetchSchemasResponse, error) {
	return m.schemasRes, m.mockError
}

func (m *MockClient) FetchTables(context.Context, *proto.FetchTablesRequest, ...grpc.CallOption) (*proto.FetchTablesResponse, error) {
	return m.tableRes, m.mockError
}

func (m *MockClient) FetchTableAttributes(context.Context, *proto.FetchTableAttributesRequest, ...grpc.CallOption) (*proto.FetchTableAttributesResponse, error) {
	return m.tableAttributesRes, m.mockError
}

func (m *MockClient) FetchTotalCountInTable(context.Context, *proto.FetchTotalCountInTableRequest, ...grpc.CallOption) (*proto.FetchTotalCountInTableResponse, error) {
	return m.totalCountInTableRes, m.mockError
}

func (m *MockClient) FetchPartitionColumns(context.Context, *proto.FetchPartitionColumnsRequest, ...grpc.CallOption) (*proto.FetchPartitionColumnsResponse, error) {
	return m.partitionRes, m.mockError
}

func (m *MockClient) Close(context.Context, *proto.CloseRequest, ...grpc.CallOption) (*proto.CloseResponse, error) {
	return m.closeRes, m.mockError
}

func TestDeltalake_CreateTable(t *testing.T) {
	testCases := []struct {
		name           string
		columns        map[string]string
		config         map[string]interface{}
		mockError      error
		mockExecuteRes *proto.ExecuteResponse
		wantError      error
	}{
		{
			name: "No such schema",
			mockExecuteRes: &proto.ExecuteResponse{
				ErrorCode:    "42000",
				ErrorMessage: "test error",
			},
			wantError: errors.New("error while executing with response: test error"),
		},
		{
			name: "With partition",
			columns: map[string]string{
				"received_at": "datetime",
			},
		},
		{
			name: "External location",
			config: map[string]interface{}{
				"enableExternalLocation": true,
				"externalLocation":       "a/b/c/d",
			},
		},
		{
			name:      "Error creating table",
			mockError: errors.New("test error"),
			wantError: errors.New("error while executing: test error"),
		},
	}

	var (
		namespace   = "test-namespace"
		workspaceID = "test-workspace-id"
		testTable   = "test-table"
		testColumns = map[string]string{
			"id":            "string",
			"test_bool":     "boolean",
			"test_datetime": "datetime",
			"test_float":    "float",
			"test_int":      "int",
			"test_string":   "string",
		}
	)

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockClient := &MockClient{
				mockError:  tc.mockError,
				executeRes: tc.mockExecuteRes,
			}

			dl := deltalake.NewDeltalake()
			dl.Namespace = namespace
			dl.Logger = logger.NOP
			dl.Warehouse = warehouseutils.Warehouse{
				Namespace:   namespace,
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: tc.config,
				},
			}
			dl.Client = &client.Client{
				Client: mockClient,
			}

			columns := make(map[string]string)
			for k, v := range tc.columns {
				columns[k] = v
			}
			for k, v := range testColumns {
				columns[k] = v
			}

			err := dl.CreateTable(testTable, columns)
			if tc.wantError != nil {
				require.ErrorContains(t, err, tc.wantError.Error())
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDeltalake_CreateSchema(t *testing.T) {
	var (
		namespace   = "test-namespace"
		workspaceID = "test-workspace-id"
	)

	testCases := []struct {
		name           string
		mockError      error
		mockSchemasRes *proto.FetchSchemasResponse
		wantError      error
	}{
		{
			name: "No such schema",
			mockSchemasRes: &proto.FetchSchemasResponse{
				ErrorCode:    "42000",
				ErrorMessage: "test error",
			},
		},
		{
			name: "Schema already exists",
			mockSchemasRes: &proto.FetchSchemasResponse{
				Databases: []string{namespace},
			},
		},
		{
			name:      "GRPC error while fetching schema",
			mockError: errors.New("test error"),
			wantError: errors.New("fetching schemas: test error"),
		},
		{
			name: "Permission error while fetching schema",
			mockSchemasRes: &proto.FetchSchemasResponse{
				ErrorCode:    "42xxx",
				ErrorMessage: "permission error",
			},
			wantError: errors.New("fetching schemas with response: permission error"),
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockClient := &MockClient{
				mockError:  tc.mockError,
				schemasRes: tc.mockSchemasRes,
			}

			dl := deltalake.NewDeltalake()
			dl.Namespace = namespace
			dl.Logger = logger.NOP
			dl.Warehouse = warehouseutils.Warehouse{
				Type:        "test-type",
				Namespace:   namespace,
				WorkspaceID: workspaceID,
				Source: backendconfig.SourceT{
					ID: "test-source-id",
				},
				Destination: backendconfig.DestinationT{
					ID: "test-destination-id",
				},
			}
			dl.Client = &client.Client{
				Client: mockClient,
			}

			err := dl.CreateSchema()
			if tc.wantError != nil {
				require.ErrorContains(t, err, tc.wantError.Error())
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDeltalake_DropTable(t *testing.T) {
	testCases := []struct {
		name           string
		columns        map[string]string
		config         map[string]interface{}
		mockError      error
		mockExecuteRes *proto.ExecuteResponse
		wantError      error
	}{
		{
			name: "No such table",
			mockExecuteRes: &proto.ExecuteResponse{
				ErrorCode:    "42000",
				ErrorMessage: "test error",
			},
			wantError: errors.New("dropping table with response: test error"),
		},
		{
			name: "Success",
		},
		{
			name:      "GRPC error while dropping table",
			mockError: errors.New("test error"),
			wantError: errors.New("test error"),
		},
		{
			name: "Permission error while dropping table",
			mockExecuteRes: &proto.ExecuteResponse{
				ErrorCode:    "42xxx",
				ErrorMessage: "permission error",
			},
			wantError: errors.New("dropping table with response: permission error"),
		},
	}

	var (
		testTable   = "test-table"
		namespace   = "test-namespace"
		workspaceID = "test-workspace-id"
	)

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockClient := &MockClient{
				mockError:  tc.mockError,
				executeRes: tc.mockExecuteRes,
			}

			dl := deltalake.NewDeltalake()
			dl.Namespace = namespace
			dl.Warehouse = warehouseutils.Warehouse{
				Type:        "test-type",
				Namespace:   namespace,
				WorkspaceID: workspaceID,
				Source: backendconfig.SourceT{
					ID: "test-source-id",
				},
				Destination: backendconfig.DestinationT{
					ID: "test-destination-id",
				},
			}
			dl.Logger = logger.NOP
			dl.Client = &client.Client{
				Client: mockClient,
			}

			err := dl.DropTable(testTable)
			if tc.wantError != nil {
				require.ErrorContains(t, err, tc.wantError.Error())
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDeltalake_AddColumns(t *testing.T) {
	testCases := []struct {
		name           string
		columns        map[string]string
		config         map[string]interface{}
		mockError      error
		mockExecuteRes *proto.ExecuteResponse
		wantError      error
	}{
		{
			name: "Success",
		},
		{
			name:      "GRPC error while dropping table",
			mockError: errors.New("test error"),
			wantError: errors.New("test error"),
		},
		{
			name: "Permission error while dropping table",
			mockExecuteRes: &proto.ExecuteResponse{
				ErrorCode:    "42xxx",
				ErrorMessage: "permission error",
			},
			wantError: errors.New("executing with response: permission error"),
		},
	}

	var (
		namespace   = "test-namespace"
		workspaceID = "test-workspace-id"
		testTable   = "test-table"
		testColumns = []warehouseutils.ColumnInfo{
			{
				Name: "id",
				Type: "string",
			},
			{
				Name: "test_bool",
				Type: "boolean",
			},
		}
	)

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockClient := &MockClient{
				mockError:  tc.mockError,
				executeRes: tc.mockExecuteRes,
			}

			dl := deltalake.NewDeltalake()
			dl.Namespace = namespace
			dl.Logger = logger.NOP
			dl.Warehouse = warehouseutils.Warehouse{
				Type:        "test-type",
				Namespace:   namespace,
				WorkspaceID: workspaceID,
				Source: backendconfig.SourceT{
					ID: "test-source-id",
				},
				Destination: backendconfig.DestinationT{
					ID: "test-destination-id",
				},
			}
			dl.Client = &client.Client{
				Client: mockClient,
			}

			err := dl.AddColumns(testTable, testColumns)
			if tc.wantError != nil {
				require.ErrorContains(t, err, tc.wantError.Error())
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDeltalake_GetTotalCountInTable(t *testing.T) {
	testCases := []struct {
		name                 string
		mockError            error
		totalCountInTableRes *proto.FetchTotalCountInTableResponse
		wantError            error
		count                int
	}{
		{
			name:      "GRPC error while fetching table count",
			mockError: errors.New("test error"),
			wantError: errors.New("fetching table count: test error"),
		},
		{
			name: "Permission error while fetching table count",
			totalCountInTableRes: &proto.FetchTotalCountInTableResponse{
				ErrorCode:    "42xxx",
				ErrorMessage: "permission error",
			},
			wantError: errors.New("fetching table count: permission error"),
		},
		{
			name:  "Success",
			count: 5,
			totalCountInTableRes: &proto.FetchTotalCountInTableResponse{
				Count: 5,
			},
		},
	}

	var (
		namespace   = "test-namespace"
		workspaceID = "test-workspace-id"
		table       = "test-table"
		ctx         = context.TODO()
	)

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockClient := &MockClient{
				mockError:            tc.mockError,
				totalCountInTableRes: tc.totalCountInTableRes,
			}

			dl := deltalake.NewDeltalake()
			dl.Namespace = namespace
			dl.Logger = logger.NOP
			dl.Warehouse = warehouseutils.Warehouse{
				Namespace:   namespace,
				WorkspaceID: workspaceID,
			}
			dl.Client = &client.Client{
				Client: mockClient,
			}

			count, err := dl.GetTotalCountInTable(ctx, table)
			if tc.wantError != nil {
				require.ErrorContains(t, err, tc.wantError.Error())
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, tc.count, count)
		})
	}
}

type mockUploader struct {
	mockError       error
	fileType        string
	fileLocation    string
	uploadSchema    warehouseutils.TableSchemaT
	warehouseSchema warehouseutils.TableSchemaT
	firstEventAt    time.Time
	lastEventAt     time.Time
}

func (*mockUploader) GetSchemaInWarehouse() warehouseutils.SchemaT     { return warehouseutils.SchemaT{} }
func (*mockUploader) GetLocalSchema() warehouseutils.SchemaT           { return warehouseutils.SchemaT{} }
func (*mockUploader) UpdateLocalSchema(_ warehouseutils.SchemaT) error { return nil }
func (*mockUploader) ShouldOnDedupUseNewRecord() bool                  { return false }
func (*mockUploader) UseRudderStorage() bool                           { return false }
func (*mockUploader) GetLoadFileGenStartTIme() time.Time               { return time.Time{} }
func (*mockUploader) GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT) []warehouseutils.LoadFileT {
	return nil
}

func (*mockUploader) GetSingleLoadFile(_ string) (warehouseutils.LoadFileT, error) {
	return warehouseutils.LoadFileT{}, nil
}

func (m *mockUploader) GetFirstLastEvent() (time.Time, time.Time) {
	return m.firstEventAt, m.lastEventAt
}

func (m *mockUploader) GetTableSchemaInUpload(string) warehouseutils.TableSchemaT {
	return m.uploadSchema
}

func (m *mockUploader) GetTableSchemaInWarehouse(_ string) warehouseutils.TableSchemaT {
	return m.warehouseSchema
}

func (m *mockUploader) GetLoadFileType() string {
	return m.fileType
}

func (m *mockUploader) GetSampleLoadFileLocation(_ string) (string, error) {
	return m.fileLocation, m.mockError
}

func TestDeltalake_LoadTable(t *testing.T) {
	deltalake.Init()
	warehouseutils.Init()
	misc.Init()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		minioResource *destination.MINIOResource
		workspaceID   = "test_workspace_id"
		namespace     = "test-namespace"
		testTable     = "test-table"
	)

	g := errgroup.Group{}
	g.Go(func() error {
		minioResource, err = destination.SetupMINIO(pool, t)
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, g.Wait())

	testCases := []struct {
		name              string
		mockClientError   error
		wantError         error
		loadFileType      string
		loadTableStrategy string
		partitionPruning  bool
		useSTSTokens      bool
		partitionResponse *proto.FetchPartitionColumnsResponse
		executeSQLRegex   func(query string) (bool, error)
		executeResponse   *proto.ExecuteResponse
		config            map[string]interface{}
		mockUploaderError error
		namespace         string
	}{
		{
			name:            "Permission error for create table",
			mockClientError: errors.New("permission error"),
			wantError:       errors.New("error while executing: permission error"),
		},
		{
			name:         "Load file type parquet",
			loadFileType: "parquet",
		},
		{
			name:         "Load file type csv",
			loadFileType: "csv",
		},
		{
			name:              "Append mode",
			loadFileType:      "csv",
			loadTableStrategy: "APPEND",
		},
		{
			name:              "Merge mode",
			loadFileType:      "csv",
			loadTableStrategy: "MERGE",
		},
		{
			name:              "Error dropping staging table",
			loadFileType:      "csv",
			loadTableStrategy: "MERGE",
			executeSQLRegex: func(query string) (bool, error) {
				matched, err := regexp.MatchString(".*DROP TABLE.*", query)
				require.NoError(t, err)

				if matched {
					return true, errors.New("drop error")
				}
				return false, nil
			},
		},
		{
			name:              "No load file found",
			loadFileType:      "csv",
			loadTableStrategy: "MERGE",
			mockUploaderError: errors.New("no load file found for table"),
			wantError:         errors.New("no load file found for table"),
		},
		{
			name:              "Partitioning pruning supported",
			loadFileType:      "csv",
			loadTableStrategy: "MERGE",
			partitionPruning:  true,
			partitionResponse: &proto.FetchPartitionColumnsResponse{
				Columns: []string{"event_date"},
			},
		},
		{
			name:              "Partitioning pruning not supported",
			loadFileType:      "csv",
			loadTableStrategy: "MERGE",
			partitionPruning:  true,
			partitionResponse: &proto.FetchPartitionColumnsResponse{
				Columns: []string{"test-column"},
			},
		},
		{
			name:              "Use STS tokens",
			loadFileType:      "csv",
			loadTableStrategy: "MERGE",
			useSTSTokens:      true,
			config: map[string]interface{}{
				"useSTSTokens":    true,
				"bucketName":      minioResource.BucketName,
				"accessKeyID":     minioResource.AccessKey,
				"accessKey":       minioResource.SecretKey,
				"secretAccessKey": minioResource.SecretKey,
				"endPoint":        minioResource.Endpoint,
				"forcePathStyle":  true,
				"disableSSL":      true,
				"region":          minioResource.SiteRegion,
				"enableSSE":       false,
			},
		},
		{
			name:              "Copy error",
			loadFileType:      "csv",
			loadTableStrategy: "MERGE",
			useSTSTokens:      true,
			config: map[string]interface{}{
				"useSTSTokens":    true,
				"bucketName":      minioResource.BucketName,
				"accessKeyID":     minioResource.AccessKey,
				"accessKey":       minioResource.SecretKey,
				"secretAccessKey": minioResource.SecretKey,
				"endPoint":        minioResource.Endpoint,
				"forcePathStyle":  true,
				"disableSSL":      true,
				"region":          minioResource.SiteRegion,
				"enableSSE":       false,
			},
			namespace: "test-namespace-copy-error",
			wantError: errors.New(`executing: copy error`),
			executeSQLRegex: func(query string) (bool, error) {
				matched, err := regexp.MatchString(".*COPY.*test-namespace-copy-error.*", query)
				require.NoError(t, err)

				if matched {
					return true, errors.New("copy error")
				}
				return false, nil
			},
		},
		{
			name:              "Load error with merge",
			loadFileType:      "csv",
			loadTableStrategy: "MERGE",
			useSTSTokens:      true,
			config: map[string]interface{}{
				"useSTSTokens":    true,
				"bucketName":      minioResource.BucketName,
				"accessKeyID":     minioResource.AccessKey,
				"accessKey":       minioResource.SecretKey,
				"secretAccessKey": minioResource.SecretKey,
				"endPoint":        minioResource.Endpoint,
				"forcePathStyle":  true,
				"disableSSL":      true,
				"region":          minioResource.SiteRegion,
				"enableSSE":       false,
			},
			namespace: "test-namespace-load-merge-error",
			executeSQLRegex: func(query string) (bool, error) {
				matched, err := regexp.MatchString(".*MERGE.*test-namespace-load-merge-error.*", query)
				require.NoError(t, err)

				if matched {
					return true, errors.New("load merge error")
				}
				return false, nil
			},
			wantError: errors.New(`executing: load merge error`),
		},
		{
			name:              "Load error with append",
			loadFileType:      "csv",
			loadTableStrategy: "APPEND",
			useSTSTokens:      true,
			config: map[string]interface{}{
				"useSTSTokens":    true,
				"bucketName":      minioResource.BucketName,
				"accessKeyID":     minioResource.AccessKey,
				"accessKey":       minioResource.SecretKey,
				"secretAccessKey": minioResource.SecretKey,
				"endPoint":        minioResource.Endpoint,
				"forcePathStyle":  true,
				"disableSSL":      true,
				"region":          minioResource.SiteRegion,
				"enableSSE":       false,
			},
			namespace: "test-namespace-load-append-error",
			executeSQLRegex: func(query string) (bool, error) {
				matched, _ := regexp.MatchString(".*INSERT.*test-namespace-load-append-error.*", query)
				if matched {
					return true, errors.New("load append error")
				}
				return false, nil
			},
			wantError: errors.New(`executing: load append error`),
		},
	}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockClient := &MockClient{
				mockError:       tc.mockClientError,
				partitionRes:    tc.partitionResponse,
				executeRes:      tc.executeResponse,
				executeSQLRegex: tc.executeSQLRegex,
			}

			conf := config.New()
			conf.Set("Warehouse.deltalake.loadTableStrategy", tc.loadTableStrategy)
			conf.Set("Warehouse.deltalake.enablePartitionPruning", tc.partitionPruning)

			dl := deltalake.NewDeltalake()
			deltalake.WithConfig(dl, conf)

			namespace := namespace
			if tc.namespace != "" {
				namespace = tc.namespace
			}

			dl.Namespace = namespace
			dl.Logger = logger.NOP
			dl.ObjectStorage = warehouseutils.MINIO
			dl.Warehouse = warehouseutils.Warehouse{
				Namespace:   namespace,
				WorkspaceID: workspaceID,
				Destination: backendconfig.DestinationT{
					Config: tc.config,
				},
			}
			dl.Client = &client.Client{
				Client: mockClient,
			}
			dl.Uploader = &mockUploader{
				fileType:     tc.loadFileType,
				mockError:    tc.mockUploaderError,
				fileLocation: "https://test-bucket.s3.amazonaws.com/myfolder/test-object.csv",
				uploadSchema: warehouseutils.TableSchemaT{
					"id":            "string",
					"received_at":   "datetime",
					"test_bool":     "boolean",
					"test_datetime": "datetime",
					"test_float":    "float",
					"test_int":      "int",
					"test_string":   "string",
				},
				warehouseSchema: warehouseutils.TableSchemaT{
					"id":                  "string",
					"received_at":         "datetime",
					"test_array_bool":     "array(boolean)",
					"test_array_datetime": "array(datetime)",
					"test_array_float":    "array(float)",
					"test_array_int":      "array(int)",
					"test_array_string":   "array(string)",
				},
				firstEventAt: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				lastEventAt:  time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC),
			}

			err := dl.LoadTable(testTable)
			if tc.wantError != nil {
				require.ErrorContains(t, err, tc.wantError.Error())
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestDeltalake_LoadUserTables(t *testing.T) {
	testCases := []struct {
		name              string
		loadFileType      string
		loadTableStrategy string
	}{
		{
			name:              "Append mode",
			loadFileType:      "csv",
			loadTableStrategy: "APPEND",
		},
		{
			name:              "Merge mode",
			loadFileType:      "csv",
			loadTableStrategy: "MERGE",
		},
	}

	var (
		namespace   = "test-namespace"
		workspaceID = "test-workspace-id"
	)

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockClient := &MockClient{}

			conf := config.New()
			conf.Set("Warehouse.deltalake.loadTableStrategy", tc.loadTableStrategy)

			dl := deltalake.NewDeltalake()
			deltalake.WithConfig(dl, conf)

			dl.Namespace = namespace
			dl.Logger = logger.NOP
			dl.Warehouse = warehouseutils.Warehouse{
				Namespace:   namespace,
				WorkspaceID: workspaceID,
			}
			dl.Client = &client.Client{
				Client: mockClient,
			}
			dl.Uploader = &mockUploader{
				fileType:     tc.loadFileType,
				fileLocation: "http://test-location",
				uploadSchema: warehouseutils.TableSchemaT{
					"id":            "string",
					"received_at":   "datetime",
					"test_bool":     "boolean",
					"test_datetime": "datetime",
					"test_float":    "float",
					"test_int":      "int",
					"test_string":   "string",
				},
				warehouseSchema: warehouseutils.TableSchemaT{
					"id":                  "string",
					"received_at":         "datetime",
					"test_array_bool":     "array(boolean)",
					"test_array_datetime": "array(datetime)",
					"test_array_float":    "array(float)",
					"test_array_int":      "array(int)",
					"test_array_string":   "array(string)",
				},
				firstEventAt: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
				lastEventAt:  time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC),
			}

			errorMap := dl.LoadUserTables()
			require.Equal(t, errorMap, map[string]error{
				warehouseutils.UsersTable:      nil,
				warehouseutils.IdentifiesTable: nil,
			})
		})
	}
}

func TestDeltalake_LoadTestTable(t *testing.T) {
	testCases := []struct {
		name            string
		loadFileType    string
		wantError       error
		mockClientError error
	}{
		{
			name:         "Success with csv",
			loadFileType: "csv",
		},
		{
			name:         "Success with parquet",
			loadFileType: "parquet",
		},
		{
			name:            "Error load data",
			mockClientError: errors.New("load-error"),
			wantError:       errors.New("load-error"),
		},
	}

	var (
		namespace    = "test-namespace"
		workspaceID  = "test-workspace-id"
		testTable    = "test-table"
		testLocation = "http://test-location"
	)

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockClient := &MockClient{
				mockError: tc.mockClientError,
			}

			dl := deltalake.NewDeltalake()
			dl.Namespace = namespace
			dl.Logger = logger.NOP
			dl.Warehouse = warehouseutils.Warehouse{
				Namespace:   namespace,
				WorkspaceID: workspaceID,
			}
			dl.Client = &client.Client{
				Client: mockClient,
			}
			dl.Uploader = &mockUploader{
				fileType:     tc.loadFileType,
				fileLocation: testLocation,
			}

			err := dl.LoadTestTable(testLocation, testTable, map[string]interface{}{}, tc.loadFileType)
			if tc.wantError != nil {
				require.ErrorContains(t, err, tc.wantError.Error())
				return
			}
			require.NoError(t, err)
		})
	}
}
