package manager

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/gofrs/uuid"
	. "github.com/onsi/gomega"
	"github.com/ory/dockertest"
	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/config"
	backendConfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/stash"
	"github.com/rudderlabs/rudder-server/router"
	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/services/archiver"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

var (
	hold   bool
	DB_DSN = "root@tcp(127.0.0.1:3306)/service"
	db     *sql.DB
)

func TestMain(m *testing.M) {
	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.Parse()

	// hack to make defer work, without being affected by the os.Exit in TestMain
	os.Exit(run(m))
}

func run(m *testing.M) int {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	database := "jobsdb"
	// pulls an image, creates a container based on it and runs it
	resourcePostgres, err := pool.Run("postgres", "11-alpine", []string{
		"POSTGRES_PASSWORD=password",
		"POSTGRES_DB=" + database,
		"POSTGRES_USER=rudder",
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err := pool.Purge(resourcePostgres); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	DB_DSN = fmt.Sprintf("postgres://rudder:password@localhost:%s/%s?sslmode=disable", resourcePostgres.GetPort("5432/tcp"), database)
	fmt.Println("DB_DSN:", DB_DSN)
	os.Setenv("JOBS_DB_DB_NAME", database)
	os.Setenv("JOBS_DB_HOST", "localhost")
	os.Setenv("JOBS_DB_NAME", "jobsdb")
	os.Setenv("JOBS_DB_USER", "rudder")
	os.Setenv("JOBS_DB_PASSWORD", "password")
	os.Setenv("JOBS_DB_PORT", resourcePostgres.GetPort("5432/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		db, err = sql.Open("postgres", DB_DSN)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	code := m.Run()
	blockOnHold()

	return code
}

func blockOnHold() {
	if !hold {
		return
	}

	fmt.Println("Test on hold, before cleanup")
	fmt.Println("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
}

const (
	WriteKeyEnabled           = "enabled-write-key"
	SourceIDEnabled           = "enabled-source"
	GADestinationDefinitionID = "gaId1"
	GADestinationID           = "did1"
)

var (
	workspaceID = uuid.Must(uuid.NewV4()).String()

	gaDestinationDefinition = backendConfig.DestinationDefinitionT{
		ID:            GADestinationDefinitionID,
		Name:          "GA",
		DisplayName:   "Google Analytics",
		Config:        nil,
		ResponseRules: nil,
	}

	sampleBackendConfig = backendConfig.ConfigT{
		Sources: []backendConfig.SourceT{
			{
				WorkspaceID: workspaceID,
				ID:          SourceIDEnabled,
				WriteKey:    WriteKeyEnabled,
				Enabled:     true,
				Destinations: []backendConfig.DestinationT{
					{
						ID:                    GADestinationID,
						Name:                  "ga dest",
						DestinationDefinition: gaDestinationDefinition,
						Enabled:               true,
						IsProcessorEnabled:    true,
					},
				},
			},
		},
	}

	CustomVal map[string]string = map[string]string{"GA": "GA"}
)

func initRouter() {
	config.Load()
	logger.Init()
	stash.Init()
	admin.Init()
	jobsdb.Init()
	jobsdb.Init2()
	jobsdb.Init3()
	archiver.Init()
	router.Init()
	router.Init2()
	batchrouter.Init()
	batchrouter.Init2()
}

func TestRouterManager(t *testing.T) {
	RegisterTestingT(t)
	initRouter()
	router.RoutersManagerSetup()
	batchrouter.BatchRoutersManagerSetup()
	stats.Setup()

	ctx := context.Background()
	r := NewRouterManager(ctx)

	//asyncHelper := testutils.AsyncTestHelper{}
	//asyncHelper.Setup()
	//mockCtrl := gomock.NewController(t)
	//mockMultitenantHandle := mocksMultitenant.NewMockMultiTenantI(mockCtrl)
	//mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)
	//mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendConfig.TopicBackendConfig).
	//	Do(func(channel chan utils.DataEvent, topic backendConfig.Topic) {
	//		// emulate a backend configuration event
	//		go func() { channel <- utils.DataEvent{Data: sampleBackendConfig, Topic: string(topic)} }()
	//	}).
	//	Do(asyncHelper.ExpectAndNotifyCallbackWithName("backend_config")).
	//	Return().Times(1)
	//mockNetHandle := mocksRouter.NewMockNetHandleI(mockCtrl)
	//mockMultitenantHandle.EXPECT().UpdateWorkspaceLatencyMap(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	//gaPayload := `{"body": {"XML": {}, "FORM": {}, "JSON": {}}, "type": "REST", "files": {}, "method": "POST", "params": {"t": "event", "v": "1", "an": "RudderAndroidClient", "av": "1.0", "ds": "android-sdk", "ea": "Demo Track", "ec": "Demo Category", "el": "Demo Label", "ni": 0, "qt": 59268380964, "ul": "en-US", "cid": "anon_id", "tid": "UA-185645846-1", "uip": "[::1]", "aiid": "com.rudderlabs.android.sdk"}, "userId": "anon_id", "headers": {}, "version": "1", "endpoint": "https://www.google-analytics.com/collect"}`
	//parameters := fmt.Sprintf(`{"source_id": "1fMCVYZboDlYlauh4GFsEo2JU77", "destination_id": "%s", "message_id": "2f548e6d-60f6-44af-a1f4-62b3272445c3", "received_at": "2021-06-28T10:04:48.527+05:30", "transform_at": "processor"}`, GADestinationID)
	//var toRetryJobsList = []*jobsdb.JobT{
	//	{
	//		UUID:         uuid.Must(uuid.NewV4()),
	//		UserID:       "u1",
	//		JobID:        2009,
	//		CreatedAt:    time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
	//		ExpireAt:     time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
	//		CustomVal:    CustomVal["GA"],
	//		EventPayload: []byte(gaPayload),
	//		LastJobStatus: jobsdb.JobStatusT{
	//			AttemptNum:    1,
	//			ErrorResponse: []byte(`{"firstAttemptedAt": "2021-06-28T15:57:30.742+05:30"}`),
	//		},
	//		Parameters:  []byte(parameters),
	//		WorkspaceId: workspaceID,
	//	},
	//}
	//
	//var unprocessedJobsList = []*jobsdb.JobT{
	//	{
	//		UUID:         uuid.Must(uuid.NewV4()),
	//		UserID:       "u1",
	//		JobID:        2010,
	//		CreatedAt:    time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
	//		ExpireAt:     time.Date(2020, 04, 28, 13, 26, 00, 00, time.UTC),
	//		CustomVal:    CustomVal["GA"],
	//		EventPayload: []byte(gaPayload),
	//		LastJobStatus: jobsdb.JobStatusT{
	//			AttemptNum: 0,
	//		},
	//		Parameters:  []byte(parameters),
	//		WorkspaceId: workspaceID,
	//	},
	//}
	//
	//allJobs := append(toRetryJobsList, unprocessedJobsList...)
	//var workspaceCount = map[string]int{}
	//workspaceCount[workspaceID] = len(unprocessedJobsList) + len(toRetryJobsList)
	//workspaceCountOut := workspaceCount
	r.StartNew()
	time.Sleep(5*time.Second)
	r.Stop()
}
