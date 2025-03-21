package processor

import (
	"context"
	"fmt"
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/trackedusers"
	"github.com/rudderlabs/rudder-server/internal/enricher"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/jsonrs"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mockDedup "github.com/rudderlabs/rudder-server/mocks/services/dedup"
	mockreportingtypes "github.com/rudderlabs/rudder-server/mocks/utils/types"
	"github.com/rudderlabs/rudder-server/processor/isolation"
	ptypes "github.com/rudderlabs/rudder-server/processor/types"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/rsources"
	transformerFeaturesService "github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/tx"
)

// Test constants
const (
	WriteKeyEnabled         = "enabled-write-key"
	WriteKeyEnabledNoUT     = "enabled-write-key-no-ut"
	WriteKeyEnabledTp       = "enabled-write-key-tp"
	WriteKeyEnabledNoUT2    = "enabled-write-key-no-ut2"
	WriteKeyEnabledOnlyUT   = "enabled-write-key-only-ut"
	WriteKeyTransient       = "transient-write-key"
	WriteKeyOneTrustConsent = "write-key-oneTrust-consent"
	WriteKeyGCM             = "write-key-gcm"
	WriteKeyKetchConsent    = "write-key-ketch-consent"

	SourceIDEnabled             = "enabled-source"
	SourceIDEnabledName         = "SourceIDEnabled"
	SourceIDTransient           = "transient-source"
	SourceIDTransientName       = "SourceIDTransient"
	SourceIDEnabledNoUT         = "enabled-source-no-ut"
	SourceIDEnabledNoUTName     = "SourceIDEnabledNoUT"
	SourceIDEnabledTp           = "enabled-source-tp"
	SourceIDEnabledTpName       = "SourceIDEnabledTp"
	SourceIDEnabledOnlyUT       = "enabled-source-only-ut"
	SourceIDEnabledOnlyUTName   = "SourceIDEnabledOnlyUT"
	SourceIDEnabledNoUT2        = "enabled-source-no-ut2"
	SourceIDEnabledNoUT2Name    = "SourceIDEnabledNoUT2"
	SourceIDDisabled            = "disabled-source"
	SourceIDDisabledName        = "SourceIDDisabled"
	SourceIDOneTrustConsent     = "source-id-oneTrust-consent"
	SourceIDOneTrustConsentName = "SourceIDOneTrustConsent"
	SourceIDGCM                 = "source-id-gcm"
	SourceIDGCMName             = "SourceIDGCM"
	SourceIDKetchConsent        = "source-id-ketch-consent"
	SourceIDKetchConsentName    = "SourceIDKetchConsent"

	DestinationIDEnabledA = "enabled-destination-a" // test destination router
	DestinationIDEnabledB = "enabled-destination-b" // test destination batch router
	DestinationIDEnabledC = "enabled-destination-c"
	DestinationIDDisabled = "disabled-destination"
)

// Test variables
var (
	gatewayCustomVal = []string{"GW"}

	sampleWorkspaceID = "some-workspace-id"

	sampleBackendConfig = backendconfig.ConfigT{
		WorkspaceID: sampleWorkspaceID,
		Sources: []backendconfig.SourceT{
			{
				ID:       SourceIDEnabled,
				Name:     SourceIDEnabledName,
				WriteKey: WriteKeyEnabled,
				Enabled:  true,
				Destinations: []backendconfig.DestinationT{
					{
						ID:                 DestinationIDEnabledA,
						Name:               "A",
						Enabled:            true,
						IsProcessorEnabled: true,
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							ID:          "enabled-destination-a-definition",
							Name:        "enabled-destination-a-definition-name",
							DisplayName: "enabled-destination-a-definition-display-name",
						},
					},
					{
						ID:                 DestinationIDEnabledB,
						Name:               "B",
						Enabled:            true,
						IsProcessorEnabled: true,
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							ID:          "enabled-destination-b-definition",
							Name:        "enabled-destination-b-definition-name",
							DisplayName: "enabled-destination-b-definition-display-name",
						},
					},
				},
			},
			{
				ID:       SourceIDDisabled,
				Name:     SourceIDDisabledName,
				WriteKey: "disabled-write-key",
				Enabled:  false,
				Destinations: []backendconfig.DestinationT{
					{
						ID:                 DestinationIDDisabled,
						Name:               "DISABLED",
						Enabled:            false,
						IsProcessorEnabled: true,
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							ID:          "destination-definition-disabled",
							Name:        "destination-definition-disabled-name",
							DisplayName: "destination-definition-disabled-display-name",
						},
					},
				},
			},
		},
	}
)

// mockObserver implements sourceObserver for testing
type mockObserver struct {
	calls []struct {
		source *backendconfig.SourceT
		events []ptypes.TransformerEvent
	}
}

func (m *mockObserver) ObserveSourceEvents(source *backendconfig.SourceT, events []ptypes.TransformerEvent) {
	m.calls = append(m.calls, struct {
		source *backendconfig.SourceT
		events []ptypes.TransformerEvent
	}{source: source, events: events})
}

// mockTrackedUsersReporter implements trackedusers.DataCollector for testing
type mockTrackedUsersReporter struct {
	generateCalls []struct {
		jobs []*jobsdb.JobT
	}
	reportCalls []struct {
		reportedReports []*trackedusers.UsersReport
	}
}

func (m *mockTrackedUsersReporter) ReportUsers(ctx context.Context, reports []*trackedusers.UsersReport, tx *tx.Tx) error {
	m.reportCalls = append(m.reportCalls, struct {
		reportedReports []*trackedusers.UsersReport
	}{reportedReports: reports})
	return nil
}

func (m *mockTrackedUsersReporter) GenerateReportsFromJobs(jobs []*jobsdb.JobT, sourceIdFilter map[string]bool) []*trackedusers.UsersReport {
	m.generateCalls = append(m.generateCalls, struct {
		jobs []*jobsdb.JobT
	}{jobs: jobs})
	return []*trackedusers.UsersReport{}
}

func (m *mockTrackedUsersReporter) MigrateDatabase(dbConn string, conf *config.Config) error {
	return nil
}

// testContext holds all mock objects needed for testing
type testContext struct {
	mockCtrl                 *gomock.Controller
	mockBackendConfig        *mocksBackendConfig.MockBackendConfig
	mockGatewayJobsDB        *mocksJobsDB.MockJobsDB
	mockRouterJobsDB         *mocksJobsDB.MockJobsDB
	mockBatchRouterJobsDB    *mocksJobsDB.MockJobsDB
	mockReadProcErrorsDB     *mocksJobsDB.MockJobsDB
	mockWriteProcErrorsDB    *mocksJobsDB.MockJobsDB
	mockEventSchemasDB       *mocksJobsDB.MockJobsDB
	mockArchivalDB           *mocksJobsDB.MockJobsDB
	MockRsourcesService      *rsources.MockJobService
	MockReportingI           *mockreportingtypes.MockReporting
	MockDedup                *mockDedup.MockDedup
	MockObserver             *mockObserver
	mockTrackedUsersReporter *mockTrackedUsersReporter
}

// setupTest creates and initializes a new testContext
func setupTest(t *testing.T) (*testContext, func()) {
	c := &testContext{}
	c.mockCtrl = gomock.NewController(t)
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockGatewayJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBatchRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockReadProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockWriteProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockEventSchemasDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockArchivalDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.MockRsourcesService = rsources.NewMockJobService(c.mockCtrl)
	c.MockReportingI = mockreportingtypes.NewMockReporting(c.mockCtrl)
	c.MockDedup = mockDedup.NewMockDedup(c.mockCtrl)
	c.MockObserver = &mockObserver{}
	c.mockTrackedUsersReporter = &mockTrackedUsersReporter{}

	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{
				Data: map[string]backendconfig.ConfigT{
					sampleWorkspaceID: sampleBackendConfig,
				},
				Topic: string(topic),
			}
			close(ch)
			return ch
		})

	// crash recovery check
	c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

	return c, func() {
		c.mockCtrl.Finish()
	}
}

// mockEventData represents test event data
type mockEventData struct {
	id                        string
	jobid                     int64
	originalTimestamp         string
	expectedOriginalTimestamp string
	sentAt                    string
	expectedSentAt            string
	expectedReceivedAt        string
	integrations              map[string]bool
}

// Test helper functions
func createBatchPayload(writeKey, receivedAt string, events []mockEventData, createMessagePayload func(mockEventData) string) []byte {
	var messagePayload string
	for _, e := range events {
		if messagePayload != "" {
			messagePayload += ","
		}
		messagePayload += createMessagePayload(e)
	}
	return []byte(fmt.Sprintf(`{"batch":[%s],"receivedAt":%q,"writeKey":%q}`, messagePayload, receivedAt, writeKey))
}

func createMessagePayload(e mockEventData) string {
	integrationsBytes, _ := jsonrs.Marshal(e.integrations)
	return fmt.Sprintf(`
		{
		  "rudderId": "some-rudder-id",
		  "messageId": "message-%[1]s",
		  "some-property": "property-%[1]s",
		  "originalTimestamp": %[2]q,
		  "sentAt": %[3]q,
		  "integrations": %[4]s
		}
	`,
		e.id,
		e.originalTimestamp,
		e.sentAt,
		string(integrationsBytes),
	)
}

func createMessagePayloadWithoutSources(e mockEventData) string {
	integrationsBytes, _ := jsonrs.Marshal(e.integrations)
	return fmt.Sprintf(`
		{
		  "rudderId": "some-rudder-id",
		  "messageId": "message-%[1]s",
		  "some-property": "property-%[1]s",
		  "originalTimestamp": %[2]q,
		  "sentAt": %[3]q,
		  "integrations": %[4]s
		}
	`,
		e.id,
		e.originalTimestamp,
		e.sentAt,
		string(integrationsBytes),
	)
}

func createBatchParameters(sourceId string) []byte {
	return []byte(fmt.Sprintf(`{"source_id": %q}`, sourceId))
}

func createBatchParametersWithSources() []byte {
	return []byte(fmt.Sprintf(`{"source_id": %q}`, SourceIDEnabledNoUT))
}

// initProcessor initializes processor dependencies
func initProcessor() {
	config.Reset()
	logger.Reset()
	admin.Init()
	misc.Init()
}

// setDisableDedupFeature overrides SetDisableDedupFeature configuration and returns previous value
func setDisableDedupFeature(proc *Handle, b bool) bool {
	prev := proc.config.enableDedup
	proc.config.enableDedup = b
	return prev
}

// Setup initializes a processor handle with test dependencies
func Setup(processor *Handle, c *testContext, enableDedup, enableReporting bool) {
	setDisableDedupFeature(processor, enableDedup)
	err := processor.Setup(
		c.mockBackendConfig,
		c.mockGatewayJobsDB,
		c.mockRouterJobsDB,
		c.mockBatchRouterJobsDB,
		c.mockReadProcErrorsDB,
		c.mockWriteProcErrorsDB,
		c.mockEventSchemasDB,
		c.mockArchivalDB,
		c.MockReportingI,
		transientsource.NewStaticService([]string{SourceIDTransient}),
		fileuploader.NewDefaultProvider(),
		c.MockRsourcesService,
		transformerFeaturesService.NewNoOpService(),
		destinationdebugger.NewNoOpService(),
		transformationdebugger.NewNoOpService(),
		[]enricher.PipelineEnricher{},
		c.mockTrackedUsersReporter,
	)
	if err != nil {
		panic(err)
	}
	processor.reportingEnabled = enableReporting
	processor.sourceObservers = []sourceObserver{c.MockObserver}
}

// prepareHandle prepares a processor handle for testing
func prepareHandle(proc *Handle) *Handle {
	isolationStrategy, err := isolation.GetStrategy(isolation.ModeNone)
	if err != nil {
		panic(err)
	}
	proc.isolationStrategy = isolationStrategy
	return proc
}
