package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-server/admin"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/trackedusers"
	"github.com/rudderlabs/rudder-server/internal/enricher"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	mocksTransformer "github.com/rudderlabs/rudder-server/mocks/processor/transformer"
	mockDedup "github.com/rudderlabs/rudder-server/mocks/services/dedup"
	mock_features "github.com/rudderlabs/rudder-server/mocks/services/transformer"
	mockReportingTypes "github.com/rudderlabs/rudder-server/mocks/utils/types"
	"github.com/rudderlabs/rudder-server/processor/isolation"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	transformationdebugger "github.com/rudderlabs/rudder-server/services/debugger/transformation"
	dedupTypes "github.com/rudderlabs/rudder-server/services/dedup/types"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/services/rsources"
	transformerFeaturesService "github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
)

type mockObserver struct {
	calls []struct {
		source *backendconfig.SourceT
		events []transformer.TransformerEvent
	}
}

func (m *mockObserver) ObserveSourceEvents(source *backendconfig.SourceT, events []transformer.TransformerEvent) {
	m.calls = append(m.calls, struct {
		source *backendconfig.SourceT
		events []transformer.TransformerEvent
	}{source: source, events: events})
}

type mockTrackedUsersReporter struct {
	generateCalls []struct {
		jobs []*jobsdb.JobT
	}
	reportCalls []struct {
		reportedReports []*trackedusers.UsersReport
	}
}

func (m *mockTrackedUsersReporter) ReportUsers(_ context.Context, reports []*trackedusers.UsersReport, _ *Tx) error {
	m.reportCalls = append(m.reportCalls, struct {
		reportedReports []*trackedusers.UsersReport
	}{reportedReports: reports})
	return nil
}

func (m *mockTrackedUsersReporter) GenerateReportsFromJobs(jobs []*jobsdb.JobT, _ map[string]bool) []*trackedusers.UsersReport {
	m.generateCalls = append(m.generateCalls, struct {
		jobs []*jobsdb.JobT
	}{jobs: jobs})
	return lo.FilterMap(jobs, func(job *jobsdb.JobT, _ int) (*trackedusers.UsersReport, bool) {
		return &trackedusers.UsersReport{
			WorkspaceID: job.WorkspaceId,
			SourceID:    gjson.GetBytes(job.Parameters, "source_id").String(),
		}, true
	})
}

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
	MockReportingI           *mockReportingTypes.MockReporting
	MockDedup                *mockDedup.MockDedup
	MockObserver             *mockObserver
	MockRsourcesService      *rsources.MockJobService
	mockTrackedUsersReporter *mockTrackedUsersReporter
}

func (c *testContext) Setup() {
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)
	c.mockGatewayJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBatchRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockReadProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockWriteProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockEventSchemasDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockArchivalDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.MockRsourcesService = rsources.NewMockJobService(c.mockCtrl)

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
	c.MockReportingI = mockReportingTypes.NewMockReporting(c.mockCtrl)
	c.MockDedup = mockDedup.NewMockDedup(c.mockCtrl)
	c.MockObserver = &mockObserver{}
	c.mockTrackedUsersReporter = &mockTrackedUsersReporter{}
}

func (c *testContext) Finish() {
	c.mockCtrl.Finish()
}

const (
	WriteKeyEnabled           = "enabled-write-key"
	WriteKeyEnabledNoUT       = "enabled-write-key-no-ut"
	WriteKeyEnabledTp         = "enabled-write-key-tp"
	WriteKeyEnabledNoUT2      = "enabled-write-key-no-ut2"
	WriteKeyEnabledOnlyUT     = "enabled-write-key-only-ut"
	SourceIDEnabled           = "enabled-source"
	SourceIDEnabledName       = "SourceIDEnabled"
	SourceIDTransient         = "transient-source"
	SourceIDTransientName     = "SourceIDTransient"
	WriteKeyTransient         = "transient-write-key"
	SourceIDEnabledNoUT       = "enabled-source-no-ut"
	SourceIDEnabledNoUTName   = "SourceIDEnabledNoUT"
	SourceIDEnabledTp         = "enabled-source-tp"
	SourceIDEnabledTpName     = "SourceIDEnabledTp"
	SourceIDEnabledOnlyUT     = "enabled-source-only-ut"
	SourceIDEnabledOnlyUTName = "SourceIDEnabledOnlyUT"
	SourceIDEnabledNoUT2      = "enabled-source-no-ut2"
	SourceIDEnabledNoUT2Name  = "SourceIDEnabledNoUT2"
	SourceIDDisabled          = "disabled-source"
	SourceIDDisabledName      = "SourceIDDisabled"
	DestinationIDEnabledA     = "enabled-destination-a" // test destination router
	DestinationIDEnabledB     = "enabled-destination-b" // test destination batch router
	DestinationIDEnabledC     = "enabled-destination-c"
	DestinationIDDisabled     = "disabled-destination"

	SourceIDOneTrustConsent     = "source-id-oneTrust-consent"
	SourceIDOneTrustConsentName = "SourceIDOneTrustConsent"
	SourceIDGCM                 = "source-id-gcm"
	SourceIDGCMName             = "SourceIDGCM"
	WriteKeyOneTrustConsent     = "write-key-oneTrust-consent"
	WriteKeyGCM                 = "write-key-gcm"

	SourceIDKetchConsent     = "source-id-ketch-consent"
	SourceIDKetchConsentName = "SourceIDKetchConsent"
	WriteKeyKetchConsent     = "write-key-ketch-consent"
)

var (
	gatewayCustomVal = []string{"GW"}
	emptyJobsList    []*jobsdb.JobT

	sourceIDToName = map[string]string{
		SourceIDEnabled:         SourceIDEnabledName,
		SourceIDEnabledNoUT:     SourceIDEnabledNoUTName,
		SourceIDEnabledOnlyUT:   SourceIDEnabledOnlyUTName,
		SourceIDEnabledNoUT2:    SourceIDEnabledNoUT2Name,
		SourceIDDisabled:        SourceIDDisabledName,
		SourceIDOneTrustConsent: SourceIDOneTrustConsentName,
		SourceIDGCM:             SourceIDGCMName,
		SourceIDKetchConsent:    SourceIDKetchConsentName,
		SourceIDTransient:       SourceIDTransientName,
		SourceIDEnabledTp:       SourceIDEnabledTpName,
	}
)

// SetDisableDedupFeature overrides SetDisableDedupFeature configuration and returns previous value
func setDisableDedupFeature(proc *Handle, b bool) bool {
	prev := proc.config.enableDedup
	proc.config.enableDedup = b
	return prev
}

func setMainLoopTimeout(proc *Handle, timeout time.Duration) {
	proc.config.mainLoopTimeout = timeout
}

var sampleWorkspaceID = "some-workspace-id"

// This configuration is assumed by all processor tests and, is returned on Subscribe of mocked backend config
var sampleBackendConfig = backendconfig.ConfigT{
	WorkspaceID: sampleWorkspaceID,
	Sources: []backendconfig.SourceT{
		{
			ID:       SourceIDDisabled,
			Name:     SourceIDDisabledName,
			WriteKey: WriteKeyEnabled,
			Enabled:  false,
		},
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
						ID:          "enabled-destination-a-definition-id",
						Name:        "enabled-destination-a-definition-name",
						DisplayName: "enabled-destination-a-definition-display-name",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 DestinationIDEnabledB,
					Name:               "B",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-b-definition-id",
						Name:        "MINIO",
						DisplayName: "enabled-destination-b-definition-display-name",
						Config:      map[string]interface{}{},
					},
					Transformations: []backendconfig.TransformationT{
						{
							VersionID: "transformation-version-id",
						},
					},
				},
				{
					ID:                 DestinationIDEnabledC,
					Name:               "C",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-c-definition-id",
						Name:        "WEBHOOK",
						DisplayName: "enabled-destination-c-definition-display-name",
						Config:      map[string]interface{}{"transformAtV1": "none"},
					},
				},
				// This destination should receive no events
				{
					ID:                 DestinationIDDisabled,
					Name:               "C",
					Enabled:            false,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-disabled",
						Name:        "destination-definition-name-disabled",
						DisplayName: "destination-definition-display-name-disabled",
						Config:      map[string]interface{}{},
					},
				},
			},
		},
		{
			ID:       SourceIDEnabledNoUT,
			Name:     SourceIDEnabledNoUTName,
			WriteKey: WriteKeyEnabledNoUT,
			Enabled:  true,
			SourceDefinition: backendconfig.SourceDefinitionT{
				Category: "webhook",
			},
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestinationIDEnabledA,
					Name:               "A",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-a-definition-id",
						Name:        "enabled-destination-a-definition-name",
						DisplayName: "enabled-destination-a-definition-display-name",
						Config:      map[string]interface{}{},
					},
				},
				// This destination should receive no events
				{
					ID:                 DestinationIDDisabled,
					Name:               "C",
					Enabled:            false,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-disabled",
						Name:        "destination-definition-name-disabled",
						DisplayName: "destination-definition-display-name-disabled",
						Config:      map[string]interface{}{},
					},
				},
			},
		},
		{
			ID:       SourceIDEnabledOnlyUT,
			Name:     SourceIDEnabledOnlyUTName,
			WriteKey: WriteKeyEnabledOnlyUT,
			Enabled:  true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestinationIDEnabledB,
					Name:               "B",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-b-definition-id",
						Name:        "MINIO",
						DisplayName: "enabled-destination-b-definition-display-name",
						Config:      map[string]interface{}{},
					},
					Transformations: []backendconfig.TransformationT{
						{
							VersionID: "transformation-version-id",
						},
					},
				},
			},
		},
		{
			ID:       SourceIDEnabledNoUT2,
			Name:     SourceIDEnabledNoUT2Name,
			WriteKey: WriteKeyEnabledNoUT2,
			Enabled:  true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestinationIDEnabledA,
					Name:               "A",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-a-definition-id",
						Name:        "enabled-destination-a-definition-name",
						DisplayName: "enabled-destination-a-definition-display-name",
						Config: map[string]interface{}{
							"supportedMessageTypes": []interface{}{"identify", "track"},
						},
					},
				},
				// This destination should receive no events
				{
					ID:                 DestinationIDDisabled,
					Name:               "C",
					Enabled:            false,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-disabled",
						Name:        "destination-definition-name-disabled",
						DisplayName: "destination-definition-display-name-disabled",
						Config:      map[string]interface{}{},
					},
				},
			},
		},
		{
			ID:          SourceIDOneTrustConsent,
			Name:        SourceIDOneTrustConsentName,
			WriteKey:    WriteKeyOneTrustConsent,
			WorkspaceID: sampleWorkspaceID,
			Enabled:     true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 "dest-id-1",
					Name:               "D1",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{"oneTrustCookieCategory": "category1"},
							map[string]interface{}{"oneTrustCookieCategory": "category2"},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-2",
					Name:               "D2",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{"oneTrustCookieCategory": ""},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-3",
					Name:               "D3",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{"oneTrustCookieCategory": "category1"},
							map[string]interface{}{"oneTrustCookieCategory": "someOtherCategory"},
							map[string]interface{}{"oneTrustCookieCategory": "someOtherCategory2"},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-4",
					Name:               "D4",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{"oneTrustCookieCategory": "category2"},
							map[string]interface{}{"oneTrustCookieCategory": ""},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-5",
					Name:               "D5",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
			},
		},
		{
			ID:          SourceIDGCM,
			Name:        SourceIDGCMName,
			WriteKey:    WriteKeyGCM,
			WorkspaceID: sampleWorkspaceID,
			Enabled:     true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 "gcm-dest-id-6",
					Name:               "D6",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "consent category 2"},
									{"consent": "someOtherCategory"},
								},
							},
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "purpose 4"},
									{"consent": "purpose 2"},
								},
							},
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{"consent": "custom consent category 1"},
									{"consent": "custom consent category 2"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "gcm-dest-id-7",
					Name:               "D7",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"oneTrustCookieCategories": []interface{}{
							map[string]interface{}{"oneTrustCookieCategory": "consent category 1"},
						},
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "consent category 1"},
									{"consent": "consent category 2"},
								},
							},
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "purpose 1"},
									{"consent": "purpose 3"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "gcm-dest-id-8",
					Name:               "D8",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{
									{"consent": "consent category 3"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-9",
					Name:               "D9",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "oneTrust",
								"consents": []map[string]interface{}{},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "gcm-dest-id-10",
					Name:               "D10",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "gcm-dest-id-11",
					Name:               "D11",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "ketch",
								"consents": []map[string]interface{}{
									{"consent": "purpose 1"},
									{"consent": "purpose 2"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "gcm-dest-id-12",
					Name:               "D12",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "or",
								"consents": []map[string]interface{}{
									{"consent": "consent category 2"},
									{"consent": "someOtherCategory"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-13",
					Name:               "D13",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider": "custom",
								"consents": []map[string]interface{}{
									{"consent": "someOtherCategory"},
									{"consent": "consent category 4"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-14",
					Name:               "D14",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"consentManagement": []interface{}{
							map[string]interface{}{
								"provider":           "custom",
								"resolutionStrategy": "and",
								"consents": []map[string]interface{}{
									{"consent": "custom consent category 3"},
									{"consent": "consent category 4"},
								},
							},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
			},
		},
		{
			ID:          SourceIDKetchConsent,
			Name:        SourceIDKetchConsentName,
			WriteKey:    WriteKeyKetchConsent,
			WorkspaceID: sampleWorkspaceID,
			Enabled:     true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 "dest-id-5",
					Name:               "D5",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"ketchConsentPurposes": []interface{}{
							map[string]interface{}{"purpose": "purpose1"},
							map[string]interface{}{"purpose": "purpose2"},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-6",
					Name:               "D6",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"ketchConsentPurposes": []interface{}{
							map[string]interface{}{"purpose": ""},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-7",
					Name:               "D7",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-8",
					Name:               "D8",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"ketchConsentPurposes": []interface{}{
							map[string]interface{}{"purpose": "purpose3"},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 "dest-id-9",
					Name:               "D9",
					Enabled:            true,
					IsProcessorEnabled: true,
					Config: map[string]interface{}{
						"ketchConsentPurposes": []interface{}{
							map[string]interface{}{"purpose": "purpose1"},
							map[string]interface{}{"purpose": "purpose3"},
						},
						"enableServerSideIdentify": false,
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-enabled",
						Name:        "destination-definition-name-enabled",
						DisplayName: "destination-definition-display-name-enabled",
						Config:      map[string]interface{}{},
					},
				},
			},
		},
		{
			ID:        SourceIDTransient,
			Name:      SourceIDTransientName,
			WriteKey:  WriteKeyTransient,
			Enabled:   true,
			Transient: true,
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestinationIDEnabledA,
					Name:               "A",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-a-definition-id",
						Name:        "enabled-destination-a-definition-name",
						DisplayName: "enabled-destination-a-definition-display-name",
						Config:      map[string]interface{}{},
					},
				},
				{
					ID:                 DestinationIDEnabledB,
					Name:               "B",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-b-definition-id",
						Name:        "MINIO",
						DisplayName: "enabled-destination-b-definition-display-name",
						Config:      map[string]interface{}{},
					},
					Transformations: []backendconfig.TransformationT{
						{
							VersionID: "transformation-version-id",
						},
					},
				},
				{
					ID:                 DestinationIDEnabledC,
					Name:               "C",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-c-definition-id",
						Name:        "WEBHOOK",
						DisplayName: "enabled-destination-c-definition-display-name",
						Config:      map[string]interface{}{"transformAtV1": "none"},
					},
				},
				// This destination should receive no events
				{
					ID:                 DestinationIDDisabled,
					Name:               "C",
					Enabled:            false,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "destination-definition-disabled",
						Name:        "destination-definition-name-disabled",
						DisplayName: "destination-definition-display-name-disabled",
						Config:      map[string]interface{}{},
					},
				},
			},
		},
		{
			ID:       SourceIDEnabledTp,
			Name:     SourceIDEnabledTpName,
			WriteKey: WriteKeyEnabledTp,
			Enabled:  true,
			SourceDefinition: backendconfig.SourceDefinitionT{
				Category: "webhook",
			},
			Destinations: []backendconfig.DestinationT{
				{
					ID:                 DestinationIDEnabledA,
					Name:               "A",
					Enabled:            true,
					IsProcessorEnabled: true,
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						ID:          "enabled-destination-a-definition-id",
						Name:        "enabled-destination-a-definition-name",
						DisplayName: "enabled-destination-a-definition-display-name",
						Config:      map[string]interface{}{},
					},
				},
			},
			DgSourceTrackingPlanConfig: backendconfig.DgSourceTrackingPlanConfigT{
				SourceId: SourceIDEnabledTp,
				TrackingPlan: backendconfig.TrackingPlanT{
					Id:      "tracking-plan-id",
					Version: 100,
				},
			},
		},
	},
	Settings: backendconfig.Settings{
		EventAuditEnabled: true,
	},
}

func initProcessor() {
	config.Reset()
	logger.Reset()
	admin.Init()
	misc.Init()
	format.MaxLength = 100000
	format.MaxDepth = 10
}

var _ = Describe("Tracking Plan Validation", Ordered, func() {
	initProcessor()

	var c *testContext
	BeforeEach(func() {
		c = &testContext{}
		c.Setup()
		c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1) // crash recovery check
	})
	AfterEach(func() {
		c.Finish()
	})

	Context("RudderTyper", func() {
		It("Tracking plan id and version from DgSourceTrackingPlanConfig", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Validate(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(transformer.Response{})

			isolationStrategy, err := isolation.GetStrategy(isolation.ModeNone)
			Expect(err).To(BeNil())

			processor := NewHandle(config.Default, mockTransformer)
			processor.isolationStrategy = isolationStrategy
			processor.config.archivalEnabled = config.SingleValueLoader(false)
			Setup(processor, c, false, false)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
			GinkgoT().Log("Processor setup and init done")

			preTransMessage, err := processor.processJobsForDest(
				"",
				subJob{
					subJobs: []*jobsdb.JobT{
						{
							UUID:      uuid.New(),
							JobID:     1,
							CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
							ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
							CustomVal: gatewayCustomVal[0],
							EventPayload: createBatchPayload(
								WriteKeyEnabledTp,
								"2001-01-02T02:23:45.000Z",
								[]mockEventData{
									{
										id:                        "1",
										jobid:                     1,
										originalTimestamp:         "2000-01-02T01:23:45",
										expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
										sentAt:                    "2000-01-02 01:23",
										expectedSentAt:            "2000-01-02T01:23:00.000Z",
										expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
									},
								},
								func(e mockEventData) string {
									return fmt.Sprintf(`
										{
										  "rudderId": "some-rudder-id",
										  "messageId": "message-%[1]s",
										  "some-property": "property-%[1]s",
										  "originalTimestamp": %[2]q,
										  "sentAt": %[3]q
										}
									`,
										e.id,
										e.originalTimestamp,
										e.sentAt,
									)
								},
							),
							EventCount:    1,
							LastJobStatus: jobsdb.JobStatusT{},
							Parameters:    createBatchParameters(SourceIDEnabledTp),
							WorkspaceId:   sampleWorkspaceID,
						},
					},
				},
			)
			Expect(err).To(BeNil())
			_, _ = processor.generateTransformationMessage(preTransMessage)

			Expect(c.MockObserver.calls).To(HaveLen(1))
			for _, v := range c.MockObserver.calls {
				for _, e := range v.events {
					Expect(e.Metadata.TrackingPlanID).To(BeEquivalentTo("tracking-plan-id"))
					Expect(e.Metadata.TrackingPlanVersion).To(BeEquivalentTo(100)) // from DgSourceTrackingPlanConfig
				}
			}
		})
		It("Tracking plan version override from context.ruddertyper", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformer.EXPECT().Validate(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(transformer.Response{})

			isolationStrategy, err := isolation.GetStrategy(isolation.ModeNone)
			Expect(err).To(BeNil())

			processor := NewHandle(config.Default, mockTransformer)
			processor.isolationStrategy = isolationStrategy
			processor.config.archivalEnabled = config.SingleValueLoader(false)
			Setup(processor, c, false, false)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
			GinkgoT().Log("Processor setup and init done")

			preTransMessage, err := processor.processJobsForDest(
				"",
				subJob{
					subJobs: []*jobsdb.JobT{
						{
							UUID:      uuid.New(),
							JobID:     1,
							CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
							ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
							CustomVal: gatewayCustomVal[0],
							EventPayload: createBatchPayload(
								WriteKeyEnabledTp,
								"2001-01-02T02:23:45.000Z",
								[]mockEventData{
									{
										id:                        "1",
										jobid:                     1,
										originalTimestamp:         "2000-01-02T01:23:45",
										expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
										sentAt:                    "2000-01-02 01:23",
										expectedSentAt:            "2000-01-02T01:23:00.000Z",
										expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
									},
								},
								func(e mockEventData) string {
									return fmt.Sprintf(`
										{
										  "rudderId": "some-rudder-id",
										  "messageId": "message-%[1]s",
										  "some-property": "property-%[1]s",
										  "originalTimestamp": %[2]q,
										  "sentAt": %[3]q,
										  "context": {
											"ruddertyper": {
                                              "trackingPlanId": "tracking-plan-id",
											  "trackingPlanVersion": 123
											}
										  }
										}
									`,
										e.id,
										e.originalTimestamp,
										e.sentAt,
									)
								},
							),
							EventCount:    1,
							LastJobStatus: jobsdb.JobStatusT{},
							Parameters:    createBatchParameters(SourceIDEnabledTp),
							WorkspaceId:   sampleWorkspaceID,
						},
					},
				},
			)
			Expect(err).To(BeNil())
			_, _ = processor.generateTransformationMessage(preTransMessage)

			Expect(c.MockObserver.calls).To(HaveLen(1))
			for _, v := range c.MockObserver.calls {
				for _, e := range v.events {
					Expect(e.Metadata.TrackingPlanID).To(BeEquivalentTo("tracking-plan-id"))
					Expect(e.Metadata.TrackingPlanVersion).To(BeEquivalentTo(123)) // Overridden happens when tracking plan id is same in context.ruddertyper and DgSourceTrackingPlanConfig
				}
			}
		})
	})
})

var _ = Describe("Processor with event schemas v2", Ordered, func() {
	initProcessor()

	var c *testContext

	prepareHandle := func(proc *Handle) *Handle {
		proc.eventSchemaDB = c.mockEventSchemasDB
		proc.config.eventSchemaV2Enabled = true
		isolationStrategy, err := isolation.GetStrategy(isolation.ModeNone)
		Expect(err).To(BeNil())
		proc.isolationStrategy = isolationStrategy
		return proc
	}

	BeforeEach(func() {
		c = &testContext{}
		c.Setup()
		// crash recovery check
		c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("event schemas DB", func() {
		It("should process events and write to event schemas DB", func() {
			messages := map[string]mockEventData{
				// this message should be delivered only to destination A
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
				},
				// this message should not be delivered to destination A
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-a-definition-display-name": false},
				},
				// this message should be delivered to all destinations
				"message-3": {
					id:                 "3",
					jobid:              2010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": true},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     2010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              2010,
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": false},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
					WorkspaceId:   sampleWorkspaceID,
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						},
						createMessagePayloadWithoutSources,
					),
					EventCount:    2,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledNoUT),
					WorkspaceId:   sampleWorkspaceID,
				},
				{
					UUID:          uuid.New(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
					WorkspaceId:   sampleWorkspaceID,
				},
				{
					UUID:          uuid.New(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
					WorkspaceId:   sampleWorkspaceID,
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2002-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-3"],
							messages["message-4"],
							messages["message-5"],
						},
						createMessagePayload,
					),
					EventCount:  3,
					Parameters:  createBatchParametersWithSources(SourceIDEnabledNoUT),
					WorkspaceId: sampleWorkspaceID,
				},
			}
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			c.mockEventSchemasDB.EXPECT().
				WithStoreSafeTx(
					gomock.Any(),
					gomock.Any(),
				).Times(1).
				Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
					_ = f(jobsdb.EmptyStoreSafeTx())
				}).Return(nil)
			c.mockEventSchemasDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
				})

			c.mockArchivalDB.EXPECT().
				WithStoreSafeTx(
					gomock.Any(),
					gomock.Any(),
				).Times(1)
			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(0)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			Setup(processor, c, false, false)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
			GinkgoT().Log("Processor setup and init done")
			preTransMessage, err := processor.processJobsForDest(
				"",
				subJob{
					subJobs: unprocessedJobsList,
				},
			)
			Expect(err).To(BeNil())
			_, _ = processor.generateTransformationMessage(preTransMessage)

			Expect(c.MockObserver.calls).To(HaveLen(1))
		})

		It("should process events and write to event schemas DB with enableParallelScan", func() {
			messages := map[string]mockEventData{
				// this message should be delivered only to destination A
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
				},
				// this message should not be delivered to destination A
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-a-definition-display-name": false},
				},
				// this message should be delivered to all destinations
				"message-3": {
					id:                 "3",
					jobid:              2010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": true},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     2010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              2010,
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": false},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
					WorkspaceId:   sampleWorkspaceID,
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						},
						createMessagePayloadWithoutSources,
					),
					EventCount:    2,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledNoUT),
					WorkspaceId:   sampleWorkspaceID,
				},
				{
					UUID:          uuid.New(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
					WorkspaceId:   sampleWorkspaceID,
				},
				{
					UUID:          uuid.New(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
					WorkspaceId:   sampleWorkspaceID,
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2002-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-3"],
							messages["message-4"],
							messages["message-5"],
						},
						createMessagePayload,
					),
					EventCount:  3,
					Parameters:  createBatchParametersWithSources(SourceIDEnabledNoUT),
					WorkspaceId: sampleWorkspaceID,
				},
			}
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			c.mockEventSchemasDB.EXPECT().
				WithStoreSafeTx(
					gomock.Any(),
					gomock.Any(),
				).Times(1).
				Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
					_ = f(jobsdb.EmptyStoreSafeTx())
				}).Return(nil)
			c.mockEventSchemasDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
				})

			c.mockArchivalDB.EXPECT().
				WithStoreSafeTx(
					gomock.Any(),
					gomock.Any(),
				).Times(1)
			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(0)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			Setup(processor, c, false, false)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
			GinkgoT().Log("Processor setup and init done")
			preTransMessage, err := processor.processJobsForDest(
				"",
				subJob{
					subJobs: unprocessedJobsList,
				},
			)
			Expect(err).To(BeNil())
			_, _ = processor.generateTransformationMessage(preTransMessage)

			Expect(c.MockObserver.calls).To(HaveLen(1))
		})
	})
})

var _ = Describe("Processor with ArchivalV2 enabled", Ordered, func() {
	initProcessor()

	var c *testContext

	prepareHandle := func(proc *Handle) *Handle {
		proc.archivalDB = c.mockArchivalDB
		proc.config.archivalEnabled = config.SingleValueLoader(true)
		isolationStrategy, err := isolation.GetStrategy(isolation.ModeNone)
		Expect(err).To(BeNil())
		proc.isolationStrategy = isolationStrategy
		return proc
	}

	BeforeEach(func() {
		c = &testContext{}
		c.Setup()
		// crash recovery check
		c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("archival DB", func() {
		It("should process events and write to archival DB", func() {
			messages := map[string]mockEventData{
				// this message should be delivered only to destination A
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
				},
				// this message should not be delivered to destination A
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-a-definition-display-name": false},
				},
				// this message should be delivered to all destinations
				"message-3": {
					id:                 "3",
					jobid:              2010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": true},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     2010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              2010,
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": false},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						},
						createMessagePayloadWithoutSources, // should be stored
					),
					EventCount:    2,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledNoUT),
				},
				{
					UUID:          uuid.New(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:          uuid.New(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2002-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-3"],
							messages["message-4"],
							messages["message-5"],
						},
						createMessagePayload, // shouldn't be stored to archivedb
					),
					EventCount: 3,
					Parameters: createBatchParametersWithSources(SourceIDEnabledNoUT),
				},
			}
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			c.mockArchivalDB.EXPECT().
				WithStoreSafeTx(
					gomock.Any(),
					gomock.Any(),
				).Times(1).
				Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
					_ = f(jobsdb.EmptyStoreSafeTx())
				}).Return(nil)
			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
				})

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			Setup(processor, c, false, false)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
			GinkgoT().Log("Processor setup and init done")
			preTransMessage, err := processor.processJobsForDest(
				"",
				subJob{
					subJobs: unprocessedJobsList,
				},
			)
			Expect(err).To(BeNil())
			_, _ = processor.generateTransformationMessage(preTransMessage)

			Expect(c.MockObserver.calls).To(HaveLen(1))
		})

		It("should process events and write to archival DB with parallelScan", func() {
			messages := map[string]mockEventData{
				// this message should be delivered only to destination A
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
				},
				// this message should not be delivered to destination A
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-a-definition-display-name": false},
				},
				// this message should be delivered to all destinations
				"message-3": {
					id:                 "3",
					jobid:              2010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": true},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     2010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              2010,
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": false},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						},
						createMessagePayloadWithoutSources, // should be stored
					),
					EventCount:    2,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledNoUT),
				},
				{
					UUID:          uuid.New(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:          uuid.New(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2002-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-3"],
							messages["message-4"],
							messages["message-5"],
						},
						createMessagePayload, // shouldn't be stored to archivedb
					),
					EventCount: 3,
					Parameters: createBatchParametersWithSources(SourceIDEnabledNoUT),
				},
			}
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			c.mockArchivalDB.EXPECT().
				WithStoreSafeTx(
					gomock.Any(),
					gomock.Any(),
				).Times(1).
				Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
					_ = f(jobsdb.EmptyStoreSafeTx())
				}).Return(nil)
			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
				})

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			Setup(processor, c, false, false)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
			GinkgoT().Log("Processor setup and init done")
			preTransMessage, err := processor.processJobsForDest(
				"",
				subJob{
					subJobs: unprocessedJobsList,
				},
			)
			Expect(err).To(BeNil())
			_, _ = processor.generateTransformationMessage(preTransMessage)

			Expect(c.MockObserver.calls).To(HaveLen(1))
		})

		It("should skip writing events belonging to transient sources in archival DB", func() {
			messages := map[string]mockEventData{
				// this message should be delivered only to destination A
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
				},
				// this message should not be delivered to destination A
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-a-definition-display-name": false},
				},
				// this message should be delivered to all destinations
				"message-3": {
					id:                 "3",
					jobid:              2010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": true},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     2010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              2010,
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": false},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyTransient,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						},
						createMessagePayloadWithoutSources,
					),
					EventCount:    2,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDTransient),
				},
				{
					UUID:          uuid.New(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:          uuid.New(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyTransient,
						"2002-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-3"],
							messages["message-4"],
							messages["message-5"],
						},
						createMessagePayload,
					),
					EventCount: 3,
					Parameters: createBatchParameters(SourceIDTransient),
				},
			}
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			c.mockArchivalDB.EXPECT().
				WithStoreSafeTx(
					gomock.Any(),
					gomock.Any(),
				).Times(0)
			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(0)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			Setup(processor, c, false, false)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
			GinkgoT().Log("Processor setup and init done")
			preTransMessage, err := processor.processJobsForDest(
				"",
				subJob{
					subJobs: unprocessedJobsList,
				},
			)
			Expect(err).To(BeNil())
			_, _ = processor.generateTransformationMessage(preTransMessage)

			Expect(c.MockObserver.calls).To(HaveLen(1))
		})
	})
})

var _ = Describe("Processor with trackedUsers feature enabled", Ordered, func() {
	initProcessor()

	var c *testContext

	prepareHandle := func(proc *Handle) *Handle {
		isolationStrategy, err := isolation.GetStrategy(isolation.ModeNone)
		Expect(err).To(BeNil())
		proc.isolationStrategy = isolationStrategy
		return proc
	}
	BeforeEach(func() {
		c = &testContext{}
		c.Setup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("trackedUsers", func() {
		BeforeEach(func() {
			// crash recovery check
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)
		})

		It("should track Users from unprocessed jobs ", func() {
			messages := map[string]mockEventData{
				// this message should be delivered only to destination A
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
					params:                    map[string]string{"source_id": "enabled-source-no-ut"},
				},
				// this message should not be delivered to destination A
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-a-definition-display-name": false},
					params:                    map[string]string{"source_id": "enabled-source-no-ut"},
				},
				// this message should be delivered to all destinations
				"message-3": {
					id:                 "3",
					jobid:              2010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": true},
					params:             map[string]string{"source_id": "enabled-source-no-ut", "source_job_run_id": "job_run_id_1", "source_task_run_id": "task_run_id_1"},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     2010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{},
					params:                    map[string]string{"source_id": "enabled-source-no-ut", "source_job_run_id": "job_run_id_1", "source_task_run_id": "task_run_id_1"},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              2010,
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": false},
					params:             map[string]string{"source_id": "enabled-source-no-ut", "source_job_run_id": "job_run_id_1", "source_task_run_id": "task_run_id_1"},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						}, createMessagePayloadWithoutSources,
					),
					EventCount:    2,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledNoUT),
				},
				{
					UUID:          uuid.New(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:          uuid.New(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2002-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-3"],
							messages["message-4"],
							messages["message-5"],
						},
						createMessagePayloadWithoutSources,
					),
					EventCount: 3,
					Parameters: createBatchParametersWithSources(SourceIDEnabledNoUT),
				},
			}
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))
			processor.trackedUsersReporter = c.mockTrackedUsersReporter

			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(
				gomock.Any(),
				jobsdb.GetQueryParams{
					CustomValFilters: gatewayCustomVal,
					JobsLimit:        processor.config.maxEventsToProcess.Load(),
					EventsLimit:      processor.config.maxEventsToProcess.Load(),
					PayloadSizeLimit: processor.payloadLimit.Load(),
				}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)

			transformExpectations := map[string]transformExpectation{
				DestinationIDEnabledA: {
					events:                    3,
					messageIds:                "message-1,message-3,message-4",
					receiveMetadata:           true,
					destinationDefinitionName: "enabled-destination-a-definition-name",
				},
			}

			// We expect one transform call to destination A, after callUnprocessed.
			mockTransformer.EXPECT().Transform(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).Times(1).After(callUnprocessed).
				DoAndReturn(assertDestinationTransform(
					messages,
					SourceIDEnabledNoUT,
					DestinationIDEnabledA,
					transformExpectations[DestinationIDEnabledA],
				))

			assertStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(string(job.EventPayload)).To(Equal(fmt.Sprintf(`{"int-value":%d,"string-value":%q}`, i, destination)))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				require.JSONEq(GinkgoT(), fmt.Sprintf(`{
					"source_id":"source-from-transformer",
					"source_name": "%s",
					"destination_id":"destination-from-transformer",
					"received_at":"",
					"transform_at":"processor",
					"message_id":"",
					"gateway_job_id":0,
					"source_task_run_id":"",
					"source_job_id":"",
					"source_job_run_id":"",
					"event_name":"",
					"event_type":"",
					"source_definition_id":"",
					"destination_definition_id":"",
					"source_category":"",
					"record_id":null,
					"workspaceId":"",
					"traceparent":""
				}`, sourceIDToName[SourceIDEnabledNoUT]), string(job.Parameters))
			}
			// One Store call is expected for all events
			c.mockRouterJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			callStoreRouter := c.mockRouterJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-a")
					}
				})

			c.MockRsourcesService.EXPECT().
				IncrementStats(
					gomock.Any(),
					gomock.Any(),
					"job_run_id_1",
					rsources.JobTargetKey{
						TaskRunID: "task_run_id_1",
						SourceID:  "enabled-source-no-ut",
					},
					rsources.Stats{In: 2, Failed: 2},
				).Times(1).Return(nil)

			c.MockRsourcesService.EXPECT().
				IncrementStats(
					gomock.Any(),
					gomock.Any(),
					"job_run_id_1",
					rsources.JobTargetKey{
						TaskRunID: "task_run_id_1",
						SourceID:  "enabled-source-no-ut",
					},
					rsources.Stats{Out: 1},
				).Times(1).Return(nil)

			c.mockArchivalDB.EXPECT().
				WithStoreSafeTx(
					gomock.Any(),
					gomock.Any(),
				).Times(1).
				Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
					_ = f(jobsdb.EmptyStoreSafeTx())
				}).Return(nil)
			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
				})

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callStoreRouter).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					// jobs should be sorted by jobid, so order of statuses is different from order of jobs
					for i := range unprocessedJobsList {
						assertJobStatus(unprocessedJobsList[i], statuses[i], jobsdb.Succeeded.State)
					}
				})

			trackerUsersReports := []*trackedusers.UsersReport{
				{
					SourceID: SourceIDEnabled,
				},
				{
					SourceID: SourceIDEnabledNoUT,
				},
				{
					SourceID: SourceIDEnabled,
				},
				{
					SourceID: SourceIDEnabled,
				},
				{
					SourceID: SourceIDEnabledNoUT,
				},
			}
			Setup(processor, c, false, false)
			processor.trackedUsersReporter = c.mockTrackedUsersReporter
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
			GinkgoT().Log("Processor setup and init done")
			handlePendingGatewayJobs(processor)
			Expect(c.mockTrackedUsersReporter.generateCalls).To(HaveLen(1))
			Expect(c.mockTrackedUsersReporter.generateCalls[0].jobs).Should(Equal(unprocessedJobsList))
			Expect(c.mockTrackedUsersReporter.reportCalls).To(HaveLen(1))
			Expect(c.mockTrackedUsersReporter.reportCalls[0].reportedReports).Should(Equal(trackerUsersReports))
		})

		It("should track Users from unprocessed jobs with parallelScan", func() {
			messages := map[string]mockEventData{
				// this message should be delivered only to destination A
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
					params:                    map[string]string{"source_id": "enabled-source-no-ut"},
				},
				// this message should not be delivered to destination A
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-a-definition-display-name": false},
					params:                    map[string]string{"source_id": "enabled-source-no-ut"},
				},
				// this message should be delivered to all destinations
				"message-3": {
					id:                 "3",
					jobid:              2010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": true},
					params:             map[string]string{"source_id": "enabled-source-no-ut", "source_job_run_id": "job_run_id_1", "source_task_run_id": "task_run_id_1"},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     2010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{},
					params:                    map[string]string{"source_id": "enabled-source-no-ut", "source_job_run_id": "job_run_id_1", "source_task_run_id": "task_run_id_1"},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              2010,
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": false},
					params:             map[string]string{"source_id": "enabled-source-no-ut", "source_job_run_id": "job_run_id_1", "source_task_run_id": "task_run_id_1"},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						}, createMessagePayloadWithoutSources,
					),
					EventCount:    2,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledNoUT),
				},
				{
					UUID:          uuid.New(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:          uuid.New(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2002-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-3"],
							messages["message-4"],
							messages["message-5"],
						},
						createMessagePayloadWithoutSources,
					),
					EventCount: 3,
					Parameters: createBatchParametersWithSources(SourceIDEnabledNoUT),
				},
			}
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))
			processor.trackedUsersReporter = c.mockTrackedUsersReporter

			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(
				gomock.Any(),
				jobsdb.GetQueryParams{
					CustomValFilters: gatewayCustomVal,
					JobsLimit:        processor.config.maxEventsToProcess.Load(),
					EventsLimit:      processor.config.maxEventsToProcess.Load(),
					PayloadSizeLimit: processor.payloadLimit.Load(),
				}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)

			transformExpectations := map[string]transformExpectation{
				DestinationIDEnabledA: {
					events:                    3,
					messageIds:                "message-1,message-3,message-4",
					receiveMetadata:           true,
					destinationDefinitionName: "enabled-destination-a-definition-name",
				},
			}

			// We expect one transform call to destination A, after callUnprocessed.
			mockTransformer.EXPECT().Transform(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).Times(1).After(callUnprocessed).
				DoAndReturn(assertDestinationTransform(
					messages,
					SourceIDEnabledNoUT,
					DestinationIDEnabledA,
					transformExpectations[DestinationIDEnabledA],
				))

			assertStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(string(job.EventPayload)).To(Equal(fmt.Sprintf(`{"int-value":%d,"string-value":%q}`, i, destination)))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				require.JSONEq(GinkgoT(), fmt.Sprintf(`{
					"source_id":"source-from-transformer",
					"source_name": "%s",
					"destination_id":"destination-from-transformer",
					"received_at":"",
					"transform_at":"processor",
					"message_id":"",
					"gateway_job_id":0,
					"source_task_run_id":"",
					"source_job_id":"",
					"source_job_run_id":"",
					"event_name":"",
					"event_type":"",
					"source_definition_id":"",
					"destination_definition_id":"",
					"source_category":"",
					"record_id":null,
					"workspaceId":"",
					"traceparent":""
				}`, sourceIDToName[SourceIDEnabledNoUT]), string(job.Parameters))
			}
			// One Store call is expected for all events
			c.mockRouterJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			callStoreRouter := c.mockRouterJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-a")
					}
				})

			c.MockRsourcesService.EXPECT().
				IncrementStats(
					gomock.Any(),
					gomock.Any(),
					"job_run_id_1",
					rsources.JobTargetKey{
						TaskRunID: "task_run_id_1",
						SourceID:  "enabled-source-no-ut",
					},
					rsources.Stats{In: 2, Failed: 2},
				).Times(1).Return(nil)

			c.MockRsourcesService.EXPECT().
				IncrementStats(
					gomock.Any(),
					gomock.Any(),
					"job_run_id_1",
					rsources.JobTargetKey{
						TaskRunID: "task_run_id_1",
						SourceID:  "enabled-source-no-ut",
					},
					rsources.Stats{Out: 1},
				).Times(1).Return(nil)

			c.mockArchivalDB.EXPECT().
				WithStoreSafeTx(
					gomock.Any(),
					gomock.Any(),
				).Times(1).
				Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
					_ = f(jobsdb.EmptyStoreSafeTx())
				}).Return(nil)
			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
				})

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callStoreRouter).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					// jobs should be sorted by jobid, so order of statuses is different from order of jobs
					for i := range unprocessedJobsList {
						assertJobStatus(unprocessedJobsList[i], statuses[i], jobsdb.Succeeded.State)
					}
				})

			trackerUsersReports := []*trackedusers.UsersReport{
				{
					SourceID: SourceIDEnabled,
				},
				{
					SourceID: SourceIDEnabledNoUT,
				},
				{
					SourceID: SourceIDEnabled,
				},
				{
					SourceID: SourceIDEnabled,
				},
				{
					SourceID: SourceIDEnabledNoUT,
				},
			}
			Setup(processor, c, false, false)
			processor.trackedUsersReporter = c.mockTrackedUsersReporter
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
			GinkgoT().Log("Processor setup and init done")
			handlePendingGatewayJobs(processor)
			Expect(c.mockTrackedUsersReporter.generateCalls).To(HaveLen(1))
			Expect(c.mockTrackedUsersReporter.generateCalls[0].jobs).Should(Equal(unprocessedJobsList))
			Expect(c.mockTrackedUsersReporter.reportCalls).To(HaveLen(1))
			Expect(c.mockTrackedUsersReporter.reportCalls[0].reportedReports).Should(Equal(trackerUsersReports))
		})
	})
})

var _ = Describe("Processor", Ordered, func() {
	initProcessor()

	var c *testContext

	prepareHandle := func(proc *Handle) *Handle {
		isolationStrategy, err := isolation.GetStrategy(isolation.ModeNone)
		Expect(err).To(BeNil())
		proc.isolationStrategy = isolationStrategy
		return proc
	}
	BeforeEach(func() {
		c = &testContext{}
		c.Setup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Initialization", func() {
		It("should initialize (no jobs to recover)", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			err := processor.Setup(
				c.mockBackendConfig,
				c.mockGatewayJobsDB,
				c.mockRouterJobsDB,
				c.mockBatchRouterJobsDB,
				c.mockReadProcErrorsDB,
				c.mockWriteProcErrorsDB,
				nil,
				nil,
				nil,
				transientsource.NewEmptyService(),
				fileuploader.NewDefaultProvider(),
				c.MockRsourcesService,
				transformerFeaturesService.NewNoOpService(),
				destinationdebugger.NewNoOpService(),
				transformationdebugger.NewNoOpService(),
				[]enricher.PipelineEnricher{},
				trackedusers.NewNoopDataCollector(),
			)
			Expect(err).To(BeNil())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
		})

		It("should recover after crash", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			err := processor.Setup(
				c.mockBackendConfig,
				c.mockGatewayJobsDB,
				c.mockRouterJobsDB,
				c.mockBatchRouterJobsDB,
				c.mockReadProcErrorsDB,
				c.mockWriteProcErrorsDB,
				nil,
				nil,
				nil,
				transientsource.NewEmptyService(),
				fileuploader.NewDefaultProvider(),
				c.MockRsourcesService,
				transformerFeaturesService.NewNoOpService(),
				destinationdebugger.NewNoOpService(),
				transformationdebugger.NewNoOpService(),
				[]enricher.PipelineEnricher{},
				trackedusers.NewNoopDataCollector(),
			)
			Expect(err).To(BeNil())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
		})
	})

	Context("normal operation", func() {
		BeforeEach(func() {
			// crash recovery check
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)
		})

		It("should only send proper stats, if not pending jobs are returned", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			err := processor.Setup(
				c.mockBackendConfig,
				c.mockGatewayJobsDB,
				c.mockRouterJobsDB,
				c.mockBatchRouterJobsDB,
				c.mockReadProcErrorsDB,
				c.mockWriteProcErrorsDB,
				nil,
				nil,
				c.MockReportingI,
				transientsource.NewEmptyService(),
				fileuploader.NewDefaultProvider(),
				c.MockRsourcesService,
				transformerFeaturesService.NewNoOpService(),
				destinationdebugger.NewNoOpService(),
				transformationdebugger.NewNoOpService(),
				[]enricher.PipelineEnricher{},
				trackedusers.NewNoopDataCollector(),
			)
			Expect(err).To(BeNil())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())

			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(
				gomock.Any(),
				jobsdb.GetQueryParams{
					CustomValFilters: gatewayCustomVal,
					JobsLimit:        processor.config.maxEventsToProcess.Load(),
					EventsLimit:      processor.config.maxEventsToProcess.Load(),
					PayloadSizeLimit: processor.payloadLimit.Load(),
				}).Return(jobsdb.JobsResult{Jobs: emptyJobsList}, nil).Times(1)

			didWork := processor.handlePendingGatewayJobs("")
			Expect(didWork).To(Equal(false))
		})

		It("should process unprocessed jobs to destination without user transformation", func() {
			messages := map[string]mockEventData{
				// this message should be delivered only to destination A
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
					params:                    map[string]string{"source_id": "enabled-source-no-ut"},
				},
				// this message should not be delivered to destination A
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-a-definition-display-name": false},
					params:                    map[string]string{"source_id": "enabled-source-no-ut"},
				},
				// this message should be delivered to all destinations
				"message-3": {
					id:                 "3",
					jobid:              2010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": true},
					params:             map[string]string{"source_id": "enabled-source-no-ut", "source_job_run_id": "job_run_id_1", "source_task_run_id": "task_run_id_1"},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     2010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{},
					params:                    map[string]string{"source_id": "enabled-source-no-ut", "source_job_run_id": "job_run_id_1", "source_task_run_id": "task_run_id_1"},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              2010,
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": false},
					params:             map[string]string{"source_id": "enabled-source-no-ut", "source_job_run_id": "job_run_id_1", "source_task_run_id": "task_run_id_1"},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						}, createMessagePayloadWithoutSources,
					),
					EventCount:    2,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledNoUT),
				},
				{
					UUID:          uuid.New(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:          uuid.New(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2002-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-3"],
							messages["message-4"],
							messages["message-5"],
						},
						createMessagePayloadWithoutSources,
					),
					EventCount: 3,
					Parameters: createBatchParametersWithSources(SourceIDEnabledNoUT),
				},
			}
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(
				gomock.Any(),
				jobsdb.GetQueryParams{
					CustomValFilters: gatewayCustomVal,
					JobsLimit:        processor.config.maxEventsToProcess.Load(),
					EventsLimit:      processor.config.maxEventsToProcess.Load(),
					PayloadSizeLimit: processor.payloadLimit.Load(),
				}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)

			transformExpectations := map[string]transformExpectation{
				DestinationIDEnabledA: {
					events:                    3,
					messageIds:                "message-1,message-3,message-4",
					receiveMetadata:           true,
					destinationDefinitionName: "enabled-destination-a-definition-name",
				},
			}

			// We expect one transform call to destination A, after callUnprocessed.
			mockTransformer.EXPECT().Transform(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).Times(1).After(callUnprocessed).
				DoAndReturn(assertDestinationTransform(
					messages,
					SourceIDEnabledNoUT,
					DestinationIDEnabledA,
					transformExpectations[DestinationIDEnabledA],
				))

			assertStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(string(job.EventPayload)).To(Equal(fmt.Sprintf(`{"int-value":%d,"string-value":%q}`, i, destination)))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				require.JSONEq(GinkgoT(), fmt.Sprintf(`{
					"source_id":"source-from-transformer",
					"source_name": "%s",
					"destination_id":"destination-from-transformer",
					"received_at":"",
					"transform_at":"processor",
					"message_id":"",
					"gateway_job_id":0,
					"source_task_run_id":"",
					"source_job_id":"",
					"source_job_run_id":"",
					"event_name":"",
					"event_type":"",
					"source_definition_id":"",
					"destination_definition_id":"",
					"source_category":"",
					"record_id":null,
					"workspaceId":"",
					"traceparent":""
				}`, sourceIDToName[SourceIDEnabledNoUT]), string(job.Parameters))
			}
			// One Store call is expected for all events
			c.mockRouterJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			callStoreRouter := c.mockRouterJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-a")
					}
				})

			c.MockRsourcesService.EXPECT().
				IncrementStats(
					gomock.Any(),
					gomock.Any(),
					"job_run_id_1",
					rsources.JobTargetKey{
						TaskRunID: "task_run_id_1",
						SourceID:  "enabled-source-no-ut",
					},
					rsources.Stats{In: 2, Failed: 2},
				).Times(1).Return(nil)

			c.MockRsourcesService.EXPECT().
				IncrementStats(
					gomock.Any(),
					gomock.Any(),
					"job_run_id_1",
					rsources.JobTargetKey{
						TaskRunID: "task_run_id_1",
						SourceID:  "enabled-source-no-ut",
					},
					rsources.Stats{Out: 1},
				).Times(1).Return(nil)

			c.mockArchivalDB.EXPECT().
				WithStoreSafeTx(
					gomock.Any(),
					gomock.Any(),
				).Times(1).
				Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
					_ = f(jobsdb.EmptyStoreSafeTx())
				}).Return(nil)
			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
				})

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callStoreRouter).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					// jobs should be sorted by jobid, so order of statuses is different from order of jobs
					for i := range unprocessedJobsList {
						assertJobStatus(unprocessedJobsList[i], statuses[i], jobsdb.Succeeded.State)
					}
				})

			processorSetupAndAssertJobHandling(processor, c)
		})

		It("should process unprocessed jobs to destination with only user transformation", func() {
			messages := map[string]mockEventData{
				// this message should only be delivered to destination B
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-b-definition-display-name": true},
				},
				// this message should not be delivered to destination B
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-b-definition-display-name": false},
				},
				// this message should be delivered to all destinations
				"message-3": {
					id:                 "3",
					jobid:              2010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": true},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     2010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              2010,
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": false},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledOnlyUT,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						},
						createMessagePayloadWithoutSources,
					),
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledOnlyUT),
				},
				{
					UUID:         uuid.New(),
					JobID:        2002,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:    gatewayCustomVal[0],
					EventPayload: nil,
					EventCount:   1,
					Parameters:   createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:         uuid.New(),
					JobID:        2003,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					CustomVal:    gatewayCustomVal[0],
					EventPayload: nil,
					EventCount:   1,
					Parameters:   createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledOnlyUT,
						"2002-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-3"],
							messages["message-4"],
							messages["message-5"],
						},
						createMessagePayloadWithoutSources,
					),
					EventCount: 1,
					Parameters: createBatchParameters(SourceIDEnabledOnlyUT),
				},
			}

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParams{
				CustomValFilters: gatewayCustomVal,
				JobsLimit:        processor.config.maxEventsToProcess.Load(),
				EventsLimit:      processor.config.maxEventsToProcess.Load(),
				PayloadSizeLimit: processor.payloadLimit.Load(),
			}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)

			transformExpectations := map[string]transformExpectation{
				DestinationIDEnabledB: {
					events:                    3,
					messageIds:                "message-1,message-3,message-4",
					destinationDefinitionName: "minio",
				},
			}

			// We expect one call to user transform for destination B
			callUserTransform := mockTransformer.EXPECT().UserTransform(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).After(callUnprocessed).
				DoAndReturn(func(ctx context.Context, clientEvents []transformer.TransformerEvent, batchSize int) transformer.Response {
					defer GinkgoRecover()

					outputEvents := make([]transformer.TransformerResponse, 0)

					for _, event := range clientEvents {
						event.Message["user-transform"] = "value"
						outputEvents = append(outputEvents, transformer.TransformerResponse{
							Output: event.Message,
						})
					}

					return transformer.Response{
						Events: outputEvents,
					}
				})

			// We expect one transform call to destination B, after user transform for destination B.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				After(callUserTransform).DoAndReturn(assertDestinationTransform(messages, SourceIDEnabledOnlyUT, DestinationIDEnabledB, transformExpectations[DestinationIDEnabledB]))

			assertStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				// Expect(job.CustomVal).To(Equal("destination-definition-name-a"))
				Expect(string(job.EventPayload)).To(Equal(fmt.Sprintf(`{"int-value":%d,"string-value":%q}`, i, destination)))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				require.JSONEq(GinkgoT(), fmt.Sprintf(`{
					"source_id": "source-from-transformer",
					"source_name": "%s",
					"destination_id": "destination-from-transformer",
					"received_at": "",
					"transform_at": "processor",
					"message_id": "",
					"gateway_job_id": 0,
					"source_task_run_id": "",
					"source_job_id": "",
					"source_job_run_id": "",
					"event_name": "",
					"event_type": "",
					"source_definition_id": "",
					"destination_definition_id": "",
					"source_category": "",
					"record_id": null,
					"workspaceId": "",
					"traceparent": ""
				}`, sourceIDToName[SourceIDEnabledOnlyUT]), string(job.Parameters))
			}

			c.mockBatchRouterJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			callStoreBatchRouter := c.mockBatchRouterJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-b")
					}
				})

			c.mockArchivalDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).AnyTimes().Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				AnyTimes()
			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callStoreBatchRouter).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					for i := range unprocessedJobsList {
						assertJobStatus(unprocessedJobsList[i], statuses[i], jobsdb.Succeeded.State)
					}
				})

			processorSetupAndAssertJobHandling(processor, c)
		})

		It("should process unprocessed jobs to destination without user transformation with enabled Dedup", func() {
			messages := map[string]mockEventData{
				// this message should be delivered only to destination A
				"message-some-id-1": {
					id:                        "some-id",
					jobid:                     2010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-03-02T01:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-c-definition-display-name": true},
				},
				// this message should not be delivered to destination A
				"message-some-id-2": {
					id:                        "some-id",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-c-definition-display-name": true},
				},
				"message-some-id-3": {
					id:                        "some-id",
					jobid:                     3010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-03-02T01:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-c-definition-display-name": true},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabled,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-some-id-2"],
							messages["message-some-id-1"],
						},
						createMessagePayloadWithSameMessageId,
					),
					EventCount:    2,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabled,
						"2002-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-some-id-3"],
						},
						createMessagePayloadWithSameMessageId,
					),
					EventCount: 1,
					Parameters: createBatchParameters(SourceIDEnabled),
				},
			}

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), gomock.Any()).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)
			c.MockDedup.EXPECT().GetBatch(gomock.Any()).Return(map[dedupTypes.KeyValue]bool{
				{Key: "message-some-id", JobID: 1010, WorkspaceID: ""}: true,
				{Key: "message-some-id", JobID: 2010, WorkspaceID: ""}: true,
			}, nil).After(callUnprocessed).AnyTimes()
			c.MockDedup.EXPECT().Commit(gomock.Any()).Times(1)

			// We expect one transform call to destination A, after callUnprocessed.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any()).Times(0).After(callUnprocessed)
			// One Store call is expected for all events
			c.mockRouterJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil).Times(1)
			callStoreRouter := c.mockRouterJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Len(3)).Times(1)

			c.mockArchivalDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).AnyTimes().Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				AnyTimes()

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callStoreRouter)
			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			Setup(processor, c, true, false)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())

			processor.dedup = c.MockDedup
			handlePendingGatewayJobs(processor)
		})

		It("should process unprocessed jobs to destination without user transformation and parallelScan", func() {
			messages := map[string]mockEventData{
				// this message should be delivered only to destination A
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
					params:                    map[string]string{"source_id": "enabled-source-no-ut"},
				},
				// this message should not be delivered to destination A
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-a-definition-display-name": false},
					params:                    map[string]string{"source_id": "enabled-source-no-ut"},
				},
				// this message should be delivered to all destinations
				"message-3": {
					id:                 "3",
					jobid:              2010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": true},
					params:             map[string]string{"source_id": "enabled-source-no-ut", "source_job_run_id": "job_run_id_1", "source_task_run_id": "task_run_id_1"},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     2010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{},
					params:                    map[string]string{"source_id": "enabled-source-no-ut", "source_job_run_id": "job_run_id_1", "source_task_run_id": "task_run_id_1"},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              2010,
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": false},
					params:             map[string]string{"source_id": "enabled-source-no-ut", "source_job_run_id": "job_run_id_1", "source_task_run_id": "task_run_id_1"},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						}, createMessagePayloadWithoutSources,
					),
					EventCount:    2,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledNoUT),
				},
				{
					UUID:          uuid.New(),
					JobID:         2002,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:          uuid.New(),
					JobID:         2003,
					CreatedAt:     time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledNoUT,
						"2002-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-3"],
							messages["message-4"],
							messages["message-5"],
						},
						createMessagePayloadWithoutSources,
					),
					EventCount: 3,
					Parameters: createBatchParametersWithSources(SourceIDEnabledNoUT),
				},
			}
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))
			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(
				gomock.Any(),
				jobsdb.GetQueryParams{
					CustomValFilters: gatewayCustomVal,
					JobsLimit:        processor.config.maxEventsToProcess.Load(),
					EventsLimit:      processor.config.maxEventsToProcess.Load(),
					PayloadSizeLimit: processor.payloadLimit.Load(),
				}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)

			transformExpectations := map[string]transformExpectation{
				DestinationIDEnabledA: {
					events:                    3,
					messageIds:                "message-1,message-3,message-4",
					receiveMetadata:           true,
					destinationDefinitionName: "enabled-destination-a-definition-name",
				},
			}

			// We expect one transform call to destination A, after callUnprocessed.
			mockTransformer.EXPECT().Transform(
				gomock.Any(),
				gomock.Any(),
				gomock.Any(),
			).Times(1).After(callUnprocessed).
				DoAndReturn(assertDestinationTransform(
					messages,
					SourceIDEnabledNoUT,
					DestinationIDEnabledA,
					transformExpectations[DestinationIDEnabledA],
				))

			assertStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(string(job.EventPayload)).To(Equal(fmt.Sprintf(`{"int-value":%d,"string-value":%q}`, i, destination)))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				require.JSONEq(GinkgoT(), fmt.Sprintf(`{
					"source_id":"source-from-transformer",
					"source_name": "%s",
					"destination_id":"destination-from-transformer",
					"received_at":"",
					"transform_at":"processor",
					"message_id":"",
					"gateway_job_id":0,
					"source_task_run_id":"",
					"source_job_id":"",
					"source_job_run_id":"",
					"event_name":"",
					"event_type":"",
					"source_definition_id":"",
					"destination_definition_id":"",
					"source_category":"",
					"record_id":null,
					"workspaceId":"",
					"traceparent":""
				}`, sourceIDToName[SourceIDEnabledNoUT]), string(job.Parameters))
			}
			// One Store call is expected for all events
			c.mockRouterJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			callStoreRouter := c.mockRouterJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-a")
					}
				})

			c.MockRsourcesService.EXPECT().
				IncrementStats(
					gomock.Any(),
					gomock.Any(),
					"job_run_id_1",
					rsources.JobTargetKey{
						TaskRunID: "task_run_id_1",
						SourceID:  "enabled-source-no-ut",
					},
					rsources.Stats{In: 2, Failed: 2},
				).Times(1).Return(nil)

			c.MockRsourcesService.EXPECT().
				IncrementStats(
					gomock.Any(),
					gomock.Any(),
					"job_run_id_1",
					rsources.JobTargetKey{
						TaskRunID: "task_run_id_1",
						SourceID:  "enabled-source-no-ut",
					},
					rsources.Stats{Out: 1},
				).Times(1).Return(nil)

			c.mockArchivalDB.EXPECT().
				WithStoreSafeTx(
					gomock.Any(),
					gomock.Any(),
				).Times(1).
				Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
					_ = f(jobsdb.EmptyStoreSafeTx())
				}).Return(nil)
			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
				})

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callStoreRouter).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					// jobs should be sorted by jobid, so order of statuses is different from order of jobs
					for i := range unprocessedJobsList {
						assertJobStatus(unprocessedJobsList[i], statuses[i], jobsdb.Succeeded.State)
					}
				})

			processorSetupAndAssertJobHandling(processor, c)
		})

		It("should process unprocessed jobs to destination with only user transformation and parallelScan", func() {
			messages := map[string]mockEventData{
				// this message should only be delivered to destination B
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-b-definition-display-name": true},
				},
				// this message should not be delivered to destination B
				"message-2": {
					id:                        "2",
					jobid:                     1010,
					originalTimestamp:         "2000-02-02T01:23:45",
					expectedOriginalTimestamp: "2000-02-02T01:23:45.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": true, "enabled-destination-b-definition-display-name": false},
				},
				// this message should be delivered to all destinations
				"message-3": {
					id:                 "3",
					jobid:              2010,
					originalTimestamp:  "malformed timestamp",
					sentAt:             "2000-03-02T01:23:15",
					expectedSentAt:     "2000-03-02T01:23:15.000Z",
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": true},
				},
				// this message should be delivered to all destinations (default All value)
				"message-4": {
					id:                        "4",
					jobid:                     2010,
					originalTimestamp:         "2000-04-02T02:23:15.000Z", // missing sentAt
					expectedOriginalTimestamp: "2000-04-02T02:23:15.000Z",
					expectedReceivedAt:        "2002-01-02T02:23:45.000Z",
					integrations:              map[string]bool{},
				},
				// this message should not be delivered to any destination
				"message-5": {
					id:                 "5",
					jobid:              2010,
					expectedReceivedAt: "2002-01-02T02:23:45.000Z",
					integrations:       map[string]bool{"All": false},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1002,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 27, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  nil,
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledOnlyUT,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						},
						createMessagePayloadWithoutSources,
					),
					EventCount:    1,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledOnlyUT),
				},
				{
					UUID:         uuid.New(),
					JobID:        2002,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 27, 0o0, 0o0, time.UTC),
					CustomVal:    gatewayCustomVal[0],
					EventPayload: nil,
					EventCount:   1,
					Parameters:   createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:         uuid.New(),
					JobID:        2003,
					CreatedAt:    time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					ExpireAt:     time.Date(2020, 0o4, 28, 13, 28, 0o0, 0o0, time.UTC),
					CustomVal:    gatewayCustomVal[0],
					EventPayload: nil,
					EventCount:   1,
					Parameters:   createBatchParameters(SourceIDEnabled),
				},
				{
					UUID:      uuid.New(),
					JobID:     2010,
					CreatedAt: time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 13, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabledOnlyUT,
						"2002-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-3"],
							messages["message-4"],
							messages["message-5"],
						},
						createMessagePayloadWithoutSources,
					),
					EventCount: 1,
					Parameters: createBatchParameters(SourceIDEnabledOnlyUT),
				},
			}

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			callUnprocessed := c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParams{
				CustomValFilters: gatewayCustomVal,
				JobsLimit:        processor.config.maxEventsToProcess.Load(),
				EventsLimit:      processor.config.maxEventsToProcess.Load(),
				PayloadSizeLimit: processor.payloadLimit.Load(),
			}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)

			transformExpectations := map[string]transformExpectation{
				DestinationIDEnabledB: {
					events:                    3,
					messageIds:                "message-1,message-3,message-4",
					destinationDefinitionName: "minio",
				},
			}

			// We expect one call to user transform for destination B
			callUserTransform := mockTransformer.EXPECT().UserTransform(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).After(callUnprocessed).
				DoAndReturn(func(ctx context.Context, clientEvents []transformer.TransformerEvent, batchSize int) transformer.Response {
					defer GinkgoRecover()

					outputEvents := make([]transformer.TransformerResponse, 0)

					for _, event := range clientEvents {
						event.Message["user-transform"] = "value"
						outputEvents = append(outputEvents, transformer.TransformerResponse{
							Output: event.Message,
						})
					}

					return transformer.Response{
						Events: outputEvents,
					}
				})

			// We expect one transform call to destination B, after user transform for destination B.
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				After(callUserTransform).DoAndReturn(assertDestinationTransform(messages, SourceIDEnabledOnlyUT, DestinationIDEnabledB, transformExpectations[DestinationIDEnabledB]))

			assertStoreJob := func(job *jobsdb.JobT, i int, destination string) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				// Expect(job.CustomVal).To(Equal("destination-definition-name-a"))
				Expect(string(job.EventPayload)).To(Equal(fmt.Sprintf(`{"int-value":%d,"string-value":%q}`, i, destination)))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))
				require.JSONEq(GinkgoT(), fmt.Sprintf(`{
					"source_id": "source-from-transformer",
					"source_name": "%s",
					"destination_id": "destination-from-transformer",
					"received_at": "",
					"transform_at": "processor",
					"message_id": "",
					"gateway_job_id": 0,
					"source_task_run_id": "",
					"source_job_id": "",
					"source_job_run_id": "",
					"event_name": "",
					"event_type": "",
					"source_definition_id": "",
					"destination_definition_id": "",
					"source_category": "",
					"record_id": null,
					"workspaceId": "",
					"traceparent": ""
				}`, sourceIDToName[SourceIDEnabledOnlyUT]), string(job.Parameters))
			}

			c.mockBatchRouterJobsDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).Times(1).Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)
			callStoreBatchRouter := c.mockBatchRouterJobsDB.EXPECT().StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, tx jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertStoreJob(job, i, "value-enabled-destination-b")
					}
				})

			c.mockArchivalDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).AnyTimes().Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				AnyTimes()
			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).After(callStoreBatchRouter).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					for i := range unprocessedJobsList {
						assertJobStatus(unprocessedJobsList[i], statuses[i], jobsdb.Succeeded.State)
					}
				})

			processorSetupAndAssertJobHandling(processor, c)
		})
	})

	Context("transformations", func() {
		It("messages should be skipped on destination transform failures, without failing the job", func() {
			messages := map[string]mockEventData{
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
				},
				"message-2": {
					id:                        "2",
					jobid:                     1011,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabled, "2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						},
						createMessagePayload,
					),
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
			}

			transformerResponses := []transformer.TransformerResponse{
				{
					Metadata: transformer.Metadata{
						MessageID: "message-1",
					},
					StatusCode: 400,
					Error:      "error-1",
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					StatusCode: 400,
					Error:      "error-2",
				},
			}

			assertErrStoreJob := func(job *jobsdb.JobT, i int) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.CustomVal).To(Equal("enabled-destination-a-definition-name"))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))

				var paramsMap, expectedParamsMap map[string]interface{}
				err := json.Unmarshal(job.Parameters, &paramsMap)
				Expect(err).To(BeNil())
				expectedStr := []byte(fmt.Sprintf(`{"source_id": "%v", "destination_id": "enabled-destination-a", "source_job_run_id": "", "error": "error-%v", "status_code": 400, "stage": "dest_transformer", "source_task_run_id": "", "record_id": null}`, SourceIDEnabled, i+1))
				err = json.Unmarshal(expectedStr, &expectedParamsMap)
				Expect(err).To(BeNil())
				equals := reflect.DeepEqual(paramsMap, expectedParamsMap)
				Expect(equals).To(Equal(true))

				// compare payloads
				var payload []map[string]interface{}
				err = json.Unmarshal(job.EventPayload, &payload)
				Expect(err).To(BeNil())
				Expect(len(payload)).To(Equal(1))
				message := messages[fmt.Sprintf(`message-%v`, i+1)]
				Expect(fmt.Sprintf(`message-%s`, message.id)).To(Equal(payload[0]["messageId"]))
				Expect(payload[0]["some-property"]).To(Equal(fmt.Sprintf(`property-%s`, message.id)))
				Expect(message.expectedOriginalTimestamp).To(Equal(payload[0]["originalTimestamp"]))
			}

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParams{
				CustomValFilters: gatewayCustomVal,
				JobsLimit:        processor.config.maxEventsToProcess.Load(),
				EventsLimit:      processor.config.maxEventsToProcess.Load(),
				PayloadSizeLimit: processor.payloadLimit.Load(),
			}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)
			// Test transformer failure
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Return(transformer.Response{
					Events:       []transformer.TransformerResponse{},
					FailedEvents: transformerResponses,
				})

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					// job should be marked as successful regardless of transformer response
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Succeeded.State)
				})

			c.mockArchivalDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).AnyTimes().Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				AnyTimes()

			// One Store call is expected for all events
			c.mockWriteProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(2))
					for i, job := range jobs {
						assertErrStoreJob(job, i)
					}
				})

			processorSetupAndAssertJobHandling(processor, c)
		})

		It("messages should be skipped on user transform failures, without failing the job", func() {
			messages := map[string]mockEventData{
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-b-definition-display-name": true},
				},
				"message-2": {
					id:                        "2",
					jobid:                     1011,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-b-definition-display-name": true},
				},
			}

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:      uuid.New(),
					JobID:     1010,
					CreatedAt: time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:  time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal: gatewayCustomVal[0],
					EventPayload: createBatchPayload(
						WriteKeyEnabled,
						"2001-01-02T02:23:45.000Z",
						[]mockEventData{
							messages["message-1"],
							messages["message-2"],
						},
						createMessagePayload,
					),
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabled),
				},
			}

			transformerResponses := []transformer.TransformerResponse{
				{
					Metadata: transformer.Metadata{
						MessageIDs: []string{"message-1", "message-2"},
					},
					StatusCode: 400,
					Error:      "error-combined",
				},
			}

			assertErrStoreJob := func(job *jobsdb.JobT) {
				Expect(job.UUID.String()).To(testutils.BeValidUUID())
				Expect(job.JobID).To(Equal(int64(0)))
				Expect(job.CreatedAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.ExpireAt).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
				Expect(job.CustomVal).To(Equal("MINIO"))
				Expect(len(job.LastJobStatus.JobState)).To(Equal(0))

				var paramsMap, expectedParamsMap map[string]interface{}
				err := json.Unmarshal(job.Parameters, &paramsMap)
				Expect(err).To(BeNil())
				expectedStr := []byte(fmt.Sprintf(`{"source_id": "%v", "destination_id": "enabled-destination-b", "source_job_run_id": "", "error": "error-combined", "status_code": 400, "stage": "user_transformer", "source_task_run_id":"", "record_id": null}`, SourceIDEnabled))
				err = json.Unmarshal(expectedStr, &expectedParamsMap)
				Expect(err).To(BeNil())
				equals := reflect.DeepEqual(paramsMap, expectedParamsMap)
				Expect(equals).To(Equal(true))

				// compare payloads
				var payload []map[string]interface{}
				err = json.Unmarshal(job.EventPayload, &payload)
				Expect(err).To(BeNil())
				Expect(len(payload)).To(Equal(2))
				message1 := messages[fmt.Sprintf(`message-%v`, 1)]
				Expect(fmt.Sprintf(`message-%s`, message1.id)).To(Equal(payload[0]["messageId"]))
				Expect(payload[0]["some-property"]).To(Equal(fmt.Sprintf(`property-%s`, message1.id)))
				Expect(message1.expectedOriginalTimestamp).To(Equal(payload[0]["originalTimestamp"]))
				message2 := messages[fmt.Sprintf(`message-%v`, 2)]
				Expect(fmt.Sprintf(`message-%s`, message2.id)).To(Equal(payload[1]["messageId"]))
				Expect(payload[1]["some-property"]).To(Equal(fmt.Sprintf(`property-%s`, message2.id)))
				Expect(message2.expectedOriginalTimestamp).To(Equal(payload[1]["originalTimestamp"]))
			}

			var toRetryJobsList []*jobsdb.JobT

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParams{
				CustomValFilters: gatewayCustomVal,
				JobsLimit:        processor.config.maxEventsToProcess.Load(),
				EventsLimit:      processor.config.maxEventsToProcess.Load(),
				PayloadSizeLimit: processor.payloadLimit.Load(),
			}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)

			// Test transformer failure
			mockTransformer.EXPECT().UserTransform(gomock.Any(), gomock.Any(), gomock.Any()).Times(1).
				Return(transformer.Response{
					Events:       []transformer.TransformerResponse{},
					FailedEvents: transformerResponses,
				})

			c.mockArchivalDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).AnyTimes().Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				AnyTimes()

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					// job should be marked as successful regardless of transformer response
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Succeeded.State)
				})

			// One Store call is expected for all events
			c.mockWriteProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(1))
					for _, job := range jobs {
						assertErrStoreJob(job)
					}
				})

			processorSetupAndAssertJobHandling(processor, c)
		})

		It("should drop messages that the destination doesn't support", func() {
			messages := map[string]mockEventData{
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
				},
			}
			payload := createBatchPayload(
				WriteKeyEnabledNoUT2,
				"2001-01-02T02:23:45.000Z",
				[]mockEventData{
					messages["message-1"],
				},
				createMessagePayload,
			)
			payload, _ = sjson.SetBytes(payload, "batch.0.type", "identify")

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1010,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  payload,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParameters(SourceIDEnabledNoUT2),
				},
			}

			var toRetryJobsList []*jobsdb.JobT

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParams{
				CustomValFilters: gatewayCustomVal,
				JobsLimit:        processor.config.maxEventsToProcess.Load(),
				EventsLimit:      processor.config.maxEventsToProcess.Load(),
				PayloadSizeLimit: processor.payloadLimit.Load(),
			}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)

			// Test transformer failure
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Len(0), gomock.Any()).Times(0)

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					// job should be marked as successful regardless of transformer response
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Succeeded.State)
				})

			c.mockArchivalDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).AnyTimes().Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				AnyTimes()

			c.mockWriteProcErrorsDB.EXPECT().WithTx(gomock.Any()).Do(func(f func(tx *Tx) error) {
				_ = f(&Tx{})
			}).Return(nil).Times(0)

			// One Store call is expected for all events
			c.mockWriteProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(0).
				Do(func(ctx context.Context, jobs []*jobsdb.JobT) {})

			processorSetupAndAssertJobHandling(processor, c)
		})

		It("should drop messages where jobRunID is cancelled", func() {
			messages := map[string]mockEventData{
				"message-1": {
					id:                        "1",
					jobid:                     1010,
					originalTimestamp:         "2000-01-02T01:23:45",
					expectedOriginalTimestamp: "2000-01-02T01:23:45.000Z",
					sentAt:                    "2000-01-02 01:23",
					expectedSentAt:            "2000-01-02T01:23:00.000Z",
					expectedReceivedAt:        "2001-01-02T02:23:45.000Z",
					integrations:              map[string]bool{"All": false, "enabled-destination-a-definition-display-name": true},
				},
			}
			payload := createBatchPayload(
				WriteKeyEnabledNoUT2,
				"2001-01-02T02:23:45.000Z",
				[]mockEventData{
					messages["message-1"],
				},
				createMessagePayload,
			)
			payload, _ = sjson.SetBytes(payload, "batch.0.type", "identify")

			unprocessedJobsList := []*jobsdb.JobT{
				{
					UUID:          uuid.New(),
					JobID:         1010,
					CreatedAt:     time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					ExpireAt:      time.Date(2020, 0o4, 28, 23, 26, 0o0, 0o0, time.UTC),
					CustomVal:     gatewayCustomVal[0],
					EventPayload:  payload,
					LastJobStatus: jobsdb.JobStatusT{},
					Parameters:    createBatchParametersWithSources(SourceIDEnabledNoUT2),
				},
			}

			var toRetryJobsList []*jobsdb.JobT

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), jobsdb.GetQueryParams{
				CustomValFilters: gatewayCustomVal,
				JobsLimit:        processor.config.maxEventsToProcess.Load(),
				EventsLimit:      processor.config.maxEventsToProcess.Load(),
				PayloadSizeLimit: processor.payloadLimit.Load(),
			}).Return(jobsdb.JobsResult{Jobs: unprocessedJobsList}, nil).Times(1)

			// Test transformer failure
			mockTransformer.EXPECT().Transform(gomock.Any(), gomock.Len(0), gomock.Any()).Times(0)

			c.mockGatewayJobsDB.EXPECT().WithUpdateSafeTx(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, f func(tx jobsdb.UpdateSafeTx) error) {
				_ = f(jobsdb.EmptyUpdateSafeTx())
			}).Return(nil).Times(1)
			c.mockGatewayJobsDB.EXPECT().UpdateJobStatusInTx(gomock.Any(), gomock.Any(), gomock.Len(len(toRetryJobsList)+len(unprocessedJobsList)), gatewayCustomVal, nil).Times(1).
				Do(func(ctx context.Context, txn jobsdb.UpdateSafeTx, statuses []*jobsdb.JobStatusT, _, _ interface{}) {
					// job should be marked as successful regardless of transformer response
					assertJobStatus(unprocessedJobsList[0], statuses[0], jobsdb.Succeeded.State)
				})

			c.MockRsourcesService.EXPECT().
				IncrementStats(
					gomock.Any(),
					gomock.Any(),
					"job_run_id_1",
					rsources.JobTargetKey{
						TaskRunID: "task_run_id_1",
						SourceID:  "enabled-source-no-ut2",
					},
					rsources.Stats{Out: 1},
				).Times(1)
			c.MockRsourcesService.EXPECT().
				IncrementStats(
					gomock.Any(),
					gomock.Any(),
					"job_run_id_1",
					rsources.JobTargetKey{
						TaskRunID: "task_run_id_1",
						SourceID:  "enabled-source-no-ut2",
					},
					rsources.Stats{In: 1, Failed: 1},
				).Times(1)

			c.mockArchivalDB.EXPECT().WithStoreSafeTx(gomock.Any(), gomock.Any()).AnyTimes().Do(func(ctx context.Context, f func(tx jobsdb.StoreSafeTx) error) {
				_ = f(jobsdb.EmptyStoreSafeTx())
			}).Return(nil)

			c.mockArchivalDB.EXPECT().
				StoreInTx(gomock.Any(), gomock.Any(), gomock.Any()).
				Times(0)

			c.mockWriteProcErrorsDB.EXPECT().WithTx(gomock.Any()).Return(nil).Times(0)

			// One Store call is expected for all events
			c.mockWriteProcErrorsDB.EXPECT().Store(gomock.Any(), gomock.Any()).Times(1).
				Do(func(ctx context.Context, jobs []*jobsdb.JobT) {
					Expect(jobs).To(HaveLen(1))
				})

			config.Set("drain.jobRunIDs", "job_run_id_1")
			defer config.Reset()
			processorSetupAndAssertJobHandling(processor, c)
		})
	})

	Context("MainLoop Tests", func() {
		It("Should not handle jobs when transformer features are not set", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)
			mockTransformerFeaturesService := mock_features.NewMockFeaturesService(c.mockCtrl)
			mockTransformerFeaturesService.EXPECT().Wait().Return(make(chan struct{})).AnyTimes()
			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)
			err := processor.Setup(
				c.mockBackendConfig,
				c.mockGatewayJobsDB,
				c.mockRouterJobsDB,
				c.mockBatchRouterJobsDB,
				c.mockReadProcErrorsDB,
				c.mockWriteProcErrorsDB,
				nil,
				nil,
				nil,
				transientsource.NewEmptyService(),
				fileuploader.NewDefaultProvider(),
				c.MockRsourcesService,
				mockTransformerFeaturesService,
				destinationdebugger.NewNoOpService(),
				transformationdebugger.NewNoOpService(),
				[]enricher.PipelineEnricher{},
				trackedusers.NewNoopDataCollector(),
			)
			Expect(err).To(BeNil())
			setMainLoopTimeout(processor, 1*time.Second)

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			c.mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).Times(1)
			c.mockReadProcErrorsDB.EXPECT().FailExecuting().Times(1)
			c.mockReadProcErrorsDB.EXPECT().GetJobs(gomock.Any(), []string{jobsdb.Failed.State, jobsdb.Unprocessed.State}, gomock.Any()).AnyTimes()
			c.mockRouterJobsDB.EXPECT().GetPileUpCounts(gomock.Any()).AnyTimes()
			c.mockBatchRouterJobsDB.EXPECT().GetPileUpCounts(gomock.Any()).AnyTimes()

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				err := processor.Start(ctx)
				Expect(err).To(BeNil())
				wg.Done()
			}()

			Consistently(func() bool {
				select {
				case <-processor.transformerFeaturesService.Wait():
					return true
				default:
					return false
				}
			}, 2*time.Second, 10*time.Millisecond).Should(BeFalse())
			wg.Wait()
		})
	})

	Context("ProcessorLoop Tests", func() {
		It("Should not handle jobs when transformer features are not set", func() {
			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			// crash recover returns empty list
			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)
			err := processor.Setup(
				c.mockBackendConfig,
				c.mockGatewayJobsDB,
				c.mockRouterJobsDB,
				c.mockBatchRouterJobsDB,
				c.mockReadProcErrorsDB,
				c.mockWriteProcErrorsDB,
				nil,
				nil,
				c.MockReportingI,
				transientsource.NewEmptyService(),
				fileuploader.NewDefaultProvider(),
				c.MockRsourcesService,
				transformerFeaturesService.NewNoOpService(),
				destinationdebugger.NewNoOpService(),
				transformationdebugger.NewNoOpService(),
				[]enricher.PipelineEnricher{},
				trackedusers.NewNoopDataCollector(),
			)
			Expect(err).To(BeNil())
			defer processor.Shutdown()

			processor.config.readLoopSleep = config.SingleValueLoader(time.Millisecond)

			c.mockReadProcErrorsDB.EXPECT().FailExecuting()
			c.mockReadProcErrorsDB.EXPECT().GetJobs(gomock.Any(), []string{jobsdb.Failed.State, jobsdb.Unprocessed.State}, gomock.Any()).AnyTimes()
			c.mockBackendConfig.EXPECT().WaitForConfig(gomock.Any()).Times(1)
			c.mockRouterJobsDB.EXPECT().GetPileUpCounts(gomock.Any()).AnyTimes()
			c.mockBatchRouterJobsDB.EXPECT().GetPileUpCounts(gomock.Any()).AnyTimes()

			c.mockGatewayJobsDB.EXPECT().GetUnprocessed(gomock.Any(), gomock.Any()).Times(0)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			Expect(processor.Start(ctx)).To(BeNil())
		})
	})

	Context("getConsentFilteredDestinations", func() {
		It("should filter based on oneTrust consent management preferences", func() {
			eventWithDeniedConsents := types.SingularEventT{
				"originalTimestamp": "2019-03-10T10:10:10.10Z",
				"event":             "Demo Track",
				"sentAt":            "2019-03-10T10:10:10.10Z",
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"deniedConsentIds": []interface{}{"category1", "someOtherCategory"},
					},
				},
				"type":      "track",
				"channel":   "mobile",
				"rudderId":  "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
				"messageId": "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
				"properties": map[string]interface{}{
					"label":    "",
					"value":    float64(1),
					"testMap":  nil,
					"category": "",
					"floatVal": 4.51,
				},
				"integrations": map[string]interface{}{
					"All": true,
				},
			}
			_, err1 := json.Marshal(eventWithDeniedConsents)
			Expect(err1).To(BeNil())

			eventWithoutDeniedConsents := types.SingularEventT{
				"originalTimestamp": "2019-03-10T10:10:10.10Z",
				"event":             "Demo Track",
				"sentAt":            "2019-03-10T10:10:10.10Z",
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"deniedConsentIds": []interface{}{},
					},
				},
				"type":      "track",
				"channel":   "mobile",
				"rudderId":  "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
				"messageId": "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
				"properties": map[string]interface{}{
					"label":    "",
					"value":    float64(1),
					"testMap":  nil,
					"category": "",
					"floatVal": 4.51,
				},
				"integrations": map[string]interface{}{
					"All": true,
				},
			}
			_, err2 := json.Marshal(eventWithoutDeniedConsents)
			Expect(err2).To(BeNil())

			eventWithoutConsentManagementData := types.SingularEventT{
				"originalTimestamp": "2019-03-10T10:10:10.10Z",
				"event":             "Demo Track",
				"sentAt":            "2019-03-10T10:10:10.10Z",
				"context":           map[string]interface{}{},
				"type":              "track",
				"channel":           "mobile",
				"rudderId":          "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
				"messageId":         "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
				"properties": map[string]interface{}{
					"label":    "",
					"value":    float64(1),
					"testMap":  nil,
					"category": "",
					"floatVal": 4.51,
				},
				"integrations": map[string]interface{}{
					"All": true,
				},
			}
			_, err3 := json.Marshal(eventWithoutConsentManagementData)
			Expect(err3).To(BeNil())

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			Setup(processor, c, false, false)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())

			Expect(processor.isDestinationAvailable(eventWithDeniedConsents, SourceIDOneTrustConsent, "")).To(BeTrue())
			Expect(
				len(processor.getConsentFilteredDestinations(
					eventWithDeniedConsents,
					processor.getEnabledDestinations(
						SourceIDOneTrustConsent,
						"destination-definition-name-enabled",
					),
				)),
			).To(Equal(3)) // all except D1 and D3

			Expect(processor.isDestinationAvailable(eventWithoutDeniedConsents, SourceIDOneTrustConsent, "")).To(BeTrue())
			Expect(
				len(processor.getConsentFilteredDestinations(
					eventWithoutDeniedConsents,
					processor.getEnabledDestinations(
						SourceIDOneTrustConsent,
						"destination-definition-name-enabled",
					),
				)),
			).To(Equal(5)) // all

			Expect(processor.isDestinationAvailable(eventWithoutConsentManagementData, SourceIDOneTrustConsent, "")).To(BeTrue())
			Expect(
				len(processor.getConsentFilteredDestinations(
					eventWithoutConsentManagementData,
					processor.getEnabledDestinations(
						SourceIDOneTrustConsent,
						"destination-definition-name-enabled",
					),
				)),
			).To(Equal(5)) // all

			Expect(processor.isDestinationAvailable(eventWithoutConsentManagementData, SourceIDOneTrustConsent, "dest-id-1")).To(BeTrue())
			Expect(
				len(processor.getConsentFilteredDestinations(
					eventWithoutConsentManagementData,
					processor.getEnabledDestinations(
						SourceIDOneTrustConsent,
						"destination-definition-name-enabled",
					),
				)),
			).To(Equal(5)) // all
		})

		It("should filter based on ketch consent management preferences", func() {
			event := types.SingularEventT{
				"originalTimestamp": "2019-03-10T10:10:10.10Z",
				"event":             "Demo Track",
				"sentAt":            "2019-03-10T10:10:10.10Z",
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"deniedConsentIds": []interface{}{"purpose1", "purpose2", "someOtherCategory"},
					},
				},
				"type":      "track",
				"channel":   "android-srk",
				"rudderId":  "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
				"messageId": "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
				"properties": map[string]interface{}{
					"lbael":    "",
					"value":    float64(1),
					"testMap":  nil,
					"category": "",
					"floatVal": 4.51,
				},
				"integrations": map[string]interface{}{
					"All": true,
				},
			}
			_, err := json.Marshal(event)
			Expect(err).To(BeNil())

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			processor := prepareHandle(NewHandle(config.Default, mocksTransformer.NewMockTransformer(c.mockCtrl)))

			Setup(processor, c, false, false)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())

			filteredDestinations := processor.getConsentFilteredDestinations(
				event,
				processor.getEnabledDestinations(
					SourceIDKetchConsent,
					"destination-definition-name-enabled",
				),
			)
			Expect(len(filteredDestinations)).To(Equal(4)) // all except dest-id-5 since both purpose1 and purpose2 are denied
			Expect(processor.isDestinationAvailable(event, SourceIDKetchConsent, "")).To(BeTrue())
		})

		It("should filter based on generic consent management preferences", func() {
			eventWithoutConsentManagementData := types.SingularEventT{
				"originalTimestamp": "2019-03-10T10:10:10.10Z",
				"event":             "Demo Track",
				"sentAt":            "2019-03-10T10:10:10.10Z",
				"context":           map[string]interface{}{},
				"type":              "track",
				"channel":           "mobile",
				"rudderId":          "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
				"messageId":         "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
				"properties": map[string]interface{}{
					"label":    "",
					"value":    float64(1),
					"testMap":  nil,
					"category": "",
					"floatVal": 4.51,
				},
				"integrations": map[string]interface{}{
					"All": true,
				},
			}
			_, err3 := json.Marshal(eventWithoutConsentManagementData)
			Expect(err3).To(BeNil())

			eventWithDeniedConsentsGCM := types.SingularEventT{
				"originalTimestamp": "2019-03-10T10:10:10.10Z",
				"event":             "Demo Track",
				"sentAt":            "2019-03-10T10:10:10.10Z",
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "oneTrust",
						"resolutionStrategy": "and",
						"allowedConsentIds":  []interface{}{"consent category 1"},
						"deniedConsentIds":   []interface{}{"consent category 2", "someOtherCategory"},
					},
				},
				"type":      "track",
				"channel":   "mobile",
				"rudderId":  "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
				"messageId": "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
				"properties": map[string]interface{}{
					"label":    "",
					"value":    float64(1),
					"testMap":  nil,
					"category": "",
					"floatVal": 4.51,
				},
				"integrations": map[string]interface{}{
					"All": true,
				},
			}
			_, err4 := json.Marshal(eventWithDeniedConsentsGCM)
			Expect(err4).To(BeNil())

			eventWithoutDeniedConsentsGCM := types.SingularEventT{
				"originalTimestamp": "2019-03-10T10:10:10.10Z",
				"event":             "Demo Track",
				"sentAt":            "2019-03-10T10:10:10.10Z",
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "oneTrust",
						"resolutionStrategy": "and",
						"allowedConsentIds":  []interface{}{"consent category 1", "consent category 2", "someOtherCategory", "consent category 3"},
						"deniedConsentIds":   []interface{}{},
					},
				},
				"type":      "track",
				"channel":   "mobile",
				"rudderId":  "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
				"messageId": "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
				"properties": map[string]interface{}{
					"label":    "",
					"value":    float64(1),
					"testMap":  nil,
					"category": "",
					"floatVal": 4.51,
				},
				"integrations": map[string]interface{}{
					"All": true,
				},
			}
			_, err5 := json.Marshal(eventWithoutDeniedConsentsGCM)
			Expect(err5).To(BeNil())

			eventWithCustomConsentsGCM := types.SingularEventT{
				"originalTimestamp": "2019-03-10T10:10:10.10Z",
				"event":             "Demo Track",
				"sentAt":            "2019-03-10T10:10:10.10Z",
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":          "custom",
						"allowedConsentIds": []interface{}{"consent category 1", "consent category 2"},
						"deniedConsentIds":  []interface{}{"someOtherCategory", "consent category 3"},
					},
				},
				"type":      "track",
				"channel":   "mobile",
				"rudderId":  "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
				"messageId": "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
				"properties": map[string]interface{}{
					"label":    "",
					"value":    float64(1),
					"testMap":  nil,
					"category": "",
					"floatVal": 4.51,
				},
				"integrations": map[string]interface{}{
					"All": true,
				},
			}
			_, err6 := json.Marshal(eventWithCustomConsentsGCM)
			Expect(err6).To(BeNil())

			eventWithDeniedConsentsGCMKetch := types.SingularEventT{
				"originalTimestamp": "2019-03-10T10:10:10.10Z",
				"event":             "Demo Track",
				"sentAt":            "2019-03-10T10:10:10.10Z",
				"context": map[string]interface{}{
					"consentManagement": map[string]interface{}{
						"provider":           "ketch",
						"resolutionStrategy": "or",
						"allowedConsentIds":  []interface{}{"consent category 2"},
						"deniedConsentIds":   []interface{}{"purpose 1", "purpose 3"},
					},
				},
				"type":      "track",
				"channel":   "mobile",
				"rudderId":  "90ca6da0-292e-4e79-9880-f8009e0ae4a3",
				"messageId": "f9b9b8f0-c8e9-4f7b-b8e8-f8f8f8f8f8f8",
				"properties": map[string]interface{}{
					"label":    "",
					"value":    float64(1),
					"testMap":  nil,
					"category": "",
					"floatVal": 4.51,
				},
				"integrations": map[string]interface{}{
					"All": true,
				},
			}
			_, err7 := json.Marshal(eventWithDeniedConsentsGCMKetch)
			Expect(err7).To(BeNil())

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			Setup(processor, c, false, false)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())

			Expect(processor.isDestinationAvailable(eventWithoutConsentManagementData, SourceIDGCM, "")).To(BeTrue())
			Expect(
				len(processor.getConsentFilteredDestinations(
					eventWithoutConsentManagementData,
					processor.getEnabledDestinations(
						SourceIDGCM,
						"destination-definition-name-enabled",
					),
				)),
			).To(Equal(9)) // all

			Expect(processor.isDestinationAvailable(eventWithoutDeniedConsentsGCM, SourceIDGCM, "")).To(BeTrue())
			Expect(
				len(processor.getConsentFilteredDestinations(
					eventWithoutDeniedConsentsGCM,
					processor.getEnabledDestinations(
						SourceIDGCM,
						"destination-definition-name-enabled",
					),
				)),
			).To(Equal(9)) // all

			Expect(processor.isDestinationAvailable(eventWithCustomConsentsGCM, SourceIDGCM, "")).To(BeTrue())
			Expect(
				len(processor.getConsentFilteredDestinations(
					eventWithCustomConsentsGCM,
					processor.getEnabledDestinations(
						SourceIDGCM,
						"destination-definition-name-enabled",
					),
				)),
			).To(Equal(8)) // all except D13

			Expect(processor.isDestinationAvailable(eventWithDeniedConsentsGCM, SourceIDGCM, "")).To(BeTrue())
			Expect(
				len(processor.getConsentFilteredDestinations(
					eventWithDeniedConsentsGCM,
					processor.getEnabledDestinations(
						SourceIDGCM,
						"destination-definition-name-enabled",
					),
				)),
			).To(Equal(7)) // all except D6 and D7

			Expect(processor.isDestinationAvailable(eventWithDeniedConsentsGCMKetch, SourceIDGCM, "")).To(BeTrue())
			Expect(
				len(processor.getConsentFilteredDestinations(
					eventWithDeniedConsentsGCMKetch,
					processor.getEnabledDestinations(
						SourceIDGCM,
						"destination-definition-name-enabled",
					),
				)),
			).To(Equal(8)) // all except D7

			// some unknown destination ID is passed destination will be unavailable
			Expect(processor.isDestinationAvailable(eventWithDeniedConsentsGCMKetch, SourceIDGCM, "unknown-destination")).To(BeFalse())

			// known destination ID is passed and destination is enabled
			Expect(processor.isDestinationAvailable(eventWithDeniedConsentsGCMKetch, SourceIDTransient, DestinationIDEnabledA)).To(BeTrue())

			// know destination ID is passed and destination is not enabled
			Expect(processor.isDestinationAvailable(eventWithDeniedConsentsGCMKetch, SourceIDTransient, DestinationIDDisabled)).To(BeFalse())
		})
	})

	Context("getNonSuccessfulMetrics", func() {
		It("getNonSuccessfulMetrics", func() {
			event1 := types.SingularEventT{
				"event":     "Demo Track1",
				"messageId": "msg1",
			}
			event2 := types.SingularEventT{
				"event":     "Demo Track2",
				"messageId": "msg2",
			}
			event3 := types.SingularEventT{
				"event":     "Demo Track3",
				"messageId": "msg3",
			}

			c.mockGatewayJobsDB.EXPECT().DeleteExecuting().Times(1)

			mockTransformer := mocksTransformer.NewMockTransformer(c.mockCtrl)

			processor := prepareHandle(NewHandle(config.Default, mockTransformer))

			Setup(processor, c, false, true)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())

			commonMetadata := transformer.Metadata{SourceID: SourceIDEnabled, DestinationID: DestinationIDEnabledA}
			singularEventWithReceivedAt1 := types.SingularEventWithReceivedAt{
				SingularEvent: event1,
				ReceivedAt:    time.Now(),
			}
			singularEventWithReceivedAt2 := types.SingularEventWithReceivedAt{
				SingularEvent: event2,
				ReceivedAt:    time.Now(),
			}
			singularEventWithReceivedAt3 := types.SingularEventWithReceivedAt{
				SingularEvent: event3,
				ReceivedAt:    time.Now(),
			}
			eventsByMessageID := map[string]types.SingularEventWithReceivedAt{
				"msg1": singularEventWithReceivedAt1,
				"msg2": singularEventWithReceivedAt2,
				"msg3": singularEventWithReceivedAt3,
			}
			metadata1 := commonMetadata
			metadata1.MessageID = "msg1"
			metadata2 := commonMetadata
			metadata2.MessageID = "msg2"
			metadata3 := commonMetadata
			metadata3.MessageID = "msg3"

			FailedEvents := []transformer.TransformerResponse{
				{StatusCode: 400, Metadata: metadata1, Output: event1},
				{StatusCode: 298, Metadata: metadata2, Output: event2},
				{StatusCode: 299, Metadata: metadata3, Output: event2},
			}

			transformerResponse := transformer.Response{
				Events:       []transformer.TransformerResponse{},
				FailedEvents: FailedEvents,
			}

			m := processor.getNonSuccessfulMetrics(transformerResponse,
				&commonMetadata,
				eventsByMessageID,
				types.EVENT_FILTER,
				types.DEST_TRANSFORMER,
			)

			key := strings.Join([]string{
				commonMetadata.SourceID,
				commonMetadata.DestinationID,
				commonMetadata.SourceJobRunID,
				commonMetadata.EventName,
				commonMetadata.EventType,
			}, "!<<#>>!")

			Expect(len(m.failedJobs)).To(Equal(2))
			Expect(len(m.failedMetrics)).To(Equal(2))
			slices.SortFunc(m.failedMetrics, func(a, b *types.PUReportedMetric) int {
				return a.StatusDetail.StatusCode - b.StatusDetail.StatusCode
			})
			Expect(m.failedMetrics[0].StatusDetail.StatusCode).To(Equal(299))
			Expect(m.failedMetrics[1].StatusDetail.StatusCode).To(Equal(400))
			Expect(int(m.failedCountMap[key])).To(Equal(2))

			Expect(len(m.filteredJobs)).To(Equal(1))
			Expect(len(m.filteredMetrics)).To(Equal(1))
			Expect(int(m.filteredCountMap[key])).To(Equal(1))
		})
	})
})

var _ = Describe("Static Function Tests", func() {
	initProcessor()

	Context("TransformerFormatResponse Tests", func() {
		It("Should match ConvertToTransformerResponse without filtering", func() {
			events := []transformer.TransformerEvent{
				{
					Metadata: transformer.Metadata{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"some-key-1": "some-value-1",
					},
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
					},
				},
			}
			expectedResponses := transformer.Response{
				Events: []transformer.TransformerResponse{
					{
						Output: map[string]interface{}{
							"some-key-1": "some-value-1",
						},
						StatusCode: 200,
						Metadata: transformer.Metadata{
							MessageID: "message-1",
						},
					},
					{
						Output: map[string]interface{}{
							"some-key-2": "some-value-2",
						},
						StatusCode: 200,
						Metadata: transformer.Metadata{
							MessageID: "message-2",
						},
					},
				},
			}
			response := ConvertToFilteredTransformerResponse(events, false, func(event transformer.TransformerEvent) (bool, string) { return false, "" })
			Expect(response.Events[0].StatusCode).To(Equal(expectedResponses.Events[0].StatusCode))
			Expect(response.Events[0].Metadata.MessageID).To(Equal(expectedResponses.Events[0].Metadata.MessageID))
			Expect(response.Events[0].Output["some-key-1"]).To(Equal(expectedResponses.Events[0].Output["some-key-1"]))
			Expect(response.Events[1].StatusCode).To(Equal(expectedResponses.Events[1].StatusCode))
			Expect(response.Events[1].Metadata.MessageID).To(Equal(expectedResponses.Events[1].Metadata.MessageID))
			Expect(response.Events[1].Output["some-key-2"]).To(Equal(expectedResponses.Events[1].Output["some-key-2"]))
		})
	})

	Context("getDiffMetrics Tests", func() {
		It("Should match diffMetrics response for Empty Inputs", func() {
			response := getDiffMetrics(
				"some-string-1",
				"some-string-2",
				map[string]MetricMetadata{},
				map[string]int64{},
				map[string]int64{},
				map[string]int64{},
				map[string]int64{},
				stats.NOP,
			)
			Expect(len(response)).To(Equal(0))
		})

		It("Should match diffMetrics response for Valid Inputs", func() {
			inCountMetadataMap := map[string]MetricMetadata{
				"some-key-1": {
					sourceID:        "some-source-id-1",
					destinationID:   "some-destination-id-1",
					sourceJobRunID:  "some-source-job-run-id-1",
					sourceJobID:     "some-source-job-id-1",
					sourceTaskRunID: "some-source-task-run-id-1",
				},
				"some-key-2": {
					sourceID:        "some-source-id-2",
					destinationID:   "some-destination-id-2",
					sourceJobRunID:  "some-source-job-run-id-2",
					sourceJobID:     "some-source-job-id-2",
					sourceTaskRunID: "some-source-task-run-id-2",
				},
			}

			inCountMap := map[string]int64{
				"some-key-1": 3,
				"some-key-2": 4,
			}
			successCountMap := map[string]int64{
				"some-key-1": 5,
				"some-key-2": 6,
			}
			failedCountMap := map[string]int64{
				"some-key-1": 1,
				"some-key-2": 2,
			}
			filteredCountMap := map[string]int64{
				"some-key-1": 2,
				"some-key-2": 3,
			}

			expectedResponse := []*types.PUReportedMetric{
				{
					ConnectionDetails: types.ConnectionDetails{
						SourceID:        "some-source-id-1",
						DestinationID:   "some-destination-id-1",
						SourceTaskRunID: "some-source-task-run-id-1",
						SourceJobID:     "some-source-job-id-1",
						SourceJobRunID:  "some-source-job-run-id-1",
					},
					PUDetails: types.PUDetails{
						InPU:       "some-string-1",
						PU:         "some-string-2",
						TerminalPU: false,
						InitialPU:  false,
					},
					StatusDetail: &types.StatusDetail{
						Status:         "diff",
						Count:          5,
						StatusCode:     0,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
					},
				},
				{
					ConnectionDetails: types.ConnectionDetails{
						SourceID:        "some-source-id-2",
						DestinationID:   "some-destination-id-2",
						SourceTaskRunID: "some-source-task-run-id-2",
						SourceJobID:     "some-source-job-id-2",
						SourceJobRunID:  "some-source-job-run-id-2",
					},
					PUDetails: types.PUDetails{
						InPU:       "some-string-1",
						PU:         "some-string-2",
						TerminalPU: false,
						InitialPU:  false,
					},
					StatusDetail: &types.StatusDetail{
						Status:         "diff",
						Count:          7,
						StatusCode:     0,
						SampleResponse: "",
						SampleEvent:    []byte(`{}`),
					},
				},
			}

			statsStore, err := memstats.New()
			Expect(err).To(BeNil())
			response := getDiffMetrics(
				"some-string-1",
				"some-string-2",
				inCountMetadataMap,
				inCountMap,
				successCountMap,
				failedCountMap,
				filteredCountMap,
				statsStore,
			)
			Expect(statsStore.Get("processor_diff_count", stats.Tags{
				"stage":         "some-string-2",
				"sourceId":      "some-source-id-1",
				"destinationId": "some-destination-id-1",
			}).LastValue()).To(Equal(float64(-5)))
			Expect(statsStore.Get("processor_diff_count", stats.Tags{
				"stage":         "some-string-2",
				"sourceId":      "some-source-id-2",
				"destinationId": "some-destination-id-2",
			}).LastValue()).To(Equal(float64(-7)))
			assertReportMetric(expectedResponse, response)
		})
	})

	Context("updateMetricMaps Tests", func() {
		It("Should update metric maps", func() {
			proc := NewHandle(config.Default, nil)
			proc.reportingEnabled = true
			proc.reporting = &mockReportingTypes.MockReporting{}

			inputEvent := &transformer.TransformerResponse{
				Metadata: transformer.Metadata{
					SourceID:         "source-id-1",
					DestinationID:    "destination-id-1",
					TransformationID: "transformation-id-1",
					TrackingPlanID:   "tracking-plan-id-1",
					EventName:        "event-name-1",
					EventType:        "event-type-1",
				},
				StatusCode: 200,
				Error:      "",
				ValidationErrors: []transformer.ValidationError{
					{
						Type:    "type-1",
						Message: "message-1",
					},
					{
						Type:    "type-1",
						Message: "message-2",
					},
					{
						Type:    "type-2",
						Message: "message-1",
					},
				},
			}
			expectedMetricMetadata := MetricMetadata{
				sourceID:                inputEvent.Metadata.SourceID,
				destinationID:           inputEvent.Metadata.DestinationID,
				sourceJobRunID:          inputEvent.Metadata.SourceJobRunID,
				sourceJobID:             inputEvent.Metadata.SourceJobID,
				sourceTaskRunID:         inputEvent.Metadata.SourceTaskRunID,
				sourceDefinitionID:      inputEvent.Metadata.SourceDefinitionID,
				destinationDefinitionID: inputEvent.Metadata.DestinationDefinitionID,
				sourceCategory:          inputEvent.Metadata.SourceCategory,
				transformationID:        inputEvent.Metadata.TransformationID,
				transformationVersionID: inputEvent.Metadata.TransformationVersionID,
				trackingPlanID:          inputEvent.Metadata.TrackingPlanID,
				trackingPlanVersion:     inputEvent.Metadata.TrackingPlanVersion,
			}
			expectedConnectionDetails := &types.ConnectionDetails{
				SourceID:                inputEvent.Metadata.SourceID,
				DestinationID:           inputEvent.Metadata.DestinationID,
				SourceJobRunID:          inputEvent.Metadata.SourceJobRunID,
				SourceJobID:             inputEvent.Metadata.SourceJobID,
				SourceTaskRunID:         inputEvent.Metadata.SourceTaskRunID,
				SourceDefinitionID:      inputEvent.Metadata.SourceDefinitionID,
				DestinationDefinitionID: inputEvent.Metadata.DestinationDefinitionID,
				SourceCategory:          inputEvent.Metadata.SourceCategory,
				TransformationID:        inputEvent.Metadata.TransformationID,
				TransformationVersionID: inputEvent.Metadata.TransformationVersionID,
				TrackingPlanID:          inputEvent.Metadata.TrackingPlanID,
				TrackingPlanVersion:     inputEvent.Metadata.TrackingPlanVersion,
			}
			connectionDetailsMap := make(map[string]*types.ConnectionDetails)
			statusDetailsMap := make(map[string]map[string]*types.StatusDetail)
			countMap := make(map[string]int64)
			countMetadataMap := make(map[string]MetricMetadata)
			// update metric maps
			proc.updateMetricMaps(countMetadataMap, countMap, connectionDetailsMap, statusDetailsMap, inputEvent, jobsdb.Succeeded.State, types.TRACKINGPLAN_VALIDATOR, func() json.RawMessage { return []byte(`{}`) }, nil)

			Expect(len(countMetadataMap)).To(Equal(1))
			Expect(len(countMap)).To(Equal(1))
			for k := range countMetadataMap {
				Expect(countMetadataMap[k]).To(Equal(expectedMetricMetadata))
				Expect(countMap[k]).To(Equal(int64(1)))
			}

			Expect(len(connectionDetailsMap)).To(Equal(1))
			Expect(len(statusDetailsMap)).To(Equal(1))
			for k := range connectionDetailsMap {
				Expect(connectionDetailsMap[k]).To(Equal(expectedConnectionDetails))
				Expect(len(statusDetailsMap[k])).To(Equal(3)) // count of distinct error types: "type-1", "type-2", ""
			}
		})
	})

	Context("ConvertToTransformerResponse Tests", func() {
		It("Should filter out unsupported message types", func() {
			destinationConfig := backendconfig.DestinationT{
				IsProcessorEnabled: true,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"identify"},
					},
				},
			}

			events := []transformer.TransformerEvent{
				{
					Metadata: transformer.Metadata{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"some-key-1": "some-value-1",
						"type":       "  IDENTIFY ",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       "track",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       123,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.Response{
				Events: []transformer.TransformerResponse{
					{
						Output:     events[0].Message,
						StatusCode: 200,
						Metadata:   events[0].Metadata,
					},
				},
				FailedEvents: []transformer.TransformerResponse{
					{
						Output:     events[1].Message,
						StatusCode: 298,
						Metadata:   events[1].Metadata,
						Error:      "Message type not supported",
					},
					{
						Output:     events[2].Message,
						StatusCode: 400,
						Metadata:   events[2].Metadata,
						Error:      "Invalid message type. Type assertion failed",
					},
				},
			}
			response := ConvertToFilteredTransformerResponse(events, true, func(_ transformer.TransformerEvent) (bool, string) { return false, "" })
			Expect(response).To(Equal(expectedResponse))
		})

		It("Should filter out identify events when it is supported but serverSideIdentify is disabled", func() {
			destinationConfig := backendconfig.DestinationT{
				ID: "some-destination-id",
				Config: map[string]interface{}{
					"enableServerSideIdentify": false,
				},
				IsProcessorEnabled: true, // assuming the mode is cloud/hybrid
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"identify", "track"},
					},
				},
			}

			events := []transformer.TransformerEvent{
				{
					Metadata: transformer.Metadata{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"some-key-1": "some-value-1",
						"type":       "  IDENTIFY ",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       "track",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       123,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.Response{
				Events: []transformer.TransformerResponse{
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
				},
				FailedEvents: []transformer.TransformerResponse{
					{
						Output:     events[0].Message,
						StatusCode: 298,
						Metadata:   events[0].Metadata,
						Error:      "Message type not supported",
					},
					{
						Output:     events[2].Message,
						StatusCode: 400,
						Metadata:   events[2].Metadata,
						Error:      "Invalid message type. Type assertion failed",
					},
				},
			}
			response := ConvertToFilteredTransformerResponse(events, true, func(event transformer.TransformerEvent) (bool, string) { return false, "" })
			Expect(response).To(Equal(expectedResponse))
		})

		It("Should allow all events when no supportedMessageTypes key is present in config", func() {
			destinationConfig := backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{},
				},
			}

			events := []transformer.TransformerEvent{
				{
					Metadata: transformer.Metadata{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"some-key-1": "some-value-1",
						"type":       "  IDENTIFY ",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       "track",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       123,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.Response{
				Events: []transformer.TransformerResponse{
					{
						Output:     events[0].Message,
						StatusCode: 200,
						Metadata:   events[0].Metadata,
					},
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
					{
						Output:     events[2].Message,
						StatusCode: 200,
						Metadata:   events[2].Metadata,
					},
				},
				FailedEvents: nil,
			}
			response := ConvertToFilteredTransformerResponse(events, true, func(event transformer.TransformerEvent) (bool, string) { return false, "" })
			Expect(response).To(Equal(expectedResponse))
		})
		It("Should allow all events when supportedMessageTypes is not an array", func() {
			destinationConfig := backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"supportedMessageTypes": "identify",
					},
				},
			}

			events := []transformer.TransformerEvent{
				{
					Metadata: transformer.Metadata{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"some-key-1": "some-value-1",
						"type":       "  IDENTIFY ",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       "track",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"some-key-2": "some-value-2",
						"type":       123,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.Response{
				Events: []transformer.TransformerResponse{
					{
						Output:     events[0].Message,
						StatusCode: 200,
						Metadata:   events[0].Metadata,
					},
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
					{
						Output:     events[2].Message,
						StatusCode: 200,
						Metadata:   events[2].Metadata,
					},
				},
				FailedEvents: nil,
			}
			response := ConvertToFilteredTransformerResponse(events, true, func(event transformer.TransformerEvent) (bool, string) { return false, "" })
			Expect(response).To(Equal(expectedResponse))
		})

		It("Should filter out messages containing unsupported events", func() {
			destinationConfig := backendconfig.DestinationT{
				Config: map[string]interface{}{
					"enableServerSideIdentify": false,
					"listOfConversions": []interface{}{
						map[string]interface{}{"conversions": "Credit Card Added"},
						map[string]interface{}{"conversions": "Credit Card Removed"},
					},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{},
			}

			events := []transformer.TransformerEvent{
				{
					Metadata: transformer.Metadata{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Cart Cleared",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Credit Card Added",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": 2,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.Response{
				Events: []transformer.TransformerResponse{
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
				},
				FailedEvents: []transformer.TransformerResponse{
					{
						Output:     events[0].Message,
						StatusCode: 298,
						Metadata:   events[0].Metadata,
						Error:      "Event not supported",
					},
					{
						Output:     events[2].Message,
						StatusCode: 400,
						Metadata:   events[2].Metadata,
						Error:      "Invalid message event. Type assertion failed",
					},
				},
			}
			response := ConvertToFilteredTransformerResponse(events, true, func(event transformer.TransformerEvent) (bool, string) { return false, "" })
			Expect(response).To(Equal(expectedResponse))
		})

		It("Shouldn;t filter out messages if listOfConversions config is corrupted", func() {
			destinationConfig := backendconfig.DestinationT{
				Config: map[string]interface{}{
					"enableServerSideIdentify": false,
					"listOfConversions": []interface{}{
						map[string]interface{}{"conversions": "Credit Card Added"},
						map[string]interface{}{"conversions": 1},
					},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{},
				},
			}

			events := []transformer.TransformerEvent{
				{
					Metadata: transformer.Metadata{
						MessageID: "message-1",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Cart Cleared",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Credit Card Added",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID: "message-2",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": 2,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.Response{
				Events: []transformer.TransformerResponse{
					{
						Output:     events[0].Message,
						StatusCode: 200,
						Metadata:   events[0].Metadata,
					},
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
					{
						Output:     events[2].Message,
						StatusCode: 200,
						Metadata:   events[2].Metadata,
					},
				},
				FailedEvents: nil,
			}
			response := ConvertToFilteredTransformerResponse(events, true, func(event transformer.TransformerEvent) (bool, string) { return false, "" })
			Expect(response).To(Equal(expectedResponse))
		})

		It("When web sourceType is sending events to cloud-mode destination, should filter out any event types that are not track/group", func() {
			destinationConfig := backendconfig.DestinationT{
				IsProcessorEnabled: true,
				Config: map[string]interface{}{
					"enableServerSideIdentify": false,
					"listOfConversions": []interface{}{
						map[string]interface{}{"conversions": "Credit Card Added"},
						map[string]interface{}{"conversions": 1},
					},
					"connectionMode": "cloud",
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "group", "page"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
						"hybridModeCloudEventsFilter": map[string]interface{}{
							"web": map[string]interface{}{
								"messageType": []interface{}{"track", "group"},
							},
						},
					},
				},
			}

			events := []transformer.TransformerEvent{
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-1",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Cart Cleared",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-2",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "identify",
						"event": "User Authenticated",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-2",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "screen",
						"event": 2,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.Response{
				Events: []transformer.TransformerResponse{
					{
						Output:     events[0].Message,
						StatusCode: 200,
						Metadata:   events[0].Metadata,
					},
				},
				FailedEvents: []transformer.TransformerResponse{
					{
						Output:     events[1].Message,
						StatusCode: 298,
						Metadata:   events[1].Metadata,
						Error:      "Message type not supported",
					},
					{
						Output:     events[2].Message,
						StatusCode: 298,
						Metadata:   events[2].Metadata,
						Error:      "Message type not supported",
					},
				},
			}
			response := ConvertToFilteredTransformerResponse(events, true, func(event transformer.TransformerEvent) (bool, string) { return false, "" })
			Expect(response).To(Equal(expectedResponse))
		})

		It("When web sourceType is sending events to device-mode destination, should filter out all the events to this destination", func() {
			destinationConfig := backendconfig.DestinationT{
				IsProcessorEnabled: false,
				Config: map[string]interface{}{
					"enableServerSideIdentify": false,
					"listOfConversions": []interface{}{
						map[string]interface{}{"conversions": "Credit Card Added"},
						map[string]interface{}{"conversions": 1},
					},
					"connectionMode": "device",
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "group", "alias"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
						"hybridModeCloudEventsFilter": map[string]interface{}{
							"web": map[string]interface{}{
								"messageType": []interface{}{"track", "group"},
							},
						},
					},
				},
			}

			events := []transformer.TransformerEvent{
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-1",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Cart Cleared",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-2",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Credit Card Added",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-2",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "screen",
						"event": 2,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.Response{
				Events: nil,
				FailedEvents: []transformer.TransformerResponse{
					{
						Output:     events[0].Message,
						StatusCode: 298,
						Metadata:   events[0].Metadata,
						Error:      "Filtering event based on hybridModeFilter",
					},
					{
						Output:     events[1].Message,
						StatusCode: 298,
						Metadata:   events[1].Metadata,
						Error:      "Filtering event based on hybridModeFilter",
					},
					{
						Output:     events[2].Message,
						StatusCode: 298,
						Metadata:   events[2].Metadata,
						Error:      "Message type not supported",
					},
				},
			}
			response := ConvertToFilteredTransformerResponse(events, true, func(event transformer.TransformerEvent) (bool, string) { return false, "" })
			Expect(response).To(Equal(expectedResponse))
		})

		It("When web sourceType is sending events to hybrid-mode destination, should filter out any event types that are not track/group", func() {
			destinationConfig := backendconfig.DestinationT{
				IsProcessorEnabled: true,
				Config: map[string]interface{}{
					"enableServerSideIdentify": false,
					"listOfConversions": []interface{}{
						map[string]interface{}{"conversions": "Credit Card Added"},
						map[string]interface{}{"conversions": 1},
					},
					"connectionMode": "hybrid",
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "group", "alias"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
						"hybridModeCloudEventsFilter": map[string]interface{}{
							"web": map[string]interface{}{
								"messageType": []interface{}{"track", "group"},
							},
						},
					},
				},
			}

			events := []transformer.TransformerEvent{
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-1",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Cart Cleared",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-2",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Credit Card Added",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-2",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "screen",
						"event": 2,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.Response{
				Events: []transformer.TransformerResponse{
					{
						Output:     events[0].Message,
						StatusCode: 200,
						Metadata:   events[0].Metadata,
					},
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
				},
				FailedEvents: []transformer.TransformerResponse{
					{
						Output:     events[2].Message,
						StatusCode: 298,
						Metadata:   events[2].Metadata,
						Error:      "Message type not supported",
					},
				},
			}
			response := ConvertToFilteredTransformerResponse(events, true, func(event transformer.TransformerEvent) (bool, string) { return false, "" })
			Expect(response).To(Equal(expectedResponse))
		})

		It("When web sourceType is sending events to hybrid-mode destination & hybridModeCloudEventsFilter is not present, should filter out all the events with event.type not in supportedMessageTypes to this destination", func() {
			destinationConfig := backendconfig.DestinationT{
				IsProcessorEnabled: true,
				Config: map[string]interface{}{
					"enableServerSideIdentify": false,
					"listOfConversions": []interface{}{
						map[string]interface{}{"conversions": "Credit Card Added"},
						map[string]interface{}{"conversions": 1},
					},
					"connectionMode": "hybrid",
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "group", "alias"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
					},
				},
			}

			events := []transformer.TransformerEvent{
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-1",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "group",
						"event": "Cart Cleared",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-2",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Credit Card Added",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-2",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "screen",
						"event": 2,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.Response{
				Events: []transformer.TransformerResponse{
					{
						Output:     events[0].Message,
						StatusCode: 200,
						Metadata:   events[0].Metadata,
					},
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
				},
				FailedEvents: []transformer.TransformerResponse{
					{
						Output:     events[2].Message,
						StatusCode: 298,
						Metadata:   events[2].Metadata,
						Error:      "Message type not supported",
					},
				},
			}
			response := ConvertToFilteredTransformerResponse(events, true, func(event transformer.TransformerEvent) (bool, string) { return false, "" })
			Expect(response).To(Equal(expectedResponse))
		})

		It("When web sourceType is sending events to hybrid-mode destination & hybridModeCloudEventsFilter.web is a string, should filter out all the events with event.type not in supportedMessageTypes to this destination", func() {
			destinationConfig := backendconfig.DestinationT{
				IsProcessorEnabled: true,
				Config: map[string]interface{}{
					"enableServerSideIdentify": false,
					"listOfConversions": []interface{}{
						map[string]interface{}{"conversions": "Credit Card Added"},
						map[string]interface{}{"conversions": 1},
					},
					"connectionMode": "hybrid",
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "group", "alias"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
						"hybridModeCloudEventsFilter": map[string]interface{}{
							"web": "al",
						},
					},
				},
			}

			events := []transformer.TransformerEvent{
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-1",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "group",
						"event": "Cart Cleared",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-2",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Credit Card Added",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-2",
						SourceDefinitionType: "web",
					},
					Message: map[string]interface{}{
						"type":  "screen",
						"event": 2,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.Response{
				Events: []transformer.TransformerResponse{
					{
						Output:     events[0].Message,
						StatusCode: 200,
						Metadata:   events[0].Metadata,
					},
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
				},
				FailedEvents: []transformer.TransformerResponse{
					{
						Output:     events[2].Message,
						StatusCode: 298,
						Metadata:   events[2].Metadata,
						Error:      "Message type not supported",
					},
				},
			}
			response := ConvertToFilteredTransformerResponse(events, true, func(event transformer.TransformerEvent) (bool, string) { return false, "" })
			Expect(response).To(Equal(expectedResponse))
		})

		// source-type(in sourceDefinition) is empty string
		It("When web sourceType is sending events to hybrid-mode destination & hybridModeCloudEventsFilter.web.messageType supports track only but sourceType is coming up as empty string, should filter out all the events with event.type not in supportedMessageTypes", func() {
			destinationConfig := backendconfig.DestinationT{
				IsProcessorEnabled: true,
				Config: map[string]interface{}{
					"enableServerSideIdentify": false,
					"listOfConversions": []interface{}{
						map[string]interface{}{"conversions": "Credit Card Added"},
						map[string]interface{}{"conversions": 1},
					},
					"connectionMode": "hybrid",
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"supportedMessageTypes": []interface{}{"track", "group", "alias"},
						"supportedConnectionModes": map[string]interface{}{
							"android": []interface{}{
								"cloud",
								"device",
							},
							"web": []interface{}{
								"cloud",
								"device",
								"hybrid",
							},
						},
						"hybridModeCloudEventsFilter": map[string]interface{}{
							"web": map[string]interface{}{
								"messageType": []interface{}{"track"},
							},
						},
					},
				},
			}

			events := []transformer.TransformerEvent{
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-1",
						SourceDefinitionType: "",
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "Cart Cleared",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-2",
						SourceDefinitionType: "",
					},
					Message: map[string]interface{}{
						"type":  "group",
						"event": "Credit Card Added",
					},
					Destination: destinationConfig,
				},
				{
					Metadata: transformer.Metadata{
						MessageID:            "message-2",
						SourceDefinitionType: "",
					},
					Message: map[string]interface{}{
						"type":  "screen",
						"event": 2,
					},
					Destination: destinationConfig,
				},
			}
			expectedResponse := transformer.Response{
				Events: []transformer.TransformerResponse{
					{
						Output:     events[0].Message,
						StatusCode: 200,
						Metadata:   events[0].Metadata,
					},
					{
						Output:     events[1].Message,
						StatusCode: 200,
						Metadata:   events[1].Metadata,
					},
				},
				FailedEvents: []transformer.TransformerResponse{
					{
						Output:     events[2].Message,
						StatusCode: 298,
						Metadata:   events[2].Metadata,
						Error:      "Message type not supported",
					},
				},
			}
			response := ConvertToFilteredTransformerResponse(events, true, func(event transformer.TransformerEvent) (bool, string) { return false, "" })
			Expect(response).To(Equal(expectedResponse))
		})
	})
})

type mockEventData struct {
	id                        string
	jobid                     int64
	originalTimestamp         string
	expectedOriginalTimestamp string
	sentAt                    string
	expectedSentAt            string
	expectedReceivedAt        string
	integrations              map[string]bool
	params                    map[string]string
}

type transformExpectation struct {
	events                    int
	messageIds                string
	receiveMetadata           bool
	destinationDefinitionName string
}

func createMessagePayload(e mockEventData) string {
	integrationsBytes, _ := json.Marshal(e.integrations)
	return fmt.Sprintf(
		`{"rudderId":"some-rudder-id","messageId":"message-%s","integrations":%s,"some-property":"property-%s",`+
			`"originalTimestamp":%q,"sentAt":%q,"recordId":{"id":"record_id_1"},"context":{"sources":`+
			`{"task_run_id":"task_run_id_1","batch_id":"batch_id_1","job_run_id":"job_run_id_1",`+
			`"task_id":"task_id_1","job_id":"job_id_1"}}}`,
		e.id, integrationsBytes, e.id, e.originalTimestamp, e.sentAt,
	)
}

func createMessagePayloadWithoutSources(e mockEventData) string {
	integrationsBytes, _ := json.Marshal(e.integrations)
	return fmt.Sprintf(
		`{"rudderId":"some-rudder-id","messageId":"message-%s","integrations":%s,"some-property":"property-%s",`+
			`"originalTimestamp":%q,"sentAt":%q,"context":{}}`,
		e.id, integrationsBytes, e.id, e.originalTimestamp, e.sentAt,
	)
}

func createMessagePayloadWithSameMessageId(e mockEventData) string {
	integrationsBytes, _ := json.Marshal(e.integrations)
	return fmt.Sprintf(
		`{"rudderId":"some-rudder-id","messageId":"message-%s","integrations":%s,"some-property":"property-%s",`+
			`"originalTimestamp":%q,"sentAt":%q}`, "some-id", integrationsBytes, e.id, e.originalTimestamp, e.sentAt,
	)
}

func createBatchPayload(writeKey, receivedAt string, events []mockEventData, eventCreator func(mockEventData) string) []byte {
	payloads := make([]string, 0)
	for _, event := range events {
		payloads = append(payloads, eventCreator(event))
	}
	batch := strings.Join(payloads, ",")
	return []byte(fmt.Sprintf(
		`{"writeKey":%q,"batch":[%s],"requestIP":"1.2.3.4","receivedAt":%q}`, writeKey, batch, receivedAt,
	))
}

func createBatchParameters(sourceId string) []byte {
	return []byte(fmt.Sprintf(`{"source_id":%q}`, sourceId))
}

func createBatchParametersWithSources(sourceId string) []byte {
	return []byte(fmt.Sprintf(`{"source_id":%q,"source_job_run_id":"job_run_id_1","source_task_run_id":"task_run_id_1"}`, sourceId))
}

func assertJobStatus(job *jobsdb.JobT, status *jobsdb.JobStatusT, expectedState string) {
	Expect(status.JobID).To(Equal(job.JobID))
	Expect(status.JobState).To(Equal(expectedState))
	Expect(status.RetryTime).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
	Expect(status.ExecTime).To(BeTemporally("~", time.Now(), 200*time.Millisecond))
}

func assertReportMetric(expectedMetric, actualMetric []*types.PUReportedMetric) {
	sort.Slice(expectedMetric, func(i, j int) bool {
		return expectedMetric[i].ConnectionDetails.SourceID < expectedMetric[j].ConnectionDetails.SourceID
	})
	sort.Slice(actualMetric, func(i, j int) bool {
		return actualMetric[i].ConnectionDetails.SourceID < actualMetric[j].ConnectionDetails.SourceID
	})
	Expect(len(expectedMetric)).To(Equal(len(actualMetric)))
	for index, value := range expectedMetric {
		Expect(value.ConnectionDetails.SourceID).To(Equal(actualMetric[index].ConnectionDetails.SourceID))
		Expect(value.ConnectionDetails.DestinationID).To(Equal(actualMetric[index].ConnectionDetails.DestinationID))
		Expect(value.ConnectionDetails.SourceJobID).To(Equal(actualMetric[index].ConnectionDetails.SourceJobID))
		Expect(value.ConnectionDetails.SourceJobRunID).To(Equal(actualMetric[index].ConnectionDetails.SourceJobRunID))
		Expect(value.ConnectionDetails.SourceTaskRunID).To(Equal(actualMetric[index].ConnectionDetails.SourceTaskRunID))
		Expect(value.PUDetails.InPU).To(Equal(actualMetric[index].PUDetails.InPU))
		Expect(value.PUDetails.PU).To(Equal(actualMetric[index].PUDetails.PU))
		Expect(value.PUDetails.TerminalPU).To(Equal(actualMetric[index].PUDetails.TerminalPU))
		Expect(value.PUDetails.InitialPU).To(Equal(actualMetric[index].PUDetails.InitialPU))
		Expect(value.StatusDetail.Status).To(Equal(actualMetric[index].StatusDetail.Status))
		Expect(value.StatusDetail.StatusCode).To(Equal(actualMetric[index].StatusDetail.StatusCode))
		Expect(value.StatusDetail.Count).To(Equal(actualMetric[index].StatusDetail.Count))
		Expect(value.StatusDetail.SampleResponse).To(Equal(actualMetric[index].StatusDetail.SampleResponse))
		Expect(value.StatusDetail.SampleEvent).To(Equal(actualMetric[index].StatusDetail.SampleEvent))

	}
}

func assertDestinationTransform(
	messages map[string]mockEventData,
	sourceId, destinationID string,
	expectations transformExpectation,
) func(
	ctx context.Context,
	clientEvents []transformer.TransformerEvent,
	batchSize int,
) transformer.Response {
	return func(
		ctx context.Context,
		clientEvents []transformer.TransformerEvent,
		batchSize int,
	) transformer.Response {
		defer GinkgoRecover()

		Expect(clientEvents).To(HaveLen(expectations.events))

		messageIDs := make([]string, 0)
		for i := range clientEvents {
			event := clientEvents[i]
			messageID := event.Message["messageId"].(string)
			messageIDs = append(messageIDs, messageID)

			// Expect all messages belong to same destination
			Expect(event.Destination.ID).To(Equal(destinationID))

			// Expect metadata
			Expect(event.Metadata.DestinationID).To(Equal(destinationID))

			// Metadata are stripped from destination transform, if a user transform occurred before.
			if expectations.receiveMetadata {
				Expect(event.Metadata.DestinationType).To(Equal(fmt.Sprintf("%s-definition-name", destinationID)))
				Expect(event.Metadata.JobID).To(Equal(messages[messageID].jobid))
				Expect(event.Metadata.MessageID).To(Equal(messageID))
				Expect(event.Metadata.SourceID).To(Equal(sourceId)) // ???
				Expect(event.Metadata.SourceName).To(Equal(sourceIDToName[sourceId]))
				rawEvent, err := json.Marshal(event)
				Expect(err).ToNot(HaveOccurred())
				recordID := gjson.GetBytes(rawEvent, "message.recordId").Value()
				if recordID == nil {
					Expect(event.Metadata.RecordID).To(BeNil())
				} else {
					Expect(event.Metadata.RecordID).To(Equal(recordID))
				}
				jobRunID := messages[messageID].params["source_job_run_id"]
				Expect(event.Metadata.SourceJobRunID).To(Equal(jobRunID))
				taskRunID := messages[messageID].params["source_task_run_id"]
				Expect(event.Metadata.SourceTaskRunID).To(Equal(taskRunID))
				sourcesJobID := gjson.GetBytes(rawEvent, "message.context.sources.job_id").String()
				Expect(event.Metadata.SourceJobID).To(Equal(sourcesJobID))
			} else {
				// Expect(event.Metadata.DestinationType).To(Equal(""))
				Expect(event.Metadata.JobID).To(Equal(int64(0)))
				Expect(event.Metadata.MessageID).To(Equal(""))
				Expect(event.Metadata.SourceID).To(Equal(sourceId)) // ???
				Expect(event.Metadata.SourceName).To(Equal(sourceIDToName[sourceId]))
			}

			// Expect timestamp fields
			expectTimestamp := func(input string, expected time.Time) {
				inputTime, _ := time.Parse(misc.RFC3339Milli, input)
				Expect(inputTime).To(BeTemporally("~", expected, time.Second))
			}

			parseTimestamp := func(timestamp string, defaultTimeStamp time.Time) time.Time {
				parsed, ok := time.Parse(misc.RFC3339Milli, timestamp)
				if ok != nil {
					return defaultTimeStamp
				} else {
					return parsed
				}
			}

			receivedAt := parseTimestamp(messages[messageID].expectedReceivedAt, time.Now())
			sentAt := parseTimestamp(messages[messageID].expectedSentAt, receivedAt)
			originalTimestamp := parseTimestamp(messages[messageID].expectedOriginalTimestamp, receivedAt)

			expectTimestamp(event.Message["receivedAt"].(string), receivedAt)
			expectTimestamp(event.Message["sentAt"].(string), sentAt)
			expectTimestamp(event.Message["originalTimestamp"].(string), originalTimestamp)
			expectTimestamp(event.Message["timestamp"].(string), misc.GetChronologicalTimeStamp(receivedAt, sentAt, originalTimestamp))

			// Expect message properties
			Expect(event.Message["some-property"].(string)).To(Equal(fmt.Sprintf("property-%s", messages[messageID].id)))

			if destinationID == "enabled-destination-b" {
				Expect(event.Message["user-transform"].(string)).To(Equal("value"))
			}
		}

		Expect(strings.Join(messageIDs, ",")).To(Equal(expectations.messageIds))

		return transformer.Response{
			Events: []transformer.TransformerResponse{
				{
					Output: map[string]interface{}{
						"int-value":    0,
						"string-value": fmt.Sprintf("value-%s", destinationID),
					},
					Metadata: transformer.Metadata{
						SourceID:      "source-from-transformer", // transformer should replay source id
						SourceName:    "source-from-transformer-name",
						DestinationID: "destination-from-transformer", // transformer should replay destination id
					},
				},
				{
					Output: map[string]interface{}{
						"int-value":    1,
						"string-value": fmt.Sprintf("value-%s", destinationID),
					},
					Metadata: transformer.Metadata{
						SourceID:      "source-from-transformer", // transformer should replay source id
						SourceName:    "source-from-transformer-name",
						DestinationID: "destination-from-transformer", // transformer should replay destination id
					},
				},
			},
		}
	}
}

func processorSetupAndAssertJobHandling(processor *Handle, c *testContext) {
	Setup(processor, c, false, false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	Expect(processor.config.asyncInit.WaitContext(ctx)).To(BeNil())
	GinkgoT().Log("Processor setup and init done")
	handlePendingGatewayJobs(processor)
}

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
		trackedusers.NewNoopDataCollector(),
	)
	Expect(err).To(BeNil())
	processor.reportingEnabled = enableReporting
	processor.sourceObservers = []sourceObserver{c.MockObserver}
}

func handlePendingGatewayJobs(processor *Handle) {
	didWork := processor.handlePendingGatewayJobs("")
	Expect(didWork).To(Equal(true))
}

var _ = Describe("TestJobSplitter", func() {
	jobs := []*jobsdb.JobT{
		{
			JobID: 1,
		},
		{
			JobID: 2,
		},
		{
			JobID: 3,
		},
		{
			JobID: 4,
		},
		{
			JobID: 5,
		},
	}
	Context("testing jobs splitter, which split jobs into some sub-jobs", func() {
		It("default subJobSize: 2k", func() {
			proc := NewHandle(config.Default, nil)
			expectedSubJobs := []subJob{
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 1,
						},
						{
							JobID: 2,
						},
						{
							JobID: 3,
						},
						{
							JobID: 4,
						},
						{
							JobID: 5,
						},
					},
					hasMore: false,
				},
			}
			Expect(len(proc.jobSplitter(jobs, nil))).To(Equal(len(expectedSubJobs)))
			Expect(proc.jobSplitter(jobs, nil)).To(Equal(expectedSubJobs))
		})
		It("subJobSize: 1, i.e. dividing read jobs into batch of 1", func() {
			proc := NewHandle(config.Default, nil)
			proc.config.subJobSize = 1
			expectedSubJobs := []subJob{
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 1,
						},
					},
					hasMore: true,
				},
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 2,
						},
					},
					hasMore: true,
				},
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 3,
						},
					},
					hasMore: true,
				},
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 4,
						},
					},
					hasMore: true,
				},
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 5,
						},
					},
					hasMore: false,
				},
			}
			Expect(proc.jobSplitter(jobs, nil)).To(Equal(expectedSubJobs))
		})
		It("subJobSize: 2, i.e. dividing read jobs into batch of 2", func() {
			proc := NewHandle(config.Default, nil)
			proc.config.subJobSize = 2
			expectedSubJobs := []subJob{
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 1,
						},
						{
							JobID: 2,
						},
					},
					hasMore: true,
				},
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 3,
						},
						{
							JobID: 4,
						},
					},
					hasMore: true,
				},
				{
					subJobs: []*jobsdb.JobT{
						{
							JobID: 5,
						},
					},
					hasMore: false,
				},
			}
			Expect(proc.jobSplitter(jobs, nil)).To(Equal(expectedSubJobs))
		})
	})
})

var _ = Describe("TestConfigFilter", func() {
	Context("testing config filter", func() {
		It("success-full test", func() {
			intgConfig := backendconfig.DestinationT{
				ID:   "1",
				Name: "test",
				Config: map[string]interface{}{
					"config_key":   "config_value",
					"long_config1": "long_config1_value..................................",
					"long_config2": map[string]interface{}{"hello": "world"},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"configFilters": []interface{}{"long_config1", "long_config2"},
					},
				},
				Enabled:            true,
				IsProcessorEnabled: true,
			}
			expectedEvent := transformer.TransformerEvent{
				Message: types.SingularEventT{
					"MessageID": "messageId-1",
				},
				Destination: backendconfig.DestinationT{
					ID:   "1",
					Name: "test",
					Config: map[string]interface{}{
						"config_key": "config_value",
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Config: map[string]interface{}{
							"configFilters": []interface{}{"long_config1", "long_config2"},
						},
					},
					Enabled:            true,
					IsProcessorEnabled: true,
				},
			}
			event := transformer.TransformerEvent{
				Message: types.SingularEventT{
					"MessageID": "messageId-1",
				},
				Destination: intgConfig,
			}
			filterConfig(&event)
			Expect(event).To(Equal(expectedEvent))
		})

		It("success-full test with marshalling", func() {
			var intgConfig backendconfig.DestinationT
			var destDef backendconfig.DestinationDefinitionT
			intgConfigStr := `{
				"id": "1",
				"name": "test",
				"config": {
					"config_key":"config_value",
					"long_config1": "long_config1_value..................................",
					"long_config2": {"hello": "world"}
				},
				"enabled": true,
				"isProcessorEnabled": true
			}`
			destDefStr := `{
				"config": {
					"configFilters": ["long_config1", "long_config2"]
				}
			}`
			Expect(json.Unmarshal([]byte(intgConfigStr), &intgConfig)).To(BeNil())
			Expect(json.Unmarshal([]byte(destDefStr), &destDef)).To(BeNil())
			intgConfig.DestinationDefinition = destDef
			expectedEvent := transformer.TransformerEvent{
				Message: types.SingularEventT{
					"MessageID": "messageId-1",
				},
				Destination: backendconfig.DestinationT{
					ID:   "1",
					Name: "test",
					Config: map[string]interface{}{
						"config_key": "config_value",
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Config: map[string]interface{}{
							"configFilters": []interface{}{"long_config1", "long_config2"},
						},
					},
					Enabled:            true,
					IsProcessorEnabled: true,
				},
			}
			event := transformer.TransformerEvent{
				Message: types.SingularEventT{
					"MessageID": "messageId-1",
				},
				Destination: intgConfig,
			}
			filterConfig(&event)
			Expect(event).To(Equal(expectedEvent))
		})

		It("failure test", func() {
			intgConfig := backendconfig.DestinationT{
				ID:   "1",
				Name: "test",
				Config: map[string]interface{}{
					"config_key":   "config_value",
					"long_config1": "long_config1_value..................................",
					"long_config2": map[string]interface{}{"hello": "world"},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]interface{}{
						"configFilters": nil,
					},
				},
				Enabled:            true,
				IsProcessorEnabled: true,
			}
			expectedEvent := transformer.TransformerEvent{
				Message: types.SingularEventT{
					"MessageID": "messageId-1",
				},
				Destination: backendconfig.DestinationT{
					ID:   "1",
					Name: "test",
					Config: map[string]interface{}{
						"config_key":   "config_value",
						"long_config1": "long_config1_value..................................",
						"long_config2": map[string]interface{}{"hello": "world"},
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Config: map[string]interface{}{
							"configFilters": nil,
						},
					},
					Enabled:            true,
					IsProcessorEnabled: true,
				},
			}
			event := transformer.TransformerEvent{
				Message: types.SingularEventT{
					"MessageID": "messageId-1",
				},
				Destination: intgConfig,
			}
			filterConfig(&event)
			Expect(event).To(Equal(expectedEvent))
		})
	})
})

func Test_GetTimestampFromEvent(t *testing.T) {
	input := []struct {
		event             transformer.TransformerEvent
		timestamp         time.Time
		expectedTimeStamp time.Time
	}{
		{
			event: transformer.TransformerEvent{
				Message: types.SingularEventT{
					"timestamp": "2021-06-09T09:00:00.000Z",
				},
			},
			timestamp:         time.Now(),
			expectedTimeStamp: time.Date(2021, 6, 9, 9, 0, 0, 0, time.UTC),
		},
		{
			event: transformer.TransformerEvent{
				Message: types.SingularEventT{},
			},
			timestamp:         time.Now(),
			expectedTimeStamp: time.Now(),
		},
	}

	for _, in := range input {
		t.Run("GetTimestampFromEvent", func(t *testing.T) {
			got := getTimestampFromEvent(in.event.Message, "timestamp", in.timestamp)
			require.WithinDuration(t, in.expectedTimeStamp, got, 200*time.Millisecond, "expected timestamp %v, got %v", in.expectedTimeStamp, got)
		})
	}
}

func Test_EnhanceWithTimeFields(t *testing.T) {
	input := []struct {
		event                     transformer.TransformerEvent
		singularEvent             types.SingularEventT
		recievedAt                time.Time
		expectedSentAt            string
		expectedTimeStamp         string
		expectedOriginalTimestamp string
	}{
		{
			event: transformer.TransformerEvent{
				Message: types.SingularEventT{
					"timestamp": "2021-06-09T09:00:00.000Z",
				},
			},
			singularEvent: types.SingularEventT{
				"originalTimestamp": "2021-06-09T09:00:00.000Z",
				"sentAt":            "2021-06-09T09:00:00.000Z",
			},
			recievedAt:                time.Date(2021, 6, 9, 9, 0, 0, 0, time.UTC),
			expectedSentAt:            "2021-06-09T09:00:00.000Z",
			expectedTimeStamp:         "2021-06-09T09:00:00.000Z",
			expectedOriginalTimestamp: "2021-06-09T09:00:00.000Z",
		},
		{
			event: transformer.TransformerEvent{
				Message: types.SingularEventT{
					"timestamp": "2021-06-09T09:00:00.000Z",
				},
			},
			singularEvent:             types.SingularEventT{},
			recievedAt:                time.Date(2021, 6, 9, 9, 0, 0, 0, time.UTC),
			expectedSentAt:            "2021-06-09T09:00:00.000Z",
			expectedTimeStamp:         "2021-06-09T09:00:00.000Z",
			expectedOriginalTimestamp: "2021-06-09T09:00:00.000Z",
		},
		{
			event: transformer.TransformerEvent{
				Message: types.SingularEventT{},
			},
			singularEvent:             types.SingularEventT{},
			recievedAt:                time.Date(2021, 6, 9, 9, 0, 0, 0, time.UTC),
			expectedSentAt:            "2021-06-09T09:00:00.000Z",
			expectedTimeStamp:         "2021-06-09T09:00:00.000Z",
			expectedOriginalTimestamp: "2021-06-09T09:00:00.000Z",
		},
		{
			event: transformer.TransformerEvent{
				Message: types.SingularEventT{},
			},
			singularEvent: types.SingularEventT{
				"originalTimestamp": "2021-06-09T09:30:00.000Z",
				"sentAt":            "2021-06-09T09:15:00.000Z",
			},
			recievedAt:                time.Date(2021, 6, 9, 9, 45, 0, 0, time.UTC),
			expectedSentAt:            "2021-06-09T09:15:00.000Z",
			expectedTimeStamp:         "2021-06-09T10:00:00.000Z", // timestamp = receivedAt - (sentAt - originalTimestamp)
			expectedOriginalTimestamp: "2021-06-09T09:30:00.000Z",
		},
	}

	for _, in := range input {
		t.Run("EnhanceWithTimeFields", func(t *testing.T) {
			enhanceWithTimeFields(&in.event, in.singularEvent, in.recievedAt)
			require.Equal(t, in.expectedSentAt, in.event.Message["sentAt"])
			require.Equal(t, in.expectedTimeStamp, in.event.Message["timestamp"])
			require.Equal(t, in.expectedOriginalTimestamp, in.event.Message["originalTimestamp"])
		})
	}
}

func TestStoreMessageMerge(t *testing.T) {
	sm1 := &storeMessage{
		statusList:    []*jobsdb.JobStatusT{{JobID: 1}},
		destJobs:      []*jobsdb.JobT{{JobID: 1}},
		batchDestJobs: []*jobsdb.JobT{{JobID: 1}},
		procErrorJobsByDestID: map[string][]*jobsdb.JobT{
			"1": {{JobID: 1}},
		},
		procErrorJobs:       []*jobsdb.JobT{{JobID: 1}},
		routerDestIDs:       []string{"1"},
		reportMetrics:       []*types.PUReportedMetric{{}},
		sourceDupStats:      map[dupStatKey]int{{sourceID: "1"}: 1},
		dedupKeys:           map[string]struct{}{"1": {}},
		totalEvents:         1,
		trackedUsersReports: []*trackedusers.UsersReport{{WorkspaceID: sampleWorkspaceID}},
	}

	sm2 := &storeMessage{
		statusList:    []*jobsdb.JobStatusT{{JobID: 2}},
		destJobs:      []*jobsdb.JobT{{JobID: 2}},
		batchDestJobs: []*jobsdb.JobT{{JobID: 2}},
		procErrorJobsByDestID: map[string][]*jobsdb.JobT{
			"2": {{JobID: 2}},
		},
		procErrorJobs:       []*jobsdb.JobT{{JobID: 2}},
		routerDestIDs:       []string{"2"},
		reportMetrics:       []*types.PUReportedMetric{{}},
		sourceDupStats:      map[dupStatKey]int{{sourceID: "1"}: 2},
		dedupKeys:           map[string]struct{}{"2": {}},
		totalEvents:         1,
		trackedUsersReports: []*trackedusers.UsersReport{{WorkspaceID: sampleWorkspaceID}, {WorkspaceID: sampleWorkspaceID}},
	}

	sm3 := &storeMessage{
		[]*trackedusers.UsersReport{{WorkspaceID: sampleWorkspaceID}, {WorkspaceID: sampleWorkspaceID}},
		[]*jobsdb.JobStatusT{{JobID: 3}},
		[]*jobsdb.JobT{{JobID: 3}},
		[]*jobsdb.JobT{{JobID: 3}},
		[]*jobsdb.JobT{{JobID: 3}},
		map[string][]*jobsdb.JobT{
			"3": {{JobID: 3}},
		},
		[]*jobsdb.JobT{{JobID: 3}},
		[]string{"3"},
		[]*types.PUReportedMetric{{}},
		map[dupStatKey]int{{sourceID: "1"}: 3},
		map[string]struct{}{"3": {}},
		1,
		time.Time{},
		false,
		nil,
		map[string]stats.Tags{},
	}

	merged := storeMessage{
		procErrorJobsByDestID: map[string][]*jobsdb.JobT{},
		sourceDupStats:        map[dupStatKey]int{},
		dedupKeys:             map[string]struct{}{},
		start:                 time.UnixMicro(99999999),
	}

	merged.merge(sm1)
	require.Len(t, merged.statusList, 1, "status list should have 1 element")
	require.Len(t, merged.destJobs, 1, "dest jobs should have 1 element")
	require.Len(t, merged.batchDestJobs, 1, "batch dest jobs should have 1 element")
	require.Len(t, merged.procErrorJobsByDestID, 1, "proc error jobs by dest id should have 1 element")
	require.Len(t, merged.procErrorJobs, 1, "proc error jobs should have 1 element")
	require.Len(t, merged.routerDestIDs, 1, "router dest ids should have 1 element")
	require.Len(t, merged.reportMetrics, 1, "report metrics should have 1 element")
	require.Len(t, merged.sourceDupStats, 1, "source dup stats should have 1 element")
	require.Len(t, merged.dedupKeys, 1, "dedup keys should have 1 element")
	require.Len(t, merged.trackedUsersReports, 1, "trackedUsersReports should have 1 element")
	require.Equal(t, merged.totalEvents, 1, "total events should be 1")

	merged.merge(sm2)
	require.Len(t, merged.statusList, 2, "status list should have 2 elements")
	require.Len(t, merged.destJobs, 2, "dest jobs should have 2 elements")
	require.Len(t, merged.batchDestJobs, 2, "batch dest jobs should have 2 elements")
	require.Len(t, merged.procErrorJobsByDestID, 2, "proc error jobs by dest id should have 2 elements")
	require.Len(t, merged.procErrorJobs, 2, "proc error jobs should have 2 elements")
	require.Len(t, merged.routerDestIDs, 2, "router dest ids should have 2 elements")
	require.Len(t, merged.reportMetrics, 2, "report metrics should have 2 elements")
	require.Len(t, merged.sourceDupStats, 1, "source dup stats should have 1 element")
	require.EqualValues(t, merged.sourceDupStats[dupStatKey{sourceID: "1"}], 3)
	require.Len(t, merged.dedupKeys, 2, "dedup keys should have 2 elements")
	require.Equal(t, merged.totalEvents, 2, "total events should be 2")
	require.Len(t, merged.trackedUsersReports, 3, "trackedUsersReports should have 3 element")

	merged.merge(sm3)
	require.Equal(t, merged, storeMessage{
		trackedUsersReports: []*trackedusers.UsersReport{
			{WorkspaceID: sampleWorkspaceID},
			{WorkspaceID: sampleWorkspaceID},
			{WorkspaceID: sampleWorkspaceID},
			{WorkspaceID: sampleWorkspaceID},
			{WorkspaceID: sampleWorkspaceID},
		},
		statusList:    []*jobsdb.JobStatusT{{JobID: 1}, {JobID: 2}, {JobID: 3}},
		destJobs:      []*jobsdb.JobT{{JobID: 1}, {JobID: 2}, {JobID: 3}},
		batchDestJobs: []*jobsdb.JobT{{JobID: 1}, {JobID: 2}, {JobID: 3}},
		droppedJobs:   []*jobsdb.JobT{{JobID: 3}},
		procErrorJobsByDestID: map[string][]*jobsdb.JobT{
			"1": {{JobID: 1}},
			"2": {{JobID: 2}},
			"3": {{JobID: 3}},
		},
		procErrorJobs:  []*jobsdb.JobT{{JobID: 1}, {JobID: 2}, {JobID: 3}},
		routerDestIDs:  []string{"1", "2", "3"},
		reportMetrics:  []*types.PUReportedMetric{{}, {}, {}},
		sourceDupStats: map[dupStatKey]int{{sourceID: "1"}: 6},
		dedupKeys:      map[string]struct{}{"1": {}, "2": {}, "3": {}},
		totalEvents:    3,
		start:          time.UnixMicro(99999999),
	})
}
