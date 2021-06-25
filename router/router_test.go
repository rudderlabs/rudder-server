package router

import (
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/config/backend-config"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

const (
	WriteKeyEnabled           = "enabled-write-key"
	WriteKeyDisabled          = "disabled-write-key"
	WriteKeyInvalid           = "invalid-write-key"
	WriteKeyEmpty             = ""
	SourceIDEnabled           = "enabled-source"
	SourceIDDisabled          = "disabled-source"
	TestRemoteAddressWithPort = "test.com:80"
	TestRemoteAddress         = "test.com"
	GADestinationID           = "gad1"
)

var testTimeout = 5 * time.Second

// This configuration is assumed by all router tests and, is returned on Subscribe of mocked backend config
var sampleBackendConfig = backendconfig.ConfigT{
	Sources: []backendconfig.SourceT{
		{
			ID:       SourceIDDisabled,
			WriteKey: WriteKeyDisabled,
			Enabled:  false,
		},
		{
			ID:       SourceIDEnabled,
			WriteKey: WriteKeyEnabled,
			Enabled:  true,
		},
	},
}

var destinationDefinition = backendconfig.DestinationDefinitionT{ID: GADestinationID, Name: "GA", DisplayName: "Google Analytics", Config: nil, ResponseRules: nil}

type context struct {
	asyncHelper testutils.AsyncTestHelper

	mockCtrl          *gomock.Controller
	mockRouterJobsDB  *mocksJobsDB.MockJobsDB
	mockProcErrorsDB  *mocksJobsDB.MockJobsDB
	mockBackendConfig *mocksBackendConfig.MockBackendConfig
}

// Initiaze mocks and common expectations
func (c *context) Setup() {
	c.asyncHelper.Setup()
	c.mockCtrl = gomock.NewController(GinkgoT())
	c.mockRouterJobsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockProcErrorsDB = mocksJobsDB.NewMockJobsDB(c.mockCtrl)
	c.mockBackendConfig = mocksBackendConfig.NewMockBackendConfig(c.mockCtrl)

	// During Setup, router subscribes to backend config
	c.mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).
		Do(func(channel chan utils.DataEvent, topic backendconfig.Topic) {
			// on Subscribe, emulate a backend configuration event
			go func() { channel <- utils.DataEvent{Data: sampleBackendConfig, Topic: string(topic)} }()
		}).
		Do(c.asyncHelper.ExpectAndNotifyCallbackWithName("process_config")).
		Return().Times(1)
}

func (c *context) Finish() {
	c.asyncHelper.WaitWithTimeout(testTimeout)
	c.mockCtrl.Finish()
}

var _ = Describe("Router", func() {
	var c *context

	BeforeEach(func() {
		c = &context{}
		c.Setup()

		// setup static requirements of dependencies
		stats.Setup()
		RoutersManagerSetup()
	})

	AfterEach(func() {
		c.Finish()
	})

	Context("Initialization", func() {

		It("should initialize and recover after crash", func() {
			router := &HandleT{}

			c.mockRouterJobsDB.EXPECT().DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: []string{destinationDefinition.Name}, Count: -1}).Times(1)

			router.Setup(c.mockBackendConfig, c.mockRouterJobsDB, c.mockProcErrorsDB, destinationDefinition, nil)
		})
	})
})
