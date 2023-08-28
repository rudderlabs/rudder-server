package eloqua_test

import (
	"sync"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"

	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_bulkservice "github.com/rudderlabs/rudder-server/mocks/router/eloqua"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	eloqua "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/eloqua"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	once        sync.Once
	destination = backendconfig.DestinationT{
		Name: "eloqua",
		Config: map[string]interface{}{
			"companyName":     "company_name",
			"password":        "password",
			"userName":        "user_name",
			"rudderAccountId": "rudder_account_id",
		},
		WorkspaceID: "workspace_id",
	}
)

func initEloqua() {
	once.Do(func() {
		logger.Reset()
		misc.Init()
	})
}

var _ = Describe("Eloqua test", func() {
	Context("When uploading the file", func() {
		BeforeEach(func() {
			config.Reset()
			config.Set("BatchRouter.BING_ADS.MaxUploadLimit", 1*bytesize.KB)
		})

		AfterEach(func() {
			config.Reset()
		})
		It("TestEloquaUploadWrongFilepath", func() {
			initEloqua()
			ctrl := gomock.NewController(GinkgoT())
			eloquaService := mock_bulkservice.NewMockEloqua(ctrl)
			bulkUploader := eloqua.NewEloquaBulkUploader("Eloqua", "", "", eloquaService)
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        "",
				Destination:     &destination,
				Manager:         bulkUploader,
			}
			expected := common.AsyncUploadOutput{
				FailedReason:        "got error while opening the file. open : no such file or directory",
				ImportingJobIDs:     nil,
				FailedJobIDs:        []int64{1, 2, 3, 4},
				ImportingParameters: nil,
				ImportingCount:      0,
				FailedCount:         4,
			}
			received := bulkUploader.Upload(&asyncDestination)
			Expect(received).To(Equal(expected))
		})
	})
})
