package klaviyobulkupload_test

import (
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/klaviyobulkupload"
)

var (
	currentDir, _ = os.Getwd()
	destination   = &backendconfig.DestinationT{
		ID:   "1",
		Name: "KLAVIYO_BULK_UPLOAD",
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: "KLAVIYO_BULK_UPLOAD",
		},
		Config: map[string]interface{}{
			"privateApiKey": "1234",
		},
		Enabled:     true,
		WorkspaceID: "1",
	}
)

var _ = Describe("KlaviyoBulkUpload", func() {
	Describe(("NewManager function test"), func() {
		It("should return klaviyo bulk upload manager", func() {
			klaviyo, err := klaviyobulkupload.NewManager(destination)
			Expect(err).To(BeNil())
			Expect(klaviyo).NotTo(BeNil())
		})
	})
})
