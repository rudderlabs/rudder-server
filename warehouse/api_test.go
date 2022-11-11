package warehouse

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/services/filemanager"
)

var _ = Describe("warehouse_api", func() {
	Context("Testing objectStorageValidation ", func() {
		It("Should fallback to backup credentials when fields missing(as of now backup only supported for s3)", func() {
			fm := &filemanager.SettingsT{
				Provider: "AZURE_BLOB",
				Config:   map[string]interface{}{"containerName": "containerName1", "prefix": "prefix1", "accountKey": "accountKey1"},
			}
			overrideWithEnv(fm)
			Expect(fm.Config["accountName"]).To(BeNil())
			fm.Provider = "S3"
			fm.Config = map[string]interface{}{"bucketName": "bucket1", "prefix": "prefix1", "accessKeyID": "KeyID1"}
			overrideWithEnv(fm)
			Expect(fm.Config["accessKey"]).ToNot(BeNil())
		})
		It("Should set value for key when key not present", func() {
			jsonMap := make(map[string]interface{})
			jsonMap["config"] = "{}"
			typeValue := "GCS"
			configValue := "{\"bucketName\":\"temp\"}"
			ifNotExistThenSet("type", typeValue, jsonMap)
			ifNotExistThenSet("config", configValue, jsonMap)
			Expect(jsonMap["type"]).To(BeIdenticalTo(typeValue))
			Expect(jsonMap["config"]).To(BeIdenticalTo("{}"))
		})
	})
})
