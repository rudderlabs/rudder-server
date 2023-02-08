package misc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/google/uuid"
	"github.com/iancoleman/strcase"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
)

func initMisc() {
	config.Reset()
	logger.Reset()
	Init()
}

var _ = Describe("Misc", func() {
	initMisc()

	tmpDirPath, err := CreateTMPDIR()
	Expect(err).To(BeNil())

	Context("Remove Empty Folder Paths", func() {
		createFile := func(path, targetDir string) {
			dirPath := filepath.Dir(path)

			err := os.MkdirAll(dirPath, os.ModePerm)
			Expect(err).To(BeNil())

			var file *os.File
			file, err = os.Create(path)

			Expect(err).To(BeNil())
			Expect(FileExists(path)).To(BeTrue())
			Expect(FolderExists(targetDir)).To(BeTrue())

			defer file.Close()
		}
		onPostFileCreation := func(sourceFile, targetDir string) {
			RemoveFilePaths(sourceFile)

			empty, err := FolderExists(targetDir)
			Expect(err).To(BeNil())
			Expect(empty).To(BeTrue())
		}

		It("Rudder Async Destination Logs", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderAsyncDestinationLogs)
			uuid := uuid.New()

			// /tmp/rudder-async-destination-logs/SourceID.28da0aa8-f47d-422c-9cc8-f19d14ff158c.txt
			// /tmp/rudder-async-destination-logs/
			sourceFile := fmt.Sprintf("%v%v.txt",
				tmpDirPath+localTmpDirName,
				fmt.Sprintf("%v.%v", "SourceID", uuid.String()),
			)
			targetDir := fmt.Sprintf("%v",
				tmpDirPath+localTmpDirName,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)
		})

		It("Rudder Archives for Warehouse", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderArchives)
			pathPrefix := strcase.ToKebab("wh_staging_files")

			// /tmp/rudder-archives/wh-staging-files.SourceID.DestinationId.UploadId.1640923547.json.gz
			// /tmp/rudder-archives/
			sourceFile := fmt.Sprintf(`%v%v.%v.%v.%v.%v.json.gz`,
				tmpDirPath+localTmpDirName,
				pathPrefix,
				"SourceID",
				"DestinationId",
				"UploadId",
				timeutil.Now().Unix(),
			)
			targetDir := fmt.Sprintf(`%v`,
				tmpDirPath+localTmpDirName,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)
		})

		It("Rudder Archives for Archiver", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderArchives)
			pathPrefix := strcase.ToKebab("wh_staging_files")

			// /tmp/rudder-archives/wh-staging-files.MinID.MaxID.1640923547.json.gz
			// /tmp/rudder-archives/
			sourceFile := fmt.Sprintf(`%v%v.%v.%v.%v.json.gz`,
				tmpDirPath+localTmpDirName,
				pathPrefix,
				"MinID",
				"MaxID",
				timeutil.Now().Unix(),
			)
			targetDir := fmt.Sprintf(`%v`,
				tmpDirPath+localTmpDirName,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)
		})

		It("Rudder Warehouse Staging Uploads", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderWarehouseStagingUploads)
			uuid := uuid.New()

			// /tmp/rudder-warehouse-staging-uploads/1640923547.SourceId.abe7383c-1bb9-4b23-9f51-2e08280cbb71.json.gz
			// /tmp/rudder-warehouse-staging-uploads/
			sourceFile := fmt.Sprintf(`%v%v.json.gz`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf("%v.%v.%v", time.Now().Unix(), "SourceId", uuid.String()),
			)
			targetDir := fmt.Sprintf(`%v`,
				tmpDirPath+localTmpDirName,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)
		})

		It("Rudder Raw Data Destination Logs", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderRawDataDestinationLogs)
			uuid := uuid.New()

			// /tmp/rudder-raw-data-destination-logs/1640923547.SourceId.d481fbd8-df3a-4eed-b691-2381b260ca28.json.gz
			// /tmp/rudder-raw-data-destination-logs/
			sourceFile := fmt.Sprintf(`%v%v.json.gz`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf("%v.%v.%v", time.Now().Unix(), "SourceId", uuid.String()),
			)
			targetDir := fmt.Sprintf(`%v`,
				tmpDirPath+localTmpDirName,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)
		})

		It("Rudder Warehouse Load Upload Tmp", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderWarehouseLoadUploadsTmp)

			// /tmp/rudder-warehouse-load-uploads-tmp/DestinationName_DestinationId_1640923547/rudderstack-events/rudder-warehouse-staging-logs/20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-load-uploads-tmp/
			sourceFile := fmt.Sprintf(`%v%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s_%d/`, "DestinationName", "DestinationId", time.Now().Unix()),
				"rudderstack-events/rudder-warehouse-staging-logs/20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz",
			)
			targetDir := fmt.Sprintf(`%v`,
				tmpDirPath+localTmpDirName,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)

			// /tmp/rudder-warehouse-load-uploads-tmp/DestinationName_DestinationId_1640923547/rudder-warehouse-staging-logs/20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-load-uploads-tmp/
			sourceFile = fmt.Sprintf(`%v%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s_%d/`, "DestinationName", "DestinationId", time.Now().Unix()),
				"rudder-warehouse-staging-logs/20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz",
			)
			targetDir = fmt.Sprintf(`%v`,
				tmpDirPath+localTmpDirName,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)

			// /tmp/rudder-warehouse-load-uploads-tmp/DestinationName_DestinationId_1640923547/20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-load-uploads-tmp/
			sourceFile = fmt.Sprintf(`%v%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s_%d/`, "DestinationName", "DestinationId", time.Now().Unix()),
				"20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz",
			)
			targetDir = fmt.Sprintf(`%v`,
				tmpDirPath+localTmpDirName,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)

			// /tmp/rudder-warehouse-load-uploads-tmp/DestinationName_DestinationId_1640923547/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-load-uploads-tmp/
			sourceFile = fmt.Sprintf(`%v%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s_%d/`, "DestinationName", "DestinationId", time.Now().Unix()),
				"2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz",
			)
			targetDir = fmt.Sprintf(`%v`,
				tmpDirPath+localTmpDirName,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)

			// /tmp/rudder-warehouse-load-uploads-tmp/DestinationName_DestinationId_1640923547/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-load-uploads-tmp/
			sourceFile = fmt.Sprintf(`%v%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s_%d/`, "DestinationName", "DestinationId", time.Now().Unix()),
				"1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz",
			)
			targetDir = fmt.Sprintf(`%v`,
				tmpDirPath+localTmpDirName,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)
		})

		It("Rudder Identity Merge Rules Tmp", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderIdentityMergeRulesTmp)
			uuid := uuid.New()

			// /tmp/rudder-identity-merge-rules-tmp/DestinationName_DestinationId/UploadId/71a855c2-0535-43d1-9f00-556ecc971cc7.csv.gz
			// /tmp/rudder-identity-merge-rules-tmp/DestinationName_DestinationId/
			sourceFile := fmt.Sprintf(`%v%v%v.csv.gz`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/%v/`, "DestinationName", "DestinationId", "UploadId"),
				uuid.String(),
			)

			targetDir := fmt.Sprintf(`%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationName", "DestinationId"),
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)

			// /tmp/rudder-identity-merge-rules-tmp/DestinationName_DestinationId/71a855c2-0535-43d1-9f00-556ecc971cc7.csv.gz
			// /tmp/rudder-identity-merge-rules-tmp/DestinationName_DestinationId/
			sourceFile = fmt.Sprintf(`%v%v%v.csv.gz`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationName", "DestinationId"),
				uuid.String(),
			)

			targetDir = fmt.Sprintf(`%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationName", "DestinationId"),
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)
		})

		It("Rudder Identity Mappings Tmp", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderIdentityMappingsTmp)
			uuid := uuid.New()

			// /tmp/rudder-identity-mappings-tmp/DestinationName_DestinationId/UploadId/b012ad98-1cde-4d27-a415-cdb84021180c.csv.gz
			// /tmp/rudder-identity-mappings-tmp/DestinationName_DestinationId/
			sourceFile := fmt.Sprintf(`%v%v%v.csv.gz`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/%v/`, "DestinationName", "DestinationId", "UploadId"),
				uuid.String(),
			)

			targetDir := fmt.Sprintf(`%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationName", "DestinationId"),
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)

			// /tmp/rudder-identity-mappings-tmp/DestinationName_DestinationId/b012ad98-1cde-4d27-a415-cdb84021180c.csv.gz
			// /tmp/rudder-identity-mappings-tmp/DestinationName_DestinationId/
			sourceFile = fmt.Sprintf(`%v%v%v.csv.gz`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationName", "DestinationId"),
				uuid.String(),
			)

			targetDir = fmt.Sprintf(`%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationName", "DestinationId"),
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)
		})

		It("Rudder Redshift Manifests", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderRedshiftManifests)
			uuid := uuid.New()

			// /tmp/rudder-redshift-manifests/688d39e8-90a7-4419-986d-0c14af3760e9
			// /tmp/rudder-redshift-manifests/
			sourceFile := fmt.Sprintf(`%v%v`,
				tmpDirPath+localTmpDirName,
				uuid.String(),
			)

			targetDir := fmt.Sprintf(`%v`,
				tmpDirPath+localTmpDirName,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)
		})

		It("Rudder Warehouse Json Uploads Tmp", func() {
			localTmpDirName := fmt.Sprintf(`/%s/_0/`, RudderWarehouseJsonUploadsTmp)

			// /tmp/rudder-warehouse-json-uploads-tmp/_0/DestinationType_DestinationID/rudderstack-events/rudder-warehouse-staging-logs/20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-json-uploads-tmp/_0/DestinationType_DestinationID/
			sourceFile := fmt.Sprintf(`%v%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationType", "DestinationID"),
				"rudderstack-events/rudder-warehouse-staging-logs/20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz",
			)

			targetDir := fmt.Sprintf(`%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationType", "DestinationID"),
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)

			// /tmp/rudder-warehouse-json-uploads-tmp/_0/DestinationType_DestinationID/rudder-warehouse-staging-logs/20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-json-uploads-tmp/_0/DestinationType_DestinationID/
			sourceFile = fmt.Sprintf(`%v%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationType", "DestinationID"),
				"rudder-warehouse-staging-logs/20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz",
			)

			targetDir = fmt.Sprintf(`%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationType", "DestinationID"),
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)

			// /tmp/rudder-warehouse-json-uploads-tmp/_0/DestinationType_DestinationID/20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-json-uploads-tmp/_0/DestinationType_DestinationID/
			sourceFile = fmt.Sprintf(`%v%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationType", "DestinationID"),
				"20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz",
			)

			targetDir = fmt.Sprintf(`%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationType", "DestinationID"),
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)

			// /tmp/rudder-warehouse-json-uploads-tmp/_0/DestinationType_DestinationID/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-json-uploads-tmp/_0/DestinationType_DestinationID/
			sourceFile = fmt.Sprintf(`%v%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationType", "DestinationID"),
				"2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz",
			)

			targetDir = fmt.Sprintf(`%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationType", "DestinationID"),
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)

			// /tmp/rudder-warehouse-json-uploads-tmp/_0/DestinationType_DestinationID/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-json-uploads-tmp/_0/DestinationType_DestinationID/
			sourceFile = fmt.Sprintf(`%v%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationType", "DestinationID"),
				"1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz",
			)

			targetDir = fmt.Sprintf(`%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/`, "DestinationType", "DestinationID"),
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)
		})

		It("Rudder Connection Test", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderTestPayload)

			// /tmp/rudder-test-payload/DestinationID.638853fe-2d60-46d3-86bb-8e6d728ecb33.1640981723.csv.gz
			// /tmp/rudder-test-payload/
			sourceFile := fmt.Sprintf(`%v%v.%v.%v.csv.gz`,
				tmpDirPath+localTmpDirName,
				"DestinationID",
				uuid.New(),
				time.Now().Unix(),
			)

			targetDir := fmt.Sprintf(`%v`,
				tmpDirPath+localTmpDirName,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)
		})

		It("Random Test", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, "random")

			// /tmp/random/Folder1/Folder2/Folder3/Folder4/Folder5/Folder6/Folder7/Folder8/SourceID.DestinationID.638853fe-2d60-46d3-86bb-8e6d728ecb33.1640981723.csv.gz
			// /tmp/
			sourceFile := fmt.Sprintf(`%v/%v/%v/%v/%v/%v/%v/%v/%v/%v.%v.%v.csv.gz`,
				tmpDirPath+localTmpDirName,
				"Folder1",
				"Folder2",
				"Folder3",
				"Folder4",
				"Folder5",
				"Folder6",
				"Folder7",
				"Folder8",
				"SourceID",
				"DestinationID",
				uuid.New(),
			)

			targetDir := fmt.Sprintf(`%v`,
				tmpDirPath,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)

			// /tmp/random/SourceID.DestinationID.638853fe-2d60-46d3-86bb-8e6d728ecb33.1640981723.csv.gz
			// /tmp/
			sourceFile = fmt.Sprintf(`%v%v.%v.%v.csv.gz`,
				tmpDirPath+localTmpDirName,
				"SourceID",
				"DestinationID",
				uuid.New(),
			)

			targetDir = fmt.Sprintf(`%v`,
				tmpDirPath,
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)

			// /tmp/random/
			// /tmp/
			sourceFile = fmt.Sprintf(`%v`,
				tmpDirPath+localTmpDirName,
			)

			targetDir = fmt.Sprintf(`%v`,
				tmpDirPath,
			)

			onPostFileCreation(sourceFile, targetDir)

			// /tmp/
			// /tmp/
			sourceFile = fmt.Sprintf(`%v`,
				tmpDirPath,
			)

			targetDir = fmt.Sprintf(`%v`,
				tmpDirPath,
			)

			onPostFileCreation(sourceFile, targetDir)
		})
	})

	_ = DescribeTable("Unique tests",
		func(input, expected []string) {
			actual := Unique(input)
			Expect(actual).To(Equal(expected))
		},
		Entry("Unique Test 1 : ", []string{"a", "b", "a", "c", "d", "d"}, []string{"a", "b", "c", "d"}),
		Entry("Unique Test 2 : ", []string{"a", "b", "c"}, []string{"a", "b", "c"}),
	)
})

func TestContains(t *testing.T) {
	t.Run("strings", func(t *testing.T) {
		list := []string{"a", "b", "c"}

		for _, item := range list {
			require.True(t, Contains(list, item))
		}

		require.False(t, Contains(list, "0"))
	})

	t.Run("int", func(t *testing.T) {
		list := []int{1, 2, 3}

		for _, item := range list {
			require.True(t, Contains(list, item))
		}

		require.False(t, Contains(list, -1))
	})
}

func TestHasAWSRoleARNInConfig(t *testing.T) {
	t.Run("Config has valid IAM Role ARN", func(t *testing.T) {
		configMap := map[string]interface{}{
			"iamRoleARN": "someRole",
		}
		require.True(t, HasAWSRoleARNInConfig(configMap))
	})

	t.Run("Config has empty IAM Role ARN", func(t *testing.T) {
		configMap := map[string]interface{}{
			"iamRoleARN": "",
		}
		require.False(t, HasAWSRoleARNInConfig(configMap))
	})

	t.Run("Config has no IAM Role ARN", func(t *testing.T) {
		configMap := map[string]interface{}{}
		require.False(t, HasAWSRoleARNInConfig(configMap))
	})
}

func TestReplaceMultiRegex(t *testing.T) {
	inputs := []struct {
		expression string
		expList    map[string]string
		expected   string
	}{
		{
			expression: `CREDENTIALS = (AWS_KEY_ID='RS8trQDxFH3dbzPL' AWS_SECRET_KEY='dWcwQblVpEZgELvK' AWS_TOKEN='BxyNrYig8z5yXPpiEMK8niux')`,
			expList: map[string]string{
				"AWS_KEY_ID='[^']*'":     "AWS_KEY_ID='***'",
				"AWS_SECRET_KEY='[^']*'": "AWS_SECRET_KEY='***'",
				"AWS_TOKEN='[^']*'":      "AWS_TOKEN='***'",
			},
			expected: `CREDENTIALS = (AWS_KEY_ID='***' AWS_SECRET_KEY='***' AWS_TOKEN='***')`,
		},
		{
			expression: `STORAGE_INTEGRATION = 'VAVDUDJPxa2w8vk1EY6BA4on'`,
			expList: map[string]string{
				"STORAGE_INTEGRATION = '[^']*'": "STORAGE_INTEGRATION = '***'",
			},
			expected: `STORAGE_INTEGRATION = '***'`,
		},
		{
			expression: `ACCESS_KEY_ID 'RS8trQDxFH3dbzPL' SECRET_ACCESS_KEY 'dWcwQblVpEZgELvK' SESSION_TOKEN 'BxyNrYig8z5yXPpiEMK8niux'`,
			expList: map[string]string{
				"ACCESS_KEY_ID '[^']*'":     "ACCESS_KEY_ID '***'",
				"SECRET_ACCESS_KEY '[^']*'": "SECRET_ACCESS_KEY '***'",
				"SESSION_TOKEN '[^']*'":     "SESSION_TOKEN '***'",
			},
			expected: `ACCESS_KEY_ID '***' SECRET_ACCESS_KEY '***' SESSION_TOKEN '***'`,
		},
		{
			expression: `CREDENTIALS ( 'awsKeyId' = 'RS8trQDxFH3dbzPL', 'awsSecretKey' = 'dWcwQblVpEZgELvK', 'awsSessionToken' = 'BxyNrYig8z5yXPpiEMK8niux' )`,
			expList: map[string]string{
				"'awsKeyId' = '[^']*'":        "'awsKeyId' = '***'",
				"'awsSecretKey' = '[^']*'":    "'awsSecretKey' = '***'",
				"'awsSessionToken' = '[^']*'": "'awsSessionToken' = '***'",
			},
			expected: `CREDENTIALS ( 'awsKeyId' = '***', 'awsSecretKey' = '***', 'awsSessionToken' = '***' )`,
		},
	}
	for _, input := range inputs {
		got, err := ReplaceMultiRegex(input.expression, input.expList)
		require.NoError(t, err)
		require.Equal(t, got, input.expected)
	}
}

func TestGetObjectStorageConfig(t *testing.T) {
	sampleWorkspaceID := "someWorkspaceID"
	sampleAccessKeyID := "someAccessKeyID"
	sampleAccessKey := "someAccessKey"
	t.Setenv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY_ID", sampleAccessKeyID)
	t.Setenv("RUDDER_AWS_S3_COPY_USER_ACCESS_KEY", sampleAccessKey)
	t.Run("S3 without AccessKeys", func(t *testing.T) {
		config := GetObjectStorageConfig(ObjectStorageOptsT{
			Provider:    "S3",
			Config:      map[string]interface{}{},
			WorkspaceID: sampleWorkspaceID,
		})
		require.NotNil(t, config)
		require.Equal(t, sampleWorkspaceID, config["externalID"])
		require.Equal(t, sampleAccessKeyID, config["accessKeyID"])
		require.Equal(t, sampleAccessKey, config["accessKey"])
	})

	t.Run("S3 with AccessKeys", func(t *testing.T) {
		config := GetObjectStorageConfig(ObjectStorageOptsT{
			Provider: "S3",
			Config: map[string]interface{}{
				"accessKeyID": "someOtherAccessKeyID",
				"accessKey":   "someOtherAccessKey",
			},
			WorkspaceID: sampleWorkspaceID,
		})
		require.NotNil(t, config)
		require.Equal(t, sampleWorkspaceID, config["externalID"])
		require.Equal(t, "someOtherAccessKeyID", config["accessKeyID"])
		require.Equal(t, "someOtherAccessKey", config["accessKey"])
	})
}

// FolderExists Check if folder exists at particular path
func FolderExists(path string) (bool, error) {
	fileInfo, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return fileInfo.IsDir(), nil
}

// FileExists Check if file exists at particular path
func FileExists(path string) (bool, error) {
	fileInfo, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return !fileInfo.IsDir(), nil
}

func TestMapLookup(t *testing.T) {
	m := map[string]interface{}{
		"foo": "bar",
		"baz": "qux",
	}
	require.Nil(t, MapLookup(m, "foo", "baz"))

	m = map[string]interface{}{
		"foo": map[string]interface{}{
			"baz": "qux",
		},
	}
	require.Equal(t, "qux", MapLookup(m, "foo", "baz"))

	m = map[string]interface{}{
		"hello": map[string]interface{}{
			"foo": "bar",
		},
	}
	require.Nil(t, MapLookup(m, "foo"))
}
