package misc

import (
	"errors"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/iancoleman/strcase"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"os"
	"path/filepath"
	"time"
)

func initMisc() {
	config.Load()
	logger.Init()
	Init()
}

var _ = Describe("Misc", func() {
	initMisc()

	tmpDirPath, err := CreateTMPDIR()
	Expect(err).To(BeNil())

	Context("Remove Empty Folder Paths", func() {
		createFile := func(path string, targetDir string) {
			dirPath := filepath.Dir(path)

			err := os.MkdirAll(dirPath, os.ModePerm)
			Expect(err).To(BeNil())

			var file *os.File
			file, err = os.Create(path)
			fmt.Println(path)
			fmt.Println(targetDir)

			Expect(err).To(BeNil())
			Expect(FileExists(path)).To(BeTrue())
			Expect(FolderExists(targetDir)).To(BeTrue())

			defer file.Close()
		}
		onPostFileCreation := func(sourceFile string, targetDir string) {
			RemoveFilePaths(sourceFile)

			empty, err := FolderExists(targetDir)
			Expect(err).To(BeNil())
			Expect(empty).To(BeTrue())
		}

		It("Rudder Async Destination Logs", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderAsyncDestinationLogs)
			uuid := uuid.Must(uuid.NewV4())

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
			uuid := uuid.Must(uuid.NewV4())

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
			uuid := uuid.Must(uuid.NewV4())

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
			uuid := uuid.Must(uuid.NewV4())

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
			uuid := uuid.Must(uuid.NewV4())

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
			uuid := uuid.Must(uuid.NewV4())

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
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderWarehouseJsonUploadsTmp)

			// /tmp/rudder-warehouse-json-uploads-tmp/DestinationType_DestinationID/rudderstack-events/rudder-warehouse-staging-logs/20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-json-uploads-tmp/DestinationType_DestinationID/
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

			// /tmp/rudder-warehouse-json-uploads-tmp/DestinationType_DestinationID/rudder-warehouse-staging-logs/20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-json-uploads-tmp/DestinationType_DestinationID/
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

			// /tmp/rudder-warehouse-json-uploads-tmp/DestinationType_DestinationID/20jCt8zdozZ26iBb7xXhAas0kCs/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-json-uploads-tmp/DestinationType_DestinationID/
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

			// /tmp/rudder-warehouse-json-uploads-tmp/DestinationType_DestinationID/2021-11-11/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-json-uploads-tmp/DestinationType_DestinationID/
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

			// /tmp/rudder-warehouse-json-uploads-tmp/DestinationType_DestinationID/1636604686.20jCt8zdozZ26iBb7xXhAas0kCs.280588a5-aa54-4ad3-921b-28c09969e78a.json.gz
			// /tmp/rudder-warehouse-json-uploads-tmp/DestinationType_DestinationID/
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
				uuid.Must(uuid.NewV4()),
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
				uuid.Must(uuid.NewV4()),
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
				uuid.Must(uuid.NewV4()),
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
})

// FolderExists Check if folder exists at particular path
func FolderExists(path string) (exists bool, err error) {
	fileInfo, err := os.Stat(path)
	if err == nil {
		exists = fileInfo.IsDir()
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		exists = false
		err = nil
		return
	}
	return
}

// FileExists Check if file exists at particular path
func FileExists(path string) (exists bool, err error) {
	fileInfo, err := os.Stat(path)
	if err == nil {
		exists = !fileInfo.IsDir()
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		exists = false
		err = nil
		return
	}
	return
}
