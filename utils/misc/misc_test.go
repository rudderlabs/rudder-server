package misc

import (
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

			Expect(err).To(BeNil())
			Expect(FileExists(path)).To(BeTrue())
			Expect(FolderExists(targetDir)).To(BeTrue())

			defer file.Close()
		}
		onPostFileCreation := func(sourceFile string, targetDir string) {
			RemoveFilePaths(sourceFile)

			empty, err := IsDirectoryEmpty(targetDir)
			Expect(err).To(BeNil())
			Expect(empty).To(BeTrue())
		}

		It("Rudder Async Destination Logs", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderAsyncDestinationLogs)
			uuid := uuid.Must(uuid.NewV4())

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
		})

		It("Rudder Identity Merge Rules Tmp", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderIdentityMergeRulesTmp)
			uuid := uuid.Must(uuid.NewV4())

			sourceFile := fmt.Sprintf(`%v%v%v.csv.gz`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/%v/`, "DestinationName", "DestinationId", "UploadId"),
				uuid.String(),
			)

			targetDir := fmt.Sprintf(`%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/%v/`, "DestinationName", "DestinationId", "UploadId"),
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)
		})

		It("Rudder Identity Mappings Tmp", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderIdentityMappingsTmp)
			uuid := uuid.Must(uuid.NewV4())

			sourceFile := fmt.Sprintf(`%v%v%v.csv.gz`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/%v/`, "DestinationName", "DestinationId", "UploadId"),
				uuid.String(),
			)

			targetDir := fmt.Sprintf(`%v%v`,
				tmpDirPath+localTmpDirName,
				fmt.Sprintf(`%s_%s/%v/`, "DestinationName", "DestinationId", "UploadId"),
			)

			createFile(sourceFile, targetDir)
			onPostFileCreation(sourceFile, targetDir)
		})

		It("Rudder Redshift Manifests", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderRedshiftManifests)
			uuid := uuid.Must(uuid.NewV4())

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
		})

		It("Rudder Connection Test", func() {
			localTmpDirName := fmt.Sprintf(`/%s/`, RudderTestPayload)

			sourceFile := fmt.Sprintf(`%v/%v.%v.%v.csv.gz`,
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
	})
})
