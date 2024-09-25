package sftp

import (
	stdjson "encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/sftp/mock_sftp"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	once         sync.Once
	destinations = []backendconfig.DestinationT{
		{
			ID:   "destination_id_1",
			Name: "SFTP",
			Config: map[string]interface{}{
				"host":       "host",
				"port":       "22",
				"authMethod": "passwordAuth",
				"username":   "username",
				"password":   "password",
				"fileFormat": "csv",
				"filePath":   "/tmp/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
			},
			WorkspaceID: "workspace_id",
		},
		{
			ID:   "destination_id_2",
			Name: "SFTP",
			Config: map[string]interface{}{
				"host":       "host",
				"port":       "22",
				"authMethod": "passwordAuth",
				"username":   "username",
				"password":   "",
				"fileFormat": "csv",
				"filePath":   "/tmp/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
			},
			WorkspaceID: "workspace_id",
		},
		{
			ID:   "destination_id_3",
			Name: "SFTP",
			Config: map[string]interface{}{
				"host":       "host",
				"port":       "22",
				"authMethod": "passwordAuth",
				"username":   "username",
				"password":   "password",
				"fileFormat": "txt",
				"filePath":   "/tmp/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
			},
			WorkspaceID: "workspace_id",
		},
		{
			ID:   "destination_id_4",
			Name: "SFTP",
			Config: map[string]interface{}{
				"host":       "host",
				"port":       "",
				"authMethod": "passwordAuth",
				"username":   "username",
				"password":   "password",
				"fileFormat": "csv",
				"filePath":   "/tmp/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
			},
			WorkspaceID: "workspace_id",
		},
		{
			ID:   "destination_id_5",
			Name: "SFTP",
			Config: map[string]interface{}{
				"host":       "host",
				"port":       "0",
				"authMethod": "passwordAuth",
				"username":   "username",
				"password":   "password",
				"fileFormat": "csv",
				"filePath":   "/tmp/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
			},
			WorkspaceID: "workspace_id",
		},
		{
			ID:   "destination_id_6",
			Name: "SFTP",
			Config: map[string]interface{}{
				"host":       "host",
				"port":       "22",
				"authMethod": "keyAuth",
				"username":   "username",
				"privateKey": "privateKey",
				"fileFormat": "csv",
				"filePath":   "/tmp/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
			},
			WorkspaceID: "workspace_id",
		},
	}

	currentDir, _ = os.Getwd()
)

func initSFTP() {
	once.Do(func() {
		logger.Reset()
		misc.Init()
	})
}

var _ = Describe("SFTP test", func() {
	Context("Upload", func() {
		BeforeEach(func() {
			config.Reset()
		})

		AfterEach(func() {
			config.Reset()
		})

		It("TestUploadWrongFilepath", func() {
			initSFTP()
			ctrl := gomock.NewController(GinkgoT())
			fileManager := mock_sftp.NewMockFileManager(ctrl)
			defaultManager := newDefaultManager(logger.NOP, stats.NOP, fileManager)
			manager := common.SimpleAsyncDestinationManager{UploaderAndTransformer: defaultManager}
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1014, 1015, 1016, 1017},
				FileName:        "dir/someWrongFilePath",
				Destination:     &destinations[0],
				Manager:         manager,
			}
			expected := common.AsyncUploadOutput{
				DestinationID: "destination_id_1",
				AbortReason:   "error generating temporary file: opening file: open dir/someWrongFilePath: no such file or directory",
				AbortJobIDs:   []int64{1014, 1015, 1016, 1017},
				AbortCount:    4,
			}
			received := manager.Upload(&asyncDestination)
			Expect(received).To(Equal(expected))
		})

		It("TestErrorUploadingFileOnRemoteServer", func() {
			initSFTP()
			ctrl := gomock.NewController(GinkgoT())
			fileManager := mock_sftp.NewMockFileManager(ctrl)
			defaultManager := newDefaultManager(logger.NOP, stats.NOP, fileManager)
			fileManager.EXPECT().Upload(gomock.Any(), gomock.Any()).Return(fmt.Errorf("root directory does not exists"))
			manager := common.SimpleAsyncDestinationManager{UploaderAndTransformer: defaultManager}
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1014, 1015, 1016, 1017},
				FileName:        filepath.Join(currentDir, "testdata/uploadDataRecord.txt"),
				Destination:     &destinations[0],
				Manager:         manager,
			}
			expected := common.AsyncUploadOutput{
				DestinationID: "destination_id_1",
				AbortReason:   "error uploading file to destination: root directory does not exists",
				AbortJobIDs:   []int64{1014, 1015, 1016, 1017},
				AbortCount:    4,
			}
			received := manager.Upload(&asyncDestination)
			Expect(received).To(Equal(expected))
		})

		It("TestSuccessfulUpload", func() {
			initSFTP()
			ctrl := gomock.NewController(GinkgoT())
			fileManager := mock_sftp.NewMockFileManager(ctrl)
			defaultManager := newDefaultManager(logger.NOP, stats.NOP, fileManager)
			now := time.Now()
			filePath := fmt.Sprintf("/tmp/testDir1/destination_id_1_someJobRunId_1/file_%d_%02d_%02d_%d_1.csv", now.Year(), now.Month(), now.Day(), now.Unix())
			fileManager.EXPECT().Upload(gomock.Any(), filePath).Return(nil)
			manager := common.SimpleAsyncDestinationManager{UploaderAndTransformer: defaultManager}
			payload := make(map[int64]stdjson.RawMessage)
			id := int64(1014)
			sampleData := map[string]interface{}{
				"source_job_run_id": "someJobRunId_1",
			}
			rawMessage, _ := json.Marshal(sampleData)
			payload[id] = rawMessage

			id = int64(1016)
			payload[id] = rawMessage

			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs:       []int64{1014, 1015, 1016, 1017},
				FileName:              filePath,
				Destination:           &destinations[0],
				Manager:               manager,
				OriginalJobParameters: payload,
				CreatedAt:             now,
				PartFileNumber:        1,
				SourceJobRunID:        "someJobRunId_1",
			}
			expected := common.AsyncUploadOutput{
				DestinationID:   "destination_id_1",
				SucceededJobIDs: []int64{1014, 1015, 1016, 1017},
				SuccessResponse: "File Upload Success",
			}

			err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm)
			Expect(err).ShouldNot(HaveOccurred(), "Failed to create temporary directory")

			tempFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
			Expect(err).ShouldNot(HaveOccurred(), "Failed to create temporary file")
			defer os.RemoveAll("/tmp/testDir1")

			data := []byte(`{"message":{"action":"insert","fields":{"email":"john@email.com","name":"john"},"messageId":"b6543bc0-f280-4642-8176-4b8d5a1b8fb6","originalTimestamp":"2024-04-20T22:29:37.731+05:30","receivedAt":"2024-04-20T22:29:36.843+05:30","request_ip":"[::1]","rudderId":"853ae90f-0351-424b-973e-a615e6487517","sentAt":"2024-04-20T22:29:37.731+05:30","timestamp":"2024-04-20T22:29:36.842+05:30","type":"record"},"metadata":{"job_id":1}}`)
			_, err = tempFile.Write(data)
			Expect(err).ShouldNot(HaveOccurred(), "Failed to write to temporary file")

			received := manager.Upload(&asyncDestination)
			Expect(received).To(Equal(expected))
		})
	})

	Context("createSSHConfig", func() {
		BeforeEach(func() {
			config.Reset()
		})

		AfterEach(func() {
			config.Reset()
		})

		It("TestMissingPassword", func() {
			initSFTP()
			sshConfig, err := createSSHConfig(&destinations[1])
			Expect(sshConfig).To(BeNil())
			Expect(err).To(MatchError("invalid sftp configuration: password is required for password authentication"))
		})

		It("TestInvalidFileFormat", func() {
			initSFTP()
			sshConfig, err := createSSHConfig(&destinations[2])
			Expect(sshConfig).To(BeNil())
			Expect(err).To(MatchError("invalid sftp configuration: invalid file format: txt"))
		})

		It("TestInvalidPort", func() {
			initSFTP()
			sshConfig, err := createSSHConfig(&destinations[3])
			Expect(sshConfig).To(BeNil())
			Expect(err).To(MatchError("invalid sftp configuration: strconv.Atoi: parsing \"\": invalid syntax"))

			sshConfig, err = createSSHConfig(&destinations[4])
			Expect(sshConfig).To(BeNil())
			Expect(err).To(MatchError("invalid sftp configuration: invalid port: 0"))
		})

		It("TestSuccessfulSSHConfig", func() {
			initSFTP()
			sshConfig, err := createSSHConfig(&destinations[0])
			Expect(err).To(BeNil())
			Expect(sshConfig).ToNot(BeNil())
		})

		It("TestSuccessfulSSHConfigCreationWithPrivateKey", func() {
			initSFTP()
			sshConfig, err := createSSHConfig(&destinations[5])
			Expect(err).To(BeNil())
			Expect(sshConfig).ToNot(BeNil())
		})
	})

	Context("generateFile", func() {
		BeforeEach(func() {
			config.Reset()
		})

		AfterEach(func() {
			config.Reset()
		})

		It("TestJSONFileGeneration", func() {
			initSFTP()
			path, err := generateFile(filepath.Join(currentDir, "testdata/uploadDataRecord.txt"), "json")
			Expect(err).To(BeNil())
			Expect(path).ToNot(BeNil())
			Expect(os.Remove(path)).To(BeNil())
		})

		It("TestCSVFileGeneration", func() {
			initSFTP()
			path, err := generateFile(filepath.Join(currentDir, "testdata/uploadDataRecord.txt"), "csv")
			Expect(err).To(BeNil())
			Expect(path).ToNot(BeNil())
			Expect(os.Remove(path)).To(BeNil())
		})
	})

	Context("getUploadFilePath", func() {
		BeforeEach(func() {
			config.Reset()
		})

		AfterEach(func() {
			config.Reset()
		})

		It("TestReplaceDynamicVariables", func() {
			initSFTP()
			now := time.Now()
			input := "/path/to/{destinationID}_{jobRunID}/{YYYY}/{MM}/{DD}/{hh}/{mm}/{ss}/{ms}/{timestampInSec}/{timestampInMS}"
			expected := fmt.Sprintf("/path/to/%s_%s/%d/%02d/%02d/%02d/%02d/%02d/%03d/%d/%d", "some_destination_id", "some_source_job_run_id", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond()/1e6, now.Unix(), now.UnixNano()/1e6)
			metadata := map[string]any{
				"destinationID":  "some_destination_id",
				"sourceJobRunID": "some_source_job_run_id",
				"timestamp":      now,
			}
			received, err := getUploadFilePath(input, metadata)
			Expect(err).To(BeNil())
			Expect(received).To(Equal(expected))
		})

		It("TestNoDynamicVariables", func() {
			initSFTP()
			input := "/path/to/file.txt"
			expected := "/path/to/file.txt"
			received, err := getUploadFilePath(input, nil)
			Expect(err).To(BeNil())
			Expect(received).To(Equal(expected))
		})

		It("TestEmptyInputPath", func() {
			initSFTP()
			input := ""
			_, err := getUploadFilePath(input, nil)
			Expect(err).To(MatchError("upload file path can not be empty"))
		})

		It("TestInvalidDynamicVariables", func() {
			initSFTP()
			input := "/path/to/{invalid}/file.txt"
			expected := "/path/to/{invalid}/file.txt"
			received, err := getUploadFilePath(input, nil)
			Expect(err).To(BeNil())
			Expect(received).To(Equal(expected))
		})
	})
})
