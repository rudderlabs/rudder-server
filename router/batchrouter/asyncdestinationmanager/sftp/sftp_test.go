package sftp

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_sftp "github.com/rudderlabs/rudder-server/mocks/batchrouter/sftp"
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
				"filePath":   "/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
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
				"filePath":   "/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
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
				"filePath":   "/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
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
				"filePath":   "/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
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
				"filePath":   "/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
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
				"filePath":   "/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
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
			defaultManager := newDefaultManager(fileManager)
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
			defaultManager := newDefaultManager(fileManager)
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
			defaultManager := newDefaultManager(fileManager)
			fileManager.EXPECT().Upload(gomock.Any(), gomock.Any()).Return(nil)
			manager := common.SimpleAsyncDestinationManager{UploaderAndTransformer: defaultManager}
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1014, 1015, 1016, 1017},
				FileName:        filepath.Join(currentDir, "testdata/uploadDataRecord.txt"),
				Destination:     &destinations[0],
				Manager:         manager,
			}
			expected := common.AsyncUploadOutput{
				DestinationID:   "destination_id_1",
				SucceededJobIDs: []int64{1014, 1015, 1016, 1017},
				SuccessResponse: "File Upload Success",
			}
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
