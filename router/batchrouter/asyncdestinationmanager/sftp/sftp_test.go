package sftp

import (
	stdjson "encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/sftp/mock_sftp"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

var destinations = []backendconfig.DestinationT{
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

func TestSFTP(t *testing.T) {
	currentDir, _ := os.Getwd()

	t.Run("Upload", func(t *testing.T) {
		t.Run("TestUploadWrongFilepath", func(t *testing.T) {
			ctrl := gomock.NewController(t)
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
			require.Equal(t, expected, received)
		})

		t.Run("TestErrorUploadingFileOnRemoteServer", func(t *testing.T) {
			ctrl := gomock.NewController(t)
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
			require.Equal(t, expected, received)
		})

		t.Run("TestSuccessfulUpload", func(t *testing.T) {
			ctrl := gomock.NewController(t)
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
			require.NoError(t, err)

			tempFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
			require.NoError(t, err)
			t.Cleanup(func() { _ = os.RemoveAll("/tmp/testDir1") })

			data := []byte(`{"message":{"action":"insert","fields":{"email":"john@email.com","name":"john"},"messageId":"b6543bc0-f280-4642-8176-4b8d5a1b8fb6","originalTimestamp":"2024-04-20T22:29:37.731+05:30","receivedAt":"2024-04-20T22:29:36.843+05:30","request_ip":"[::1]","rudderId":"853ae90f-0351-424b-973e-a615e6487517","sentAt":"2024-04-20T22:29:37.731+05:30","timestamp":"2024-04-20T22:29:36.842+05:30","type":"record"},"metadata":{"job_id":1}}`)
			_, err = tempFile.Write(data)
			require.NoError(t, err)

			received := manager.Upload(&asyncDestination)
			require.Equal(t, expected, received)
		})
	})

	t.Run("createSSHConfig", func(t *testing.T) {
		t.Run("TestMissingPassword", func(t *testing.T) {
			sshConfig, err := createSSHConfig(&destinations[1])
			require.Nil(t, sshConfig)
			require.EqualError(t, err, "invalid sftp configuration: password is required for password authentication")
		})

		t.Run("TestInvalidFileFormat", func(t *testing.T) {
			sshConfig, err := createSSHConfig(&destinations[2])
			require.Nil(t, sshConfig)
			require.EqualError(t, err, "invalid sftp configuration: invalid file format: txt")
		})

		t.Run("TestInvalidPort", func(t *testing.T) {
			sshConfig, err := createSSHConfig(&destinations[3])
			require.Nil(t, sshConfig)
			require.EqualError(t, err, `invalid sftp configuration: strconv.Atoi: parsing "": invalid syntax`)

			sshConfig, err = createSSHConfig(&destinations[4])
			require.Nil(t, sshConfig)
			require.EqualError(t, err, "invalid sftp configuration: invalid port: 0")
		})

		t.Run("TestSuccessfulSSHConfig", func(t *testing.T) {
			sshConfig, err := createSSHConfig(&destinations[0])
			require.NoError(t, err)
			require.NotNil(t, sshConfig)
		})

		t.Run("TestSuccessfulSSHConfigWithPrivateKey", func(t *testing.T) {
			sshConfig, err := createSSHConfig(&destinations[5])
			require.NoError(t, err)
			require.NotNil(t, sshConfig)
		})
	})

	t.Run("generateFile", func(t *testing.T) {
		t.Run("TestJSONFileGeneration", func(t *testing.T) {
			path, err := generateFile(filepath.Join(currentDir, "testdata/uploadDataRecord.txt"), "json")
			require.NoError(t, err)
			require.NotNil(t, path)
			require.NoError(t, os.Remove(path))
		})

		t.Run("TestCSVFileGeneration", func(t *testing.T) {
			path, err := generateFile(filepath.Join(currentDir, "testdata/uploadDataRecord.txt"), "csv")
			require.NoError(t, err)
			require.NotNil(t, path)
			require.NoError(t, os.Remove(path))
		})
	})

	t.Run("getUploadFilePath", func(t *testing.T) {
		t.Run("TestReplaceDynamicVariables", func(t *testing.T) {
			now := time.Now()
			input := "/path/to/{destinationID}_{jobRunID}/{YYYY}/{MM}/{DD}/{hh}/{mm}/{ss}/{ms}/{timestampInSec}/{timestampInMS}"
			expected := fmt.Sprintf("/path/to/%s_%s/%d/%02d/%02d/%02d/%02d/%02d/%03d/%d/%d", "some_destination_id", "some_source_job_run_id", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond()/1e6, now.Unix(), now.UnixNano()/1e6)
			metadata := map[string]any{
				"destinationID":  "some_destination_id",
				"sourceJobRunID": "some_source_job_run_id",
				"timestamp":      now,
			}
			received, err := getUploadFilePath(input, metadata)
			require.NoError(t, err)
			require.Equal(t, expected, received)
		})

		t.Run("TestNoDynamicVariables", func(t *testing.T) {
			input := "/path/to/file.txt"
			expected := "/path/to/file.txt"
			received, err := getUploadFilePath(input, nil)
			require.NoError(t, err)
			require.Equal(t, expected, received)
		})

		t.Run("TestEmptyInputPath", func(t *testing.T) {
			input := ""
			_, err := getUploadFilePath(input, nil)
			require.EqualError(t, err, "upload file path can not be empty")
		})

		t.Run("TestInvalidDynamicVariables", func(t *testing.T) {
			input := "/path/to/{invalid}/file.txt"
			expected := "/path/to/{invalid}/file.txt"
			received, err := getUploadFilePath(input, nil)
			require.NoError(t, err)
			require.Equal(t, expected, received)
		})
	})
}
