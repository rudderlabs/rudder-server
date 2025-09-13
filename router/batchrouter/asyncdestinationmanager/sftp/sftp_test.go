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

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
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
			defaultManager := newDefaultManager(logger.NOP, stats.NOP, fileManager, destConfig{})
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
			defaultManager := newDefaultManager(logger.NOP, stats.NOP, fileManager, destConfig{})
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
			defaultManager := newDefaultManager(logger.NOP, stats.NOP, fileManager, destConfig{})
			now := time.Now()
			filePath := fmt.Sprintf("/tmp/testDir1/destination_id_1_someJobRunId_1/file_%d_%02d_%02d_%d_1.csv", now.Year(), now.Month(), now.Day(), now.Unix())
			fileManager.EXPECT().Upload(gomock.Any(), filePath).Return(nil)
			manager := common.SimpleAsyncDestinationManager{UploaderAndTransformer: defaultManager}
			payload := make(map[int64]stdjson.RawMessage)
			id := int64(1014)
			sampleData := map[string]interface{}{
				"source_job_run_id": "someJobRunId_1",
			}
			rawMessage, _ := jsonrs.Marshal(sampleData)
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
			config := destConfig{
				AuthMethod: "passwordAuth",
				Username:   "username",
				Host:       "host",
				Port:       "22",
				Password:   "",
				FileFormat: "csv",
				FilePath:   "/tmp/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
			}
			sshConfig, err := createSSHConfig(config)
			require.Nil(t, sshConfig)
			require.EqualError(t, err, "invalid sftp configuration: password is required for password authentication")
		})

		t.Run("TestInvalidFileFormat", func(t *testing.T) {
			config := destConfig{
				AuthMethod: "passwordAuth",
				Username:   "username",
				Host:       "host",
				Port:       "22",
				Password:   "password",
				FileFormat: "txt",
				FilePath:   "/tmp/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
			}
			sshConfig, err := createSSHConfig(config)
			require.Nil(t, sshConfig)
			require.EqualError(t, err, "invalid sftp configuration: invalid file format: txt")
		})

		t.Run("TestInvalidPort", func(t *testing.T) {
			config := destConfig{
				AuthMethod: "passwordAuth",
				Username:   "username",
				Host:       "host",
				Port:       "",
				Password:   "password",
				FileFormat: "csv",
				FilePath:   "/tmp/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
			}
			sshConfig, err := createSSHConfig(config)
			require.Nil(t, sshConfig)
			require.EqualError(t, err, `invalid sftp configuration: strconv.Atoi: parsing "": invalid syntax`)

			config.Port = "0"
			sshConfig, err = createSSHConfig(config)
			require.Nil(t, sshConfig)
			require.EqualError(t, err, "invalid sftp configuration: invalid port: 0")
		})

		t.Run("TestSuccessfulSSHConfig", func(t *testing.T) {
			config := destConfig{
				AuthMethod: "passwordAuth",
				Username:   "username",
				Host:       "host",
				Port:       "22",
				Password:   "password",
				FileFormat: "csv",
				FilePath:   "/tmp/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
			}
			sshConfig, err := createSSHConfig(config)
			require.NoError(t, err)
			require.NotNil(t, sshConfig)
		})

		t.Run("TestSuccessfulSSHConfigWithPrivateKey", func(t *testing.T) {
			config := destConfig{
				AuthMethod: "keyAuth",
				Username:   "username",
				Host:       "host",
				Port:       "22",
				PrivateKey: "privateKey",
				FileFormat: "csv",
				FilePath:   "/tmp/testDir1/{destinationID}_{jobRunID}/file_{YYYY}_{MM}_{DD}_{timestampInSec}.csv",
			}
			sshConfig, err := createSSHConfig(config)
			require.NoError(t, err)
			require.NotNil(t, sshConfig)
		})
	})

	t.Run("generateFile", func(t *testing.T) {
		t.Run("TestJSONFileGeneration", func(t *testing.T) {
			path, err := generateFile(filepath.Join(currentDir, "testdata/uploadDataRecord.txt"), "json", false)
			require.NoError(t, err)
			require.NotNil(t, path)
			require.NoError(t, os.Remove(path))
		})

		t.Run("TestCSVFileGeneration", func(t *testing.T) {
			path, err := generateFile(filepath.Join(currentDir, "testdata/uploadDataRecord.txt"), "csv", false)
			require.NoError(t, err)
			require.NotNil(t, path)
			require.NoError(t, os.Remove(path))
		})

		t.Run("TestCSVFileGenerationWithSortedColumns", func(t *testing.T) {
			path, err := generateFile(filepath.Join(currentDir, "testdata/uploadDataRecord.txt"), "csv", true)
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

	t.Run("getFieldNames", func(t *testing.T) {
		t.Run("TestSuccessfulFieldExtractionWithoutSorting", func(t *testing.T) {
			records := []record{
				{
					"message": map[string]any{
						"fields": map[string]any{
							"email": "test@example.com",
							"name":  "John Doe",
							"age":   "30",
							"city":  "New York",
						},
					},
				},
			}
			// Without sorting, order depends on map iteration order
			received, err := getFieldNames(records, false)
			require.NoError(t, err)
			require.Len(t, received, 5)
			require.Contains(t, received, "action")
			require.Contains(t, received, "email")
			require.Contains(t, received, "name")
			require.Contains(t, received, "age")
			require.Contains(t, received, "city")
		})

		t.Run("TestSuccessfulFieldExtractionWithSorting", func(t *testing.T) {
			records := []record{
				{
					"message": map[string]any{
						"fields": map[string]any{
							"email": "test@example.com",
							"name":  "John Doe",
							"age":   "30",
							"city":  "New York",
						},
					},
				},
			}
			expected := []string{"action", "age", "city", "email", "name"}
			received, err := getFieldNames(records, true)
			require.NoError(t, err)
			require.Equal(t, expected, received)
		})

		t.Run("TestEmptyRecords", func(t *testing.T) {
			records := []record{}
			_, err := getFieldNames(records, false)
			require.EqualError(t, err, "no records found")
		})

		t.Run("TestMissingMessage", func(t *testing.T) {
			records := []record{
				{
					"other": map[string]any{
						"fields": map[string]any{
							"email": "test@example.com",
						},
					},
				},
			}
			_, err := getFieldNames(records, false)
			require.EqualError(t, err, "message not found in the first record")
		})

		t.Run("TestMissingFields", func(t *testing.T) {
			records := []record{
				{
					"message": map[string]any{
						"other": "value",
					},
				},
			}
			_, err := getFieldNames(records, false)
			require.EqualError(t, err, "fields not found in the first record")
		})

		t.Run("TestFieldNamesSorting", func(t *testing.T) {
			records := []record{
				{
					"message": map[string]any{
						"fields": map[string]any{
							"zebra":  "value1",
							"apple":  "value2",
							"banana": "value3",
							"cherry": "value4",
						},
					},
				},
			}
			expected := []string{"action", "apple", "banana", "cherry", "zebra"}
			received, err := getFieldNames(records, true)
			require.NoError(t, err)
			require.Equal(t, expected, received)
		})
	})

	t.Run("parseRecords", func(t *testing.T) {
		t.Run("TestSuccessfulParsing", func(t *testing.T) {
			tempFile, err := os.CreateTemp("", "test_records_*.txt")
			require.NoError(t, err)
			defer os.Remove(tempFile.Name())

			testData := `{"message":{"action":"insert","fields":{"email":"test1@email.com","name":"test1"}}}
{"message":{"action":"update","fields":{"email":"test2@email.com","name":"test2"}}}
{"message":{"action":"insert","fields":{"email":"test3@email.com","name":"test3"}}}`
			_, err = tempFile.WriteString(testData)
			require.NoError(t, err)
			tempFile.Close()

			records, err := parseRecords(tempFile.Name())
			require.NoError(t, err)
			require.Len(t, records, 3)

			// Check first record
			message, ok := records[0]["message"].(map[string]any)
			require.True(t, ok)
			require.Equal(t, "insert", message["action"])
			fields, ok := message["fields"].(map[string]any)
			require.True(t, ok)
			require.Equal(t, "test1@email.com", fields["email"])
			require.Equal(t, "test1", fields["name"])
		})

		t.Run("TestNonExistentFile", func(t *testing.T) {
			_, err := parseRecords("non_existent_file.txt")
			require.Error(t, err)
			require.Contains(t, err.Error(), "opening file")
		})

		t.Run("TestInvalidJSON", func(t *testing.T) {
			tempFile, err := os.CreateTemp("", "test_invalid_*.txt")
			require.NoError(t, err)
			defer os.Remove(tempFile.Name())

			_, err = tempFile.WriteString(`{"invalid": json}`)
			require.NoError(t, err)
			tempFile.Close()

			_, err = parseRecords(tempFile.Name())
			require.Error(t, err)
			require.Contains(t, err.Error(), "error parsing JSON record")
		})

		t.Run("TestEmptyFile", func(t *testing.T) {
			tempFile, err := os.CreateTemp("", "test_empty_*.txt")
			require.NoError(t, err)
			defer os.Remove(tempFile.Name())
			tempFile.Close()

			records, err := parseRecords(tempFile.Name())
			require.NoError(t, err)
			require.Len(t, records, 0)
		})
	})

	t.Run("validation functions", func(t *testing.T) {
		t.Run("validateFilePath", func(t *testing.T) {
			t.Run("ValidFilePath", func(t *testing.T) {
				path := "/path/to/{destinationID}_{jobRunID}/file.csv"
				err := validateFilePath(path)
				require.NoError(t, err)
			})

			t.Run("MissingDestinationID", func(t *testing.T) {
				path := "/path/to/{jobRunID}/file.csv"
				err := validateFilePath(path)
				require.EqualError(t, err, "destinationID placeholder is missing in the upload filePath")
			})

			t.Run("MissingJobRunID", func(t *testing.T) {
				path := "/path/to/{destinationID}/file.csv"
				err := validateFilePath(path)
				require.EqualError(t, err, "jobRunID placeholder is missing in the upload filePath")
			})

			t.Run("MissingBothPlaceholders", func(t *testing.T) {
				path := "/path/to/file.csv"
				err := validateFilePath(path)
				require.EqualError(t, err, "destinationID placeholder is missing in the upload filePath")
			})
		})

		t.Run("isValidPort", func(t *testing.T) {
			t.Run("ValidPorts", func(t *testing.T) {
				validPorts := []string{"1", "22", "80", "443", "8080", "65535"}
				for _, port := range validPorts {
					err := isValidPort(port)
					require.NoError(t, err, "Port %s should be valid", port)
				}
			})

			t.Run("InvalidPorts", func(t *testing.T) {
				invalidPorts := []string{"0", "65536", "-1", "abc", ""}
				for _, port := range invalidPorts {
					err := isValidPort(port)
					require.Error(t, err, "Port %s should be invalid", port)
				}
			})
		})

		t.Run("isValidFileFormat", func(t *testing.T) {
			t.Run("ValidFormats", func(t *testing.T) {
				validFormats := []string{"json", "csv"}
				for _, format := range validFormats {
					err := isValidFileFormat(format)
					require.NoError(t, err, "Format %s should be valid", format)
				}
			})

			t.Run("InvalidFormats", func(t *testing.T) {
				invalidFormats := []string{"JSON", "CSV", "Json", "Csv", "txt", "xml", "parquet", "avro", ""}
				for _, format := range invalidFormats {
					err := isValidFileFormat(format)
					require.Error(t, err, "Format %s should be invalid", format)
				}
			})
		})
	})

	t.Run("appendFileNumberInFilePath", func(t *testing.T) {
		t.Run("TestWithExtension", func(t *testing.T) {
			path := "/path/to/file.csv"
			expected := "/path/to/file_1.csv"
			received := appendFileNumberInFilePath(path, 1)
			require.Equal(t, expected, received)
		})

		t.Run("TestWithoutExtension", func(t *testing.T) {
			path := "/path/to/file"
			expected := "/path/to/file_2"
			received := appendFileNumberInFilePath(path, 2)
			require.Equal(t, expected, received)
		})

		t.Run("TestMultipleExtensions", func(t *testing.T) {
			path := "/path/to/file.backup.csv"
			expected := "/path/to/file.backup_3.csv"
			received := appendFileNumberInFilePath(path, 3)
			require.Equal(t, expected, received)
		})

		t.Run("TestZeroFileNumber", func(t *testing.T) {
			path := "/path/to/file.json"
			expected := "/path/to/file_0.json"
			received := appendFileNumberInFilePath(path, 0)
			require.Equal(t, expected, received)
		})
	})

	t.Run("generateErrorOutput", func(t *testing.T) {
		t.Run("TestErrorOutputGeneration", func(t *testing.T) {
			errMsg := "connection failed"
			importingJobIds := []int64{1014, 1015, 1016, 1017}
			destinationID := "test_destination_id"

			expected := common.AsyncUploadOutput{
				DestinationID: destinationID,
				AbortCount:    len(importingJobIds),
				AbortJobIDs:   importingJobIds,
				AbortReason:   errMsg,
			}

			received := generateErrorOutput(errMsg, importingJobIds, destinationID)
			require.Equal(t, expected, received)
		})

		t.Run("TestEmptyJobIds", func(t *testing.T) {
			errMsg := "no jobs to process"
			importingJobIds := []int64{}
			destinationID := "test_destination_id"

			expected := common.AsyncUploadOutput{
				DestinationID: destinationID,
				AbortCount:    0,
				AbortJobIDs:   importingJobIds,
				AbortReason:   errMsg,
			}

			received := generateErrorOutput(errMsg, importingJobIds, destinationID)
			require.Equal(t, expected, received)
		})
	})
}
