package batch_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/batch"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/initialize"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/stretchr/testify/require"
)

var (
	regexRequiredSuffix = regexp.MustCompile(".json.gz$")
	mockBucketLocation  string
)

const mockBucket = "mockBucket"

func TestBatchDelete(t *testing.T) {

	initialize.Init()

	ctx := context.Background()
	tests := []struct {
		name           string
		job            model.Job
		dest           model.Destination
		expectedErr    error
		expectedStatus model.JobStatus
	}{
		{
			name: "testing batch deletion flow by deletion from 'mock_batch' destination",
			job: model.Job{
				ID:            1,
				WorkspaceID:   "1001",
				DestinationID: "1234",
				Status:        model.JobStatusPending,
				UserAttributes: []model.UserAttribute{
					{
						UserID: "Jermaine1473336609491897794707338",
						Phone:  strPtr("6463633841"),
						Email:  strPtr("dorowane8n285680461479465450293436@gmail.com"),
					},
					{
						UserID: "Mercie8221821544021583104106123",
						Email:  strPtr("dshirilad8536019424659691213279980@gmail.com"),
					},
					{
						UserID: "Claiborn443446989226249191822329",
						Phone:  strPtr("8782905113"),
					},
				},
			},
			dest: model.Destination{
				Config: map[string]interface{}{
					"bucketName":  "regulation-test-data",
					"accessKeyID": "abc",
					"accessKey":   "xyz",
					"enableSSE":   false,
					"prefix":      "reg-original",
				},
				Name: "S3",
			},
		},
	}
	bm := batch.BatchManager{
		FMFactory: mockFileManagerFactory{},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := bm.Delete(ctx, tt.job, tt.dest.Config, tt.dest.Name)
			require.Equal(t, model.JobStatusComplete, status)

			searchDir := mockBucketLocation
			var cleanedFilesList []string
			err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
				if regexRequiredSuffix.Match([]byte(path)) {
					cleanedFilesList = append(cleanedFilesList, path)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			sort.Strings(cleanedFilesList)

			searchDir = "./goldenFile"
			var goldenFilesList []string
			err = filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
				if regexRequiredSuffix.Match([]byte(path)) {
					goldenFilesList = append(goldenFilesList, path)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			sort.Strings(goldenFilesList)
			require.Equal(t, len(goldenFilesList), len(cleanedFilesList), "actual number of files in destination bucket different than expected")
			for i := 0; i < len(goldenFilesList); i++ {
				goldenFilePtr, err := os.Open(goldenFilesList[i])
				if err != nil {
					t.Fatal(err)
				}
				defer goldenFilePtr.Close()
				goldenFileContent, err := io.ReadAll(goldenFilePtr)
				if err != nil {
					t.Fatal(err)
				}

				cleanedFilePtr, err := os.Open(goldenFilesList[i])
				if err != nil {
					t.Fatal(err)
				}
				defer cleanedFilePtr.Close()
				cleanedFileContent, err := io.ReadAll(cleanedFilePtr)
				if err != nil {
					t.Fatal(err)
				}

				require.Equal(t, 0, strings.Compare(string(goldenFileContent), string(cleanedFileContent)), "actual file different than expected")
			}
			err = os.RemoveAll(mockBucketLocation)
			if err != nil {
				t.Fatal(err)
			}
		})

	}
}

func strPtr(str string) *string {
	return &(str)
}

type mockFileManagerFactory struct {
}

//creates a tmp directory & copy all the content of testData in it, to use it as mockBucket & store it in mockFileManager struct.
func (ff mockFileManagerFactory) New(settings *filemanager.SettingsT) (filemanager.FileManager, error) {
	//create tmp directory
	//parent directory of all the temporary files created/downloaded in the process of deletion.
	tmpDirPath, err := os.MkdirTemp("", "")
	if err != nil {
		panic(err)
	}
	//copy all content of testData in the tmp directory
	_, err = exec.Command("cp", "-r", mockBucket, tmpDirPath).Output()
	if err != nil {
		return nil, fmt.Errorf("error while running cp command: %s", err)
	}
	// mockBucketLocation = fmt.Sprintf("%s%s", tmpDirPath, "/mockBucket")
	mockBucketLocation = fmt.Sprintf("%s/%s", tmpDirPath, mockBucket)
	//store the location in mockBucketLocation.
	return &mockFileManager{
		mockBucketLocation: mockBucketLocation,
	}, nil
}

type mockFileManager struct {
	mockBucketLocation string
	listCalled         bool
}

func (fm *mockFileManager) SetTimeout(timeout *time.Duration) {
	return
}

//Given a file pointer with cleaned file content upload to the appropriate destination, with the same name as the original.
func (fm *mockFileManager) Upload(ctx context.Context, file *os.File, prefixes ...string) (filemanager.UploadOutput, error) {
	splitFileName := strings.Split(file.Name(), "/")
	fileName := ""
	if len(prefixes) > 0 {
		fileName = strings.Join(prefixes[:], "/") + "/"
	}
	fileName += splitFileName[len(splitFileName)-1]
	//copy the content of file to mockBucektLocation+fileName
	finalFileName := fmt.Sprintf("%s/%s", fm.mockBucketLocation, fileName)
	uploadFilePtr, err := os.OpenFile(finalFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return filemanager.UploadOutput{}, err
	}
	defer uploadFilePtr.Close()
	_, err = io.Copy(uploadFilePtr, file)
	if err != nil {
		return filemanager.UploadOutput{}, err
	}

	return filemanager.UploadOutput{
		Location:   fm.mockBucketLocation + "/" + fileName,
		ObjectName: fileName,
	}, nil
}

//Given a file name download & simply save it in the given file pointer.
func (fm *mockFileManager) Download(ctx context.Context, outputFilePtr *os.File, location string) error {
	finalFileName := fmt.Sprintf("%s%s%s", fm.mockBucketLocation, "/", location)
	uploadFilePtr, err := os.OpenFile(finalFileName, os.O_RDWR, 0644)
	if err != nil {
		if strings.Contains(finalFileName, batch.StatusTrackerFileName) {
			return nil
		}
		return err
	}
	_, err = io.Copy(outputFilePtr, uploadFilePtr)
	if err != nil {
		return err
	}

	return nil
}

//Given a file name as key, delete if it is present in the bucket.
func (fm *mockFileManager) DeleteObjects(ctx context.Context, keys []string) error {
	for _, key := range keys {
		fileLocation := fmt.Sprint(fm.mockBucketLocation, "/", key)
		_, err := exec.Command("rm", "-rf", fileLocation).Output()
		if err != nil {
			return err
		}
	}
	return nil
}

//given prefix & maxItems, return with list of Fileobject in the bucket.
func (fm *mockFileManager) ListFilesWithPrefix(ctx context.Context, prefix string, maxItems int64) (fileObjects []*filemanager.FileObject, err error) {
	if fm.listCalled {
		return []*filemanager.FileObject{}, nil
	}
	fm.listCalled = true
	searchDir := fm.mockBucketLocation
	err = filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		splitStr := strings.Split(path, mockBucket)
		finalStr := strings.TrimLeft(splitStr[len(splitStr)-1], "/")
		if finalStr != "" {
			fileObjects = append(fileObjects, &filemanager.FileObject{Key: splitStr[len(splitStr)-1]})
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	return fileObjects, nil
}

func (fm *mockFileManager) GetObjectNameFromLocation(string) (string, error) {
	return "", nil
}

func (fm *mockFileManager) GetDownloadKeyFromFileLocation(location string) string {
	return ""
}

func (fm *mockFileManager) GetConfiguredPrefix() string {
	return ""
}
