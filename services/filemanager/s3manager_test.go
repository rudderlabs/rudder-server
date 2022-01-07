package filemanager_test

import (
	"crypto/sha1"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"

	"github.com/minio/minio-go/v6"
	"github.com/ory/dockertest"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

var (
	// Configure to use MinIO Server
	minioEndpoint       string
	bucket              = "filemanager-test-1"
	region              = "us-east-1"
	hold                bool
	regexRequiredSuffix = regexp.MustCompile(".json.gz$")
	fileList            []string
)

func TestMain(m *testing.M) {
	config.Load()
	logger.Init()

	os.Exit(run(m))
}

//run minio server & store data in it.
func run(m *testing.M) int {

	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.Parse()

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "latest",
		Cmd:        []string{"server", "/data"},
		Env:        []string{"MINIO_ACCESS_KEY=MYACCESSKEY", "MINIO_SECRET_KEY=MYSECRETKEY", "MINIO_SITE_REGION=us-east-1"},
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	minioEndpoint = fmt.Sprintf("localhost:%s", resource.GetPort("9000/tcp"))

	//check if minio server is up & running.
	if err := pool.Retry(func() error {
		url := fmt.Sprintf("http://%s/minio/health/live", minioEndpoint)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status code not OK")
		}
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	fmt.Println("minio is up & running properly")

	accessKeyId := "MYACCESSKEY"
	secretAccessKey := "MYSECRETKEY"
	useSSL := false
	minioClient, err := minio.New(minioEndpoint, accessKeyId, secretAccessKey, useSSL)
	if err != nil {
		panic(err)
	}
	fmt.Println("minioClient created successfully")
	err = minioClient.MakeBucket(bucket, "")
	if err != nil {
		panic(err)
	}
	fmt.Println("bucket created successfully")
	searchDir := "./testData"
	err = filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if regexRequiredSuffix.Match([]byte(path)) {
			fileList = append(fileList, path)
		}
		return nil
	})
	if err != nil {
		fmt.Println("Error while getting fileList: ", err)
	}
	fmt.Println("files list: ", fileList)
	m.Run()
	blockOnHold()
	return 0
}

func TestS3Manager(t *testing.T) {

	// ctx := context.Background()
	tests := []struct {
		name     string
		destName string
		config   map[string]interface{}
	}{
		{
			name:     "testing s3manager functionality",
			destName: "S3",
			config: map[string]interface{}{
				"bucketName":       bucket,
				"accessKeyID":      "MYACCESSKEY",
				"accessKey":        "MYSECRETKEY",
				"enableSSE":        false,
				"prefix":           "some-prefix",
				"endPoint":         minioEndpoint,
				"s3ForcePathStyle": true,
				"disableSSL":       true,
				"region":           "us-east-1",
			},
		},
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			fmFactory := filemanager.FileManagerFactoryT{}
			fm, err := fmFactory.New(&filemanager.SettingsT{
				Provider: tt.destName,
				Config:   tt.config,
			})
			if err != nil {
				panic(err)
			}

			//upload all files
			uploadOutputs := make([]filemanager.UploadOutput, 0)
			for _, file := range fileList {
				filePtr, err := os.Open(file)
				if err != nil {
					fmt.Println("error while opening testData file to upload: ", err)
				}
				uploadOutput, err := fm.Upload(filePtr)
				if err != nil {
					panic(err)
				}
				uploadOutputs = append(uploadOutputs, uploadOutput)
			}

			//list files using ListFilesWithPrefix
			originalFileObject, err := fm.ListFilesWithPrefix("", 10)
			if err != nil {
				fmt.Println("error while getting file object: ", err)
			}

			//based on the obtained location, get object name by calling GetObjectNameFromLocation
			objectName, err := fm.GetObjectNameFromLocation(uploadOutputs[0].Location)
			require.NoError(t, err, "no error expected")
			require.Equal(t, uploadOutputs[0].ObjectName, objectName, "actual object name different than expected")

			//also get download key from file location by calling GetDownloadKeyFromFileLocation
			parsedUrl, _ := url.Parse(uploadOutputs[0].Location)
			expectedKey := strings.TrimLeft(parsedUrl.Path, "/")
			key := fm.GetDownloadKeyFromFileLocation(uploadOutputs[0].Location)
			require.Equal(t, expectedKey, key, "actual object key different than expected")

			//get prefix based on config
			splitString := strings.Split(uploadOutputs[0].ObjectName, "/")
			expectedPrefix := splitString[0]
			prefix := fm.GetConfiguredPrefix()
			require.Equal(t, expectedPrefix, prefix, "actual prefix different than expected")

			//download one of the files & assert if it matches the original one present locally.
			filePtr, err := os.Open(fileList[0])
			if err != nil {
				fmt.Printf("error: %s while opening file: %s ", err, fileList[0])
			}
			originalFile, err := io.ReadAll(filePtr)
			if err != nil {
				fmt.Printf("error: %s, while reading file: %s", err, fileList[0])
			}
			h1 := sha1.New()
			h1.Write(originalFile)
			expectedHash := h1.Sum(nil)

			DownloadedFileName := "TmpDownloadedFile"
			filePtr, err = os.OpenFile(DownloadedFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
			if err != nil {
				fmt.Println("error while Creating file to download data: ", err)
			}
			defer os.Remove(DownloadedFileName)

			err = fm.Download(filePtr, uploadOutputs[0].ObjectName)
			require.NoError(t, err, "expected no error")
			downloadedFile, err := io.ReadAll(filePtr)
			if err != nil {
				fmt.Println("error while reading downloaded file: ", err)
			}
			h2 := sha1.New()
			h2.Write(downloadedFile)
			actualHash := h2.Sum(nil)
			require.Equal(t, expectedHash, actualHash, "actual hash different than expected")

			// //delete that file
			err = fm.DeleteObjects([]string{uploadOutputs[1].ObjectName})
			require.NoError(t, err, "expected no error while deleting object")

			// list files again & assert if that file is still present.
			fmFactoryNew := filemanager.FileManagerFactoryT{}
			fmNew, err := fmFactoryNew.New(&filemanager.SettingsT{
				Provider: tt.destName,
				Config:   tt.config,
			})
			if err != nil {
				panic(err)
			}
			newFileObject, err := fmNew.ListFilesWithPrefix("", 10)
			if err != nil {
				fmt.Println("error while getting new file object: ", err)
			}
			require.Equal(t, len(originalFileObject)-1, len(newFileObject), "expected original file list length to be greater than new list by 1, but is different")
		})
	}

}

func blockOnHold() {
	if !hold {
		return
	}

	log.Println("Test on hold, before cleanup")
	log.Println("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	close(c)
}
