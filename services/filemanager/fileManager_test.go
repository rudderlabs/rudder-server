package filemanager_test

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/minio/minio-go/v6"
	"github.com/ory/dockertest"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

var (
	AzuriteEndpoint, gcsURL, minioEndpoint string
	base64Secret                           = base64.StdEncoding.EncodeToString([]byte(secretAccessKey))
	bucket                                 = "filemanager-test-1"
	region                                 = "us-east-1"
	accessKeyId                            = "MYACCESSKEY"
	secretAccessKey                        = "MYSECRETKEY"
	hold                                   bool
	regexRequiredSuffix                    = regexp.MustCompile(".json.gz$")
	fileList                               []string
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

	//docker pool setup
	pool, err := dockertest.NewPool("")
	if err != nil {
		panic(fmt.Errorf("Could not connect to docker: %s", err))
	}

	// running minio container on docker
	minioResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "minio/minio",
		Tag:        "latest",
		Cmd:        []string{"server", "/data"},
		Env: []string{
			fmt.Sprintf("MINIO_ACCESS_KEY=%s", accessKeyId),
			fmt.Sprintf("MINIO_SECRET_KEY=%s", secretAccessKey),
			fmt.Sprintf("MINIO_SITE_REGION=%s", region),
		},
	})
	if err != nil {
		panic(fmt.Errorf("Could not start resource: %s", err))
	}
	defer func() {
		if err := pool.Purge(minioResource); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	minioEndpoint = fmt.Sprintf("localhost:%s", minioResource.GetPort("9000/tcp"))

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

	useSSL := false
	minioClient, err := minio.New(minioEndpoint, accessKeyId, secretAccessKey, useSSL)
	if err != nil {
		panic(err)
	}
	fmt.Println("minioClient created successfully")

	//creating bucket inside minio where testing will happen.
	err = minioClient.MakeBucket(bucket, "us-east-1")
	if err != nil {
		panic(err)
	}
	fmt.Println("bucket created successfully")

	// Running Azure emulator, Azurite.
	AzuriteResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mcr.microsoft.com/azure-storage/azurite",
		Tag:        "latest",
		Env: []string{
			fmt.Sprintf("AZURITE_ACCOUNTS=%s:%s", accessKeyId, base64Secret),
			fmt.Sprintf("DefaultEndpointsProtocol=%s", "http"),
		},
	})
	if err != nil {
		log.Fatalf("Could not start azure resource: %s", err)
	}
	defer func() {
		if err := pool.Purge(AzuriteResource); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()
	AzuriteEndpoint = fmt.Sprintf("localhost:%s", AzuriteResource.GetPort("10000/tcp"))
	fmt.Println("Azurite endpoint", AzuriteEndpoint)
	fmt.Println("azurite resource successfully created")

	// Running GCS emulator
	GCSResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "fsouza/fake-gcs-server",
		Tag:        "latest",
		Cmd:        []string{"-scheme", "http"},
	})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err := pool.Purge(GCSResource); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	GCSEndpoint := fmt.Sprintf("localhost:%s", GCSResource.GetPort("4443/tcp"))
	fmt.Println("GCS test server successfully created with endpoint: ", GCSEndpoint)
	gcsURL = fmt.Sprintf("http://%s/storage/v1/", GCSEndpoint)
	os.Setenv("STORAGE_EMULATOR_HOST", fmt.Sprintf("%s/storage/v1/", GCSEndpoint))
	client, err := storage.NewClient(context.TODO(), option.WithEndpoint(gcsURL))
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	bkt := client.Bucket(bucket)
	err = bkt.Create(context.Background(), "test", &storage.BucketAttrs{Name: bucket})
	if err != nil {
		fmt.Println("error while creating bucket: ", err)
	}
	fmt.Println("bucket created successfully")

	//getting list of files in `testData` directory while will be used to testing filemanager.
	searchDir := "./goldenDirectory"
	err = filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if regexRequiredSuffix.Match([]byte(path)) {
			fileList = append(fileList, path)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	if len(fileList) == 0 {
		panic("file list empty, no data to test.")
	}
	fmt.Println("files list: ", fileList)

	code := m.Run()
	blockOnHold()
	return code
}

func TestFileManager(t *testing.T) {

	tests := []struct {
		name     string
		skip     string
		destName string
		config   map[string]interface{}
	}{
		{
			name:     "testing s3manager functionality",
			destName: "S3",
			config: map[string]interface{}{
				"bucketName":       bucket,
				"accessKeyID":      accessKeyId,
				"accessKey":        secretAccessKey,
				"enableSSE":        false,
				"prefix":           "some-prefix",
				"endPoint":         minioEndpoint,
				"s3ForcePathStyle": true,
				"disableSSL":       true,
				"region":           region,
			},
		},
		{
			name:     "testing minio functionality",
			destName: "MINIO",
			config: map[string]interface{}{
				"bucketName":       bucket,
				"accessKeyID":      accessKeyId,
				"secretAccessKey":  secretAccessKey,
				"enableSSE":        false,
				"prefix":           "some-prefix",
				"endPoint":         minioEndpoint,
				"s3ForcePathStyle": true,
				"disableSSL":       true,
				"region":           region,
			},
		},
		{
			name:     "testing digital ocean functionality",
			destName: "DIGITAL_OCEAN_SPACES",
			config: map[string]interface{}{
				"bucketName":     bucket,
				"accessKeyID":    accessKeyId,
				"accessKey":      secretAccessKey,
				"prefix":         "some-prefix",
				"endPoint":       minioEndpoint,
				"forcePathStyle": true,
				"disableSSL":     true,
				"region":         region,
				"enableSSE":      false,
			},
		},
		{
			name:     "testing Azure blob storage filemanager functionality",
			destName: "AZURE_BLOB",
			config: map[string]interface{}{
				"containerName":  bucket,
				"prefix":         "some-prefix",
				"accountName":    accessKeyId,
				"accountKey":     string(base64Secret),
				"endPoint":       AzuriteEndpoint,
				"forcePathStyle": true,
				"disableSSL":     true,
			},
		},
		{
			skip:     "storage emulator is not stable",
			name:     "testing GCS filemanager functionality",
			destName: "GCS",
			config: map[string]interface{}{
				"bucketName":       bucket,
				"prefix":           "some-prefix",
				"endPoint":         gcsURL,
				"s3ForcePathStyle": true,
				"disableSSL":       true,
			},
		},
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}
			fmFactory := filemanager.FileManagerFactoryT{}
			fm, err := fmFactory.New(&filemanager.SettingsT{
				Provider: tt.destName,
				Config:   tt.config,
			})
			if err != nil {
				t.Fatal(err)
			}

			//upload all files
			uploadOutputs := make([]filemanager.UploadOutput, 0)
			for _, file := range fileList {
				filePtr, err := os.Open(file)
				require.NoError(t, err, "error while opening testData file to upload")
				uploadOutput, err := fm.Upload(context.TODO(), filePtr)
				if err != nil {
					t.Fatal(err)
				}
				uploadOutputs = append(uploadOutputs, uploadOutput)
				filePtr.Close()
			}
			//list files using ListFilesWithPrefix
			originalFileObject, err := fm.ListFilesWithPrefix(context.TODO(), "", 1000)
			require.Equal(t, len(fileList), len(originalFileObject), "actual number of files different than expected")
			require.NoError(t, err, "expected no error while listing files")

			//based on the obtained location, get object name by calling GetObjectNameFromLocation
			objectName, err := fm.GetObjectNameFromLocation(uploadOutputs[0].Location)
			require.NoError(t, err, "no error expected")
			require.Equal(t, uploadOutputs[0].ObjectName, objectName, "actual object name different than expected")

			//also get download key from file location by calling GetDownloadKeyFromFileLocation
			expectedKey := uploadOutputs[0].ObjectName
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
			filePtr.Close()

			DownloadedFileName := "TmpDownloadedFile"
			filePtr, err = os.OpenFile(DownloadedFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
			if err != nil {
				fmt.Println("error while Creating file to download data: ", err)
			}
			defer os.Remove(DownloadedFileName)
			err = fm.Download(context.TODO(), filePtr, key)

			require.NoError(t, err, "expected no error")
			filePtr.Close()

			filePtr, err = os.OpenFile(DownloadedFileName, os.O_RDWR, 0644)
			if err != nil {
				fmt.Println("error while Creating file to download data: ", err)
			}
			downloadedFile, err := io.ReadAll(filePtr)
			if err != nil {
				fmt.Println("error while reading downloaded file: ", err)
			}
			filePtr.Close()

			ans := strings.Compare(string(originalFile), string(downloadedFile))
			require.Equal(t, 0, ans, "downloaded file different than actual file")
			//delete that file
			err = fm.DeleteObjects(context.TODO(), []string{key})
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
			newFileObject, err := fmNew.ListFilesWithPrefix(context.TODO(), "", 1000)
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
