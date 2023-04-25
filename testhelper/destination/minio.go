package destination

import (
	_ "encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	_ "github.com/lib/pq"
	"github.com/minio/minio-go"
	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"

	kitHelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type MINIOResource struct {
	Endpoint     string
	BucketName   string
	Port         string
	AccessKey    string
	SecretKey    string
	SiteRegion   string
	ResourceName string
	Client       *minio.Client
}

func SetupMINIO(pool *dockertest.Pool, d Cleaner) (*MINIOResource, error) {
	region := "us-east-1"
	accessKey := "MYACCESSKEY"
	secretKey := "MYSECRETKEY"
	bucketName := "testbucket"

	minioPortInt, err := kitHelper.GetFreePort()
	if err != nil {
		fmt.Println(err)
	}
	minioPort := fmt.Sprintf("%s/tcp", strconv.Itoa(minioPortInt))
	log.Println("minioPort:", minioPort)
	// Setup MINIO
	var minioClient *minio.Client

	options := &dockertest.RunOptions{
		Hostname:   "minio",
		Repository: "minio/minio",
		Tag:        "latest",
		Cmd:        []string{"server", "/data"},
		PortBindings: map[dc.Port][]dc.PortBinding{
			"9000/tcp": {{HostPort: strconv.Itoa(minioPortInt)}},
		},
		Env: []string{
			"MINIO_ACCESS_KEY=" + accessKey,
			"MINIO_SECRET_KEY=" + secretKey,
			"MINIO_SITE_REGION=" + region,
		},
	}

	minioContainer, err := pool.RunWithOptions(options)
	if err != nil {
		return nil, err
	}
	d.Cleanup(func() {
		if err := pool.Purge(minioContainer); err != nil {
			d.Log("Could not purge resource:", err)
		}
	})

	minioEndpoint := fmt.Sprintf("localhost:%s", minioContainer.GetPort("9000/tcp"))

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	// the minio client does not do service discovery for you (i.e. it does not check if connection can be established), so we have to use the health check
	if err := pool.Retry(func() error {
		url := fmt.Sprintf("http://%s/minio/health/live", minioEndpoint)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer func() { httputil.CloseResponse(resp) }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status code not OK")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	// now we can instantiate minio client
	minioClient, err = minio.New(minioEndpoint, accessKey, secretKey, false)
	if err != nil {
		return nil, err
	}
	if err = minioClient.MakeBucket(bucketName, region); err != nil {
		return nil, err
	}
	return &MINIOResource{
		Endpoint:     minioEndpoint,
		BucketName:   bucketName,
		Port:         minioContainer.GetPort("9000/tcp"),
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		SiteRegion:   region,
		Client:       minioClient,
		ResourceName: minioContainer.Container.Name,
	}, nil
}
