package kvstore_test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/go-redis/redis"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/kvstore"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
)

var (
	redisAddress string
	hold         bool
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.Parse()

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.Run("redis", "alpine3.14", []string{})
	if err != nil {
		log.Panicf("Could not start resource: %s", err)
	}
	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	redisAddress = fmt.Sprintf("localhost:%s", resource.GetPort("6379/tcp"))

	if err := pool.Retry(func() error {
		var err error
		client := redis.NewClient(&redis.Options{
			Addr:     redisAddress,
			Password: "",
			DB:       0,
		})
		if err != nil {
			return err
		}
		return client.Ping().Err()
	}); err != nil {
		log.Panicf("Could not connect to docker: %s", err)
	}
	code := m.Run()

	blockOnHold()

	return code
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

func TestRedisDeletion(t *testing.T) {
	inputTestData := []struct {
		key    string
		fields map[string]interface{}
	}{
		{
			key: "user:Jermaine1473336609491897794707338",
			fields: map[string]interface{}{
				"Phone": "6463633841",
				"Email": "dorowane8n285680461479465450293436@gmail.com",
			},
		},
		{
			key: "user:Mercie8221821544021583104106123",
			fields: map[string]interface{}{
				"Email": "dshirilad8536019424659691213279980@gmail.com",
			},
		},
		{
			key: "user:Claiborn443446989226249191822329",
			fields: map[string]interface{}{
				"Phone": "8782905113",
			},
		},
	}

	dest := model.Destination{
		Config: map[string]interface{}{
			"clusterMode": false,
			"address":     redisAddress,
		},
		Name: "REDIS",
	}

	manager := kvstoremanager.New(dest.Name, dest.Config)

	// inserting test data in Redis
	for _, test := range inputTestData {
		err := manager.HMSet(test.key, test.fields)
		if err != nil {
			fmt.Println("error while inserting into redis using HMSET: ", err)
		}
	}

	fieldCountBeforeDelete := make([]int, len(inputTestData))
	for i, test := range inputTestData {
		result, err := manager.HGetAll(test.key)
		if err != nil {
			fmt.Println("error while getting data from redis using HMGET: ", err)
		}
		fieldCountBeforeDelete[i] = len(result)
	}

	ctx := context.Background()
	kvstore := kvstore.KVDeleteManager{}

	deleteJob := model.Job{
		ID: 1,
		Users: []model.User{
			{
				ID: "Jermaine1473336609491897794707338",
				Attributes: map[string]string{
					"phone": "6463633841",
					"email": "dorowane8n285680461479465450293436@gmail.com",
				},
			},
		},
	}

	// deleting the last key inserted
	status := kvstore.Delete(ctx, deleteJob, dest)
	require.Equal(t, model.JobStatusComplete, status, "actual deletion status different than expected")

	fieldCountAfterDelete := make([]int, len(inputTestData))
	for i, test := range inputTestData {
		result, err := manager.HGetAll(test.key)
		if err != nil {
			fmt.Println("error while getting data from redis using HMGET: ", err)
		}
		fieldCountAfterDelete[i] = len(result)
	}

	for i := 1; i < len(inputTestData); i++ {
		require.Equal(t, fieldCountBeforeDelete[i], fieldCountAfterDelete[i], "expected no deletion for this key")
	}

	require.NotEqual(t, fieldCountBeforeDelete[0], fieldCountAfterDelete[0], "key found, expected no key")
}

func TestGetSupportedDestination(t *testing.T) {
	expectedDestinations := []string{"REDIS"}
	kvm := kvstore.KVDeleteManager{}
	actualSupportedDest := kvm.GetSupportedDestinations()
	require.Equal(t, expectedDestinations, actualSupportedDest, "actual supported destinatins different than expected")
}
