package kvstore_test

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/go-redis/redis"
	"github.com/ory/dockertest"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/kvstore"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
	"github.com/stretchr/testify/require"
)

var (
	hold bool
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

	resource, err := pool.Run("redis", "latest", []string{})
	if err != nil {
		log.Panicf("Could not start resource: %s", err)
	}
	defer func() {
		if err := pool.Purge(resource); err != nil {
			log.Printf("Could not purge resource: %s \n", err)
		}
	}()

	if err := pool.Retry(func() error {
		var err error
		client := redis.NewClient(&redis.Options{
			Addr:     "localhost:6379",
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

	destName := "REDIS"
	destConfig := map[string]interface{}{
		"clusterMode": false,
		"address":     "localhost:6379",
	}
	manager := kvstoremanager.New(destName, destConfig)
	kvstore := kvstore.KVDeleteManager{
		KVStoreManager: manager,
	}

	testData := []struct {
		key    string
		fields map[string]interface{}
	}{
		{
			key: "Jermaine1473336609491897794707338",
			fields: map[string]interface{}{
				"Phone": "6463633841",
				"Email": "dorowane8n285680461479465450293436@gmail.com",
			},
		},
		{
			key: "Mercie8221821544021583104106123",
			fields: map[string]interface{}{
				"Email": "dshirilad8536019424659691213279980@gmail.com",
			},
		},
		{
			key: "Claiborn443446989226249191822329",
			fields: map[string]interface{}{
				"Phone": "8782905113",
			},
		},
	}

	//inserting test data in Redis
	for _, test := range testData {
		err := kvstore.KVStoreManager.HMSet(test.key, test.fields)
		if err != nil {
			fmt.Println("error while inserting into redis using HMSET: ", err)
		}
	}

	fieldCountBeforeDelete := make([]int, len(testData))
	for i, test := range testData {
		result, err := kvstore.KVStoreManager.HGetAll(test.key)
		if err != nil {
			fmt.Println("error while getting data from redis using HMGET: ", err)
		}
		fieldCountBeforeDelete[i] = len(result)
	}

	for _, test := range testData {
		err := kvstore.KVStoreManager.DeleteKey(test.key)
		if err != nil {
			fmt.Println("error while deleting data for key: ", testData[0].key, " from redis using Del: ", err)
		}
	}

	fieldCountAfterDelete := make([]int, len(testData))
	for i, test := range testData {
		result, err := kvstore.KVStoreManager.HGetAll(test.key)
		if err != nil {
			fmt.Println("error while getting data from redis using HMGET: ", err)
		}
		fieldCountAfterDelete[i] = len(result)
	}

	for i := 0; i < len(testData); i++ {
		require.NotEqual(t, 0, fieldCountBeforeDelete[i], "expected fields")
		require.Equal(t, 0, fieldCountAfterDelete[i], "expected no field")
	}
}
