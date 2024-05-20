package customdestinationmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/redis"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_kvstoremanager "github.com/rudderlabs/rudder-server/mocks/services/kvstoremanager"
	mock_streammanager "github.com/rudderlabs/rudder-server/mocks/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/services/streammanager/lambda"
)

var once sync.Once

func initCustomerManager() {
	once.Do(func() {
		config.Reset()
		Init()
		logger.Reset()
		kafka.Init()
		skipBackendConfigSubscriber = true
	})
}

func TestCircuitBreaker(t *testing.T) {
	const (
		normalError  = "could not ping: could not dial any of the addresses"
		breakerError = "circuit breaker is open, last error: could not ping: could not dial any of the addresses"
	)

	// init various packages
	initCustomerManager()
	// don't let manager subscribe to backend-config
	skipBackendConfigSubscriber = true

	// set a custom circuit breaker timeout (default is 60 seconds)
	breakerTimeout := 1 * time.Second

	manager := New("KAFKA", Opts{Timeout: 1 * time.Microsecond}).(*CustomManagerT)
	manager.breakerTimeout = breakerTimeout

	dest := getDestConfig()
	// during the first 6 failed attempts the circuit is closed
	count := 1
	newDestination(t, manager, dest, count, normalError)
	count++
	for ; count <= 6; count++ {
		newClientAttempt(t, manager, dest.ID, count, normalError)
	}
	// after the 6th failed attempt the circuit opens
	newClientAttempt(t, manager, dest.ID, count, breakerError)
	count++
	<-time.After(breakerTimeout)
	// after the circuit breaker's timeout passes the circuit becomes half-open
	newClientAttempt(t, manager, dest.ID, count, normalError)
	count++
	// after another failure the circuit opens again
	newClientAttempt(t, manager, dest.ID, count, breakerError)
	count++

	// sending the same destination again should not try to create a client
	assert.Nil(t, manager.onNewDestination(dest))
	// and shouldn't reset the circuit breaker either
	newClientAttempt(t, manager, dest.ID, count, breakerError)

	// sending a modified destination should reset the circuit breaker
	dest = getDestConfig()
	dest.Config["modified"] = true
	count = 1
	newDestination(t, manager, dest, count, normalError)
	count++
	for ; count <= 6; count++ {
		newClientAttempt(t, manager, dest.ID, count, normalError)
	}
	// after the 6th attempt the new circuit opens
	newClientAttempt(t, manager, dest.ID, count, breakerError)
}

func newDestination(t *testing.T, manager *CustomManagerT, dest backendconfig.DestinationT, attempt int, errorString string) { // skipcq: CRT-P0003
	err := manager.onNewDestination(dest)
	assert.NotNil(t, err, "it should return an error for attempt no %d", attempt)
	assert.True(t, strings.HasPrefix(err.Error(), errorString), fmt.Sprintf("error %s should start with %s for attempt no %d", err.Error(), errorString, attempt))
}

func newClientAttempt(t *testing.T, manager *CustomManagerT, destId string, attempt int, errorString string) {
	err := manager.newClient(destId)
	assert.NotNil(t, err, "it should return an error for attempt no %d", attempt)
	assert.True(t, strings.HasPrefix(err.Error(), errorString), fmt.Sprintf("error %s should start with %s for attempt no %d", err.Error(), errorString, attempt))
}

func getDestConfig() backendconfig.DestinationT {
	return backendconfig.DestinationT{
		ID:   "test",
		Name: "test",
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: "KAFKA",
		},
		Config: map[string]interface{}{
			"hostName":      "unknown.example.com",
			"port":          "9999",
			"topic":         "topic",
			"sslEnabled":    true,
			"useSASL":       true,
			"caCertificate": "",
			"saslType":      "plain",
			"username":      "username",
			"password":      "password",
		},
		Enabled: true,
	}
}

func TestSendDataWithStreamDestination(t *testing.T) {
	initCustomerManager()

	customManager := New("LAMBDA", Opts{}).(*CustomManagerT)
	someDestination := backendconfig.DestinationT{
		ID: "someDestinationID1",
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: "LAMBDA",
		},
		Config: map[string]interface{}{
			"region": "someRegion",
		},
	}
	err := customManager.onNewDestination(someDestination)
	assert.Nil(t, err)
	assert.NotNil(t, customManager.client[someDestination.ID])
	assert.IsType(t, &lambda.LambdaProducer{}, customManager.client[someDestination.ID].client)

	ctrl := gomock.NewController(t)
	mockProducer := mock_streammanager.NewMockStreamProducer(ctrl)
	customManager.client[someDestination.ID].client = mockProducer
	event := json.RawMessage{}
	mockProducer.EXPECT().Produce(event, someDestination.Config).Times(1)
	customManager.SendData(event, someDestination.ID)
}

type transformedResponseJSON struct {
	Message map[string]interface{} `json:"message"`
	UserId  string                 `json:"userId"`
}

type sendDataResponse struct {
	statusCode int
	err        string
}

type redisTc struct {
	description              string
	transformedResponse      transformedResponseJSON // Response obtained from transformer
	redisImgRepo             string
	redisImgTag              string
	supportJsonModule        bool
	expectedSendDataResponse sendDataResponse
}

var redisJSONTestCases = []redisTc{
	{
		description: "When JSON module is loaded into Redis, should set into redis and fetching the data should be successful",
		transformedResponse: transformedResponseJSON{
			Message: map[string]interface{}{
				"key": "user:myuser-id",
				"value": map[string]interface{}{
					"key": "someKey",
					"fields": map[string]interface{}{
						"field1": "value1",
						"field2": 2,
					},
				},
			},
			UserId: "myuser-id",
		},
		redisImgRepo:      "redis/redis-stack-server",
		supportJsonModule: true,
		redisImgTag:       "latest",
		expectedSendDataResponse: sendDataResponse{
			statusCode: 200,
		},
	},
	{
		description: "When JSON module not loaded into Redis, should not be able to set into redis",
		transformedResponse: transformedResponseJSON{
			Message: map[string]interface{}{
				"key": "user:myuser-id",
				"value": map[string]interface{}{
					"key": "someKey",
					"fields": map[string]interface{}{
						"field1": "value1",
						"field2": "value2",
					},
				},
			},
			UserId: "myuser-id",
		},
		redisImgRepo: "redis",
		redisImgTag:  "alpine3.19",
		expectedSendDataResponse: sendDataResponse{
			statusCode: 500,
			err:        "ERR unknown command 'JSON.SET', with args beginning with:",
		},
	},
}

func TestKVManagerInvocations(t *testing.T) {
	initCustomerManager()
	customManager := New("REDIS", Opts{}).(*CustomManagerT)
	someDestination := backendconfig.DestinationT{
		ID: "someDestinationID1",
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: "REDIS",
		},
	}
	err := customManager.onNewDestination(someDestination)
	assert.Nil(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("HSET", func(t *testing.T) {
		event := json.RawMessage(`{
			"message": {
				"key": "someKey",
				"value" : "someValue",
				"hash": "someHash"
			}
		  }
		`)
		ctrl := gomock.NewController(t)
		mockKVStoreManager := mock_kvstoremanager.NewMockKVStoreManager(ctrl)
		mockKVStoreManager.EXPECT().ShouldSendDataAsJSON(someDestination.Config).Return(false).Times(1)
		mockKVStoreManager.EXPECT().HSet("someHash", "someKey", "someValue").Times(1)
		mockKVStoreManager.EXPECT().StatusCode(nil).Times(1)
		customManager.send(event, mockKVStoreManager, someDestination.Config)
	})

	t.Run("HMSET", func(t *testing.T) {
		event := json.RawMessage(`{
			"message": {
				"key": "someKey",
				"fields" : {
					"field1": "value1",
					"field2": "value2"
				}
			}
		  }
		`)
		mockKVStoreManager := mock_kvstoremanager.NewMockKVStoreManager(ctrl)
		mockKVStoreManager.EXPECT().ShouldSendDataAsJSON(someDestination.Config).Return(false).Times(1)
		mockKVStoreManager.EXPECT().HMSet("someKey", map[string]interface{}{
			"field1": "value1",
			"field2": "value2",
		}).Times(1)
		mockKVStoreManager.EXPECT().StatusCode(nil).Times(1)
		customManager.send(event, mockKVStoreManager, someDestination.Config)
	})
}

func TestRedisManagerForJSONStorage(t *testing.T) {
	initCustomerManager()
	customManager := New("REDIS", Opts{}).(*CustomManagerT)
	someDestination := backendconfig.DestinationT{
		ID: "someDestinationID1",
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: "REDIS",
		},
	}
	err := customManager.onNewDestination(someDestination)
	require.NoError(t, err)

	for _, tc := range redisJSONTestCases {
		t.Run(tc.description, func(t *testing.T) {
			opts := []redis.Option{
				redis.WithRepository(tc.redisImgRepo),
				redis.WithTag(tc.redisImgTag),
				redis.WithCmdArg("--protected-mode", "no"),
			}
			if tc.supportJsonModule {
				opts = append(opts, redis.WithCmdArg("--loadmodule", "/opt/redis-stack/lib/rejson.so"))
			}
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			redisRsrc, err := redis.Setup(context.Background(), pool, t, opts...)
			require.NoError(t, err)
			// redisAddr, destroy := testutils.StartRedis(t, tc.redisImgRepo, tc.redisImgTag)
			// defer destroy()
			event, err := json.Marshal(tc.transformedResponse)
			require.NoError(t, err)
			config := map[string]interface{}{
				"useJSONModule": true,
				"address":       redisRsrc.Addr,
				"db":            0,
				"clusterMode":   false,
			}
			kvMgr := kvstoremanager.NewRedisManager(config)
			db := kvMgr.GetClient()

			stCd, er := customManager.send(event, kvMgr, config)
			if er != "" {
				t.Logf("Error: %s\n", er)
				require.Contains(t, er, tc.expectedSendDataResponse.err)
				return
			}
			require.Equal(t, tc.expectedSendDataResponse.statusCode, stCd)

			msgMap := gjson.GetBytes(event, "message.value").String()
			key := gjson.GetBytes(event, "message.key").String()
			res, err := db.JSONGet(context.Background(), key).Result()
			require.NoError(t, err)
			require.Equal(t, msgMap, res)
		})
	}
}

func TestRedisMgrForMultipleJSONsSameKey(t *testing.T) {
	initCustomerManager()
	customManager := New("REDIS", Opts{}).(*CustomManagerT)
	someDestination := backendconfig.DestinationT{
		ID: "someDestinationID1",
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: "REDIS",
		},
	}
	err := customManager.onNewDestination(someDestination)
	assert.Nil(t, err)

	transformedResponse := transformedResponseJSON{
		Message: map[string]interface{}{
			"key":  "user:myuser-id",
			"path": "mode-1",
			"value": map[string]interface{}{
				"key": "someKey",
				"fields": map[string]interface{}{
					"field1": "value1",
					"field2": 2,
				},
			},
		},
		UserId: "myuser-id",
	}

	t.Run("When JSON module is loaded into Redis, path is mentioned and key is not present in Redis, should set into redis and fetching the data should be successful", func(t *testing.T) {
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		redisRsrc, err := redis.Setup(context.Background(), pool, t,
			redis.WithRepository("redis/redis-stack-server"),
			redis.WithTag("latest"),
			redis.WithCmdArg("--protected-mode", "no"),
			redis.WithCmdArg("--loadmodule", "/opt/redis-stack/lib/rejson.so"),
		)
		require.NoError(t, err)
		event, err := json.Marshal(transformedResponse)
		require.NoError(t, err)
		config := map[string]interface{}{
			"useJSONModule": true,
			"address":       redisRsrc.Addr,
			"db":            0,
			"clusterMode":   false,
		}
		kvMgr := kvstoremanager.NewRedisManager(config)
		db := kvMgr.GetClient()

		stCd, _ := customManager.send(event, kvMgr, config)
		require.Equal(t, http.StatusOK, stCd)
		v, err := db.JSONGet(context.Background(), "user:myuser-id", "$.mode-1").Result()
		require.NoError(t, err)
		require.JSONEq(t, `[{"key":"someKey","fields":{"field1":"value1","field2":2}}]`, v)
	})

	t.Run("When JSON module is loaded into Redis, path is mentioned and key is present in Redis, should set into redis and fetching the data should be successful", func(t *testing.T) {
		// uses a sensible default on windows (tcp/http) and linux/osx (socket)
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		redisRsrc, err := redis.Setup(context.Background(), pool, t,
			redis.WithRepository("redis/redis-stack-server"),
			redis.WithTag("latest"),
			redis.WithCmdArg("--protected-mode", "no"),
			redis.WithCmdArg("--loadmodule", "/opt/redis-stack/lib/rejson.so"),
		)
		require.NoError(t, err)
		event, err := json.Marshal(transformedResponse)
		require.NoError(t, err)
		config := map[string]interface{}{
			"useJSONModule": true,
			"address":       redisRsrc.Addr,
			"db":            0,
			"clusterMode":   false,
		}
		kvMgr := kvstoremanager.NewRedisManager(config)
		db := kvMgr.GetClient()
		ctx := context.Background()

		_, setErr := db.JSONSet(ctx, "user:myuser-id", "$", `{"mode-in":{"a":1,"size":"LM"}}`).Result()
		require.Nil(t, setErr)

		stCd, _ := customManager.send(event, kvMgr, config)
		require.Equal(t, http.StatusOK, stCd)

		firstVal, err := db.JSONGet(ctx, "user:myuser-id", "$.mode-in").Result()
		require.NoError(t, err)
		require.JSONEq(t, `[{"a":1,"size":"LM"}]`, firstVal)

		v, err2 := db.JSONGet(ctx, "user:myuser-id", "$.mode-1").Result()
		require.NoError(t, err2)
		require.JSONEq(t, `[{"key":"someKey","fields":{"field1":"value1","field2":2}}]`, v)
	})
}
