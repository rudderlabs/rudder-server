package customdestinationmanager

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_kvstoremanager "github.com/rudderlabs/rudder-server/mocks/services/kvstoremanager"
	mock_streammanager "github.com/rudderlabs/rudder-server/mocks/services/streammanager/common"
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
