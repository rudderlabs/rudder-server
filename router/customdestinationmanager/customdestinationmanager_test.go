package customdestinationmanager

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	mock_streammanager "github.com/rudderlabs/rudder-server/mocks/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/services/streammanager/lambda"
	"github.com/rudderlabs/rudder-server/utils/logger"
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
	const ()

	// init various packages
	initCustomerManager()
	// don't let manager subscribe to backend-config
	skipBackendConfigSubscriber = true

	// set a custom circuit breaker timeout (default is 60 seconds)
	breakerTimeout := 100 * time.Millisecond

	testcases := []struct {
		dest        backendconfig.DestinationT
		expectedErr error
	}{
		{
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "KAFKA",
				},
				Config: map[string]interface{}{
					"hostName": "unknown.example.com", // <- minimum valid configuration
					"port":     "9999",
					"topic":    "topic",
				},
				Enabled: true,
			},
			expectedErr: fmt.Errorf("could not ping: could not dial any of the addresses tcp/unknown.example.com:9999:"),
		},
		{
			dest: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: "REDIS",
				},
				Config: map[string]interface{}{
					"hostName": "unknown.example.com",
				},
				Enabled: true,
			},
			expectedErr: fmt.Errorf("dial tcp [::1]:6379: connect: connection refused"),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.dest.DestinationDefinition.Name, func(t *testing.T) {
			tc.dest.ID = uuid.New().String()
			tc.dest.Name = tc.dest.DestinationDefinition.Name

			normalError := tc.expectedErr.Error()
			breakerError := fmt.Sprintf("circuit breaker is open, last error: %s", normalError)

			manager := New(tc.dest.DestinationDefinition.Name, Opts{Timeout: 1 * time.Microsecond}).(*CustomManagerT)
			manager.breakerTimeout = breakerTimeout

			// during the first 6 failed attempts the circuit is closed
			count := 1
			newDestination(t, manager, tc.dest, count, normalError)
			count++
			for ; count <= 6; count++ {
				newClientAttempt(t, manager, tc.dest.ID, count, normalError)
			}
			// after the 6th failed attempt the circuit opens
			newClientAttempt(t, manager, tc.dest.ID, count, breakerError)
			count++
			<-time.After(breakerTimeout)
			// after the circuit breaker's timeout passes the circuit becomes half-open
			newClientAttempt(t, manager, tc.dest.ID, count, normalError)
			count++
			// after another failure the circuit opens again
			newClientAttempt(t, manager, tc.dest.ID, count, breakerError)
			count++

			// sending the same destination again should not try to create a client
			assert.Nil(t, manager.onNewDestination(tc.dest))
			// and shouldn't reset the circuit breaker either
			newClientAttempt(t, manager, tc.dest.ID, count, breakerError)

			t.Run("reset breaker on modified destination", func(t *testing.T) {
				// sending a modified destination should reset the circuit breaker
				modifiedDest := backendconfig.DestinationT{
					ID:                    tc.dest.ID,
					Name:                  tc.dest.Name,
					DestinationDefinition: tc.dest.DestinationDefinition,
					Enabled:               true,
					Config:                map[string]interface{}{},
				}
				for k, v := range tc.dest.Config {
					modifiedDest.Config[k] = v
				}
				modifiedDest.Config["modified"] = true

				count := 1
				newDestination(t, manager, modifiedDest, count, normalError)
				count++
				for ; count <= 6; count++ {
					newClientAttempt(t, manager, tc.dest.ID, count, normalError)
				}
				// after the 6th attempt the new circuit opens
				newClientAttempt(t, manager, tc.dest.ID, count, breakerError)
			})
		})
	}
}

func newDestination(t *testing.T, manager *CustomManagerT, dest backendconfig.DestinationT, attempt int, errorString string) { // skipcq: CRT-P0003
	err := manager.onNewDestination(dest)
	assert.NotNil(t, err, "it should return an error for attempt no %d", attempt)
	assert.True(t, strings.HasPrefix(err.Error(), errorString), fmt.Sprintf("error %q should start with %q for attempt no %d", err.Error(), errorString, attempt))
}

func newClientAttempt(t *testing.T, manager *CustomManagerT, destId string, attempt int, errorString string) {
	err := manager.newClient(destId)
	assert.NotNil(t, err, "it should return an error for attempt no %d", attempt)
	t.Log(err.Error())
	assert.True(t, strings.HasPrefix(err.Error(), errorString), fmt.Sprintf("error %q should start with %q for attempt no %d", err.Error(), errorString, attempt))
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
