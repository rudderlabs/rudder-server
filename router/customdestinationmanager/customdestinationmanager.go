package customdestinationmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/sony/gobreaker"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/kvstoremanager"
	"github.com/rudderlabs/rudder-server/services/streammanager"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

const (
	STREAM              = "stream"
	KV                  = "kv"
	CLIENT_EXPIRED_CODE = 721
)

var (
	ObjectStreamDestinations    []string
	KVStoreDestinations         []string
	Destinations                []string
	pkgLogger                   logger.Logger
	disableEgress               bool
	skipBackendConfigSubscriber bool
)

// DestinationManager implements the method to send the events to custom destinations
type DestinationManager interface {
	SendData(jsonData json.RawMessage, destID string) (int, string)
	BackendConfigInitialized() <-chan struct{}
}

// CustomManagerT handles this module
type CustomManagerT struct {
	destType    string
	managerType string

	stateMu  sync.RWMutex // protecting all 4 maps below
	config   map[string]backendconfig.DestinationT
	breaker  map[string]*breakerHolder
	clientMu map[string]*sync.RWMutex
	client   map[string]*clientHolder

	timeout                  time.Duration
	breakerTimeout           time.Duration
	backendConfigInitialized chan struct{}
}

// clientHolder keeps the config of a destination and corresponding producer for a stream destination
type clientHolder struct {
	config map[string]interface{}
	client interface{}
}

type breakerHolder struct {
	config    map[string]interface{}
	breaker   *gobreaker.CircuitBreaker
	lastError error
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("router").Child("customdestinationmanager")
}

func loadConfig() {
	ObjectStreamDestinations = []string{"KINESIS", "KAFKA", "AZURE_EVENT_HUB", "FIREHOSE", "EVENTBRIDGE", "GOOGLEPUBSUB", "CONFLUENT_CLOUD", "PERSONALIZE", "GOOGLESHEETS", "BQSTREAM", "LAMBDA", "GOOGLE_CLOUD_FUNCTION", "WUNDERKIND"}
	KVStoreDestinations = []string{"REDIS"}
	Destinations = append(ObjectStreamDestinations, KVStoreDestinations...)
	disableEgress = config.GetBoolVar(false, "disableEgress")
}

// newClient delegates the call to the appropriate manager
func (customManager *CustomManagerT) newClient(destID string) error {
	destination := customManager.config[destID]
	destConfig := destination.Config
	_, err := customManager.breaker[destID].breaker.Execute(func() (interface{}, error) {
		var customDestination *clientHolder
		var err error

		switch customManager.managerType {
		case STREAM:
			var producer interface{}
			producer, err = streammanager.NewProducer(&destination, common.Opts{
				Timeout: customManager.timeout,
			})
			if err == nil {
				customDestination = &clientHolder{
					config: destConfig,
					client: producer,
				}
				customManager.client[destID] = customDestination
			}
		case KV:
			kvManager := kvstoremanager.New(customManager.destType, destConfig)
			customDestination = &clientHolder{
				config: destConfig,
				client: kvManager,
			}
			customManager.client[destID] = customDestination
		default:
			return nil, fmt.Errorf("no provider configured for Custom Destination Manager")
		}
		customManager.breaker[destID].lastError = err
		return nil, err
	})
	if errors.Is(err, gobreaker.ErrOpenState) && customManager.breaker[destID].lastError != nil {
		return fmt.Errorf("%s, last error: %w", err, customManager.breaker[destID].lastError)
	}
	return err
}

func (customManager *CustomManagerT) send(jsonData json.RawMessage, client interface{}, config map[string]interface{}) (int, string) {
	var statusCode int
	var respBody string
	switch customManager.managerType {
	case STREAM:
		// If client is not properly initialized then it won't reach here
		streamProducer, _ := client.(common.StreamProducer)
		statusCode, _, respBody = streamProducer.Produce(jsonData, config)
	case KV:
		var err error
		kvManager, _ := client.(kvstoremanager.KVStoreManager)

		switch {
		case kvManager.ShouldSendDataAsJSON(config):
			_, err = kvManager.SendDataAsJSON(jsonData, config)
		case kvstoremanager.IsHSETCompatibleEvent(jsonData):
			// if the event supports HSET operation then use HSET
			hash, key, value := kvstoremanager.ExtractHashKeyValueFromEvent(jsonData)
			err = kvManager.HSet(hash, key, value)
		default:
			key, fields := kvstoremanager.EventToKeyValue(jsonData)
			err = kvManager.HMSet(key, fields)
		}

		statusCode = kvManager.StatusCode(err)
		if err != nil {
			respBody = err.Error()
		}
	default:
		return 404, "No provider configured for Custom Destination Manager"
	}

	return statusCode, respBody
}

// SendData gets the producer from streamDestinationsMap and sends data
func (customManager *CustomManagerT) SendData(jsonData json.RawMessage, destID string) (int, string) {
	if disableEgress {
		return 200, `200: outgoing disabled`
	}

	customManager.stateMu.RLock()
	clientLock, ok := customManager.clientMu[destID]
	customManager.stateMu.RUnlock()
	if !ok {
		return 500, fmt.Sprintf("[CDM %s] Unexpected state: Lock missing for %s. Config might not have been updated. Please wait for a min before sending events.", customManager.destType, destID)
	}

	clientLock.RLock()
	customDestination, ok := customManager.client[destID]

	if !ok {
		clientLock.RUnlock()
		clientLock.Lock()
		var err error
		if _, ok := customManager.client[destID]; !ok {
			err = customManager.newClient(destID)
		}
		clientLock.Unlock()
		if err != nil {
			return 400, fmt.Sprintf("[CDM %s] Unable to create client for %s %s", customManager.destType, destID, err.Error())
		}
		clientLock.RLock()
		customDestination = customManager.client[destID]
	}
	clientLock.RUnlock()

	respStatusCode, respBody := customManager.send(jsonData, customDestination.client, customDestination.config)

	if respStatusCode == CLIENT_EXPIRED_CODE {
		clientLock.Lock()
		err := customManager.refreshClient(destID)
		clientLock.Unlock()
		if err != nil {
			return 400, fmt.Sprintf("[CDM %s] Unable to refresh client for %s %s", customManager.destType, destID, err.Error())
		}
		clientLock.RLock()
		customDestination = customManager.client[destID]
		clientLock.RUnlock()
		respStatusCode, respBody = customManager.send(jsonData, customDestination.client, customDestination.config)
	}

	return respStatusCode, respBody
}

func (customManager *CustomManagerT) close(destID string) {
	customDestination := customManager.client[destID]
	switch customManager.managerType {
	case STREAM:
		streamProducer, _ := customDestination.client.(common.StreamProducer)
		_ = streamProducer.Close()
	case KV:
		kvManager, _ := customDestination.client.(kvstoremanager.KVStoreManager)
		_ = kvManager.Close()
	}
	delete(customManager.client, destID)
}

func (customManager *CustomManagerT) refreshClient(destID string) error {
	customDestination, ok := customManager.client[destID]

	if ok {
		pkgLogger.Infof("[CDM %s] [Token Expired] Closing Existing client for destination id: %s", customManager.destType, destID)
		switch customManager.managerType {
		case STREAM:
			streamProducer, _ := customDestination.client.(common.StreamProducer)
			streamProducer.Close()
		case KV:
			kvManager, _ := customDestination.client.(kvstoremanager.KVStoreManager)
			_ = kvManager.Close()
		}
	}
	err := customManager.newClient(destID)
	if err != nil {
		pkgLogger.Errorf("[CDM %s] [Token Expired] Error while creating new client for destination id: %s", customManager.destType, destID)
		return err
	}
	pkgLogger.Infof("[CDM %s] [Token Expired] Created new client for destination id: %s", customManager.destType, destID)
	return nil
}

func (customManager *CustomManagerT) onNewDestination(destination backendconfig.DestinationT) error { // skipcq: CRT-P0003
	customManager.stateMu.Lock()
	defer customManager.stateMu.Unlock()
	var err error
	clientLock, ok := customManager.clientMu[destination.ID]
	if !ok {
		clientLock = &sync.RWMutex{}
		customManager.clientMu[destination.ID] = clientLock
	}
	clientLock.Lock()
	defer clientLock.Unlock()
	customManager.config[destination.ID] = destination
	err = customManager.onConfigChange(destination.ID, destination.Config)
	return err
}

func (customManager *CustomManagerT) onConfigChange(destID string, newDestConfig map[string]interface{}) error {
	_, hasOpenClient := customManager.client[destID]
	breaker, hasCircuitBreaker := customManager.breaker[destID]
	if hasCircuitBreaker {
		hasDestConfigChanged := !reflect.DeepEqual(
			customManager.genComparisonConfig(breaker.config),
			customManager.genComparisonConfig(newDestConfig),
		)

		if !hasDestConfigChanged {
			return nil
		}
		if hasOpenClient {
			pkgLogger.Infof("[CDM %s] Config changed. Closing Existing client for destination: %s", customManager.destType, destID)
			customManager.close(destID)
		}
	}
	customManager.breaker[destID] = &breakerHolder{
		config: newDestConfig,
		breaker: gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    destID,
			Timeout: customManager.breakerTimeout,
		}),
	}
	if err := customManager.newClient(destID); err != nil {
		pkgLogger.Errorf("[CDM %s] DestID: %s, Error while creating new customer client: %v", customManager.destType, destID, err)
		return err
	}
	pkgLogger.Infof("[CDM %s] DestID: %s, Created new client", customManager.destType, destID)
	return nil
}

type Opts struct {
	Timeout time.Duration
}

// New returns CustomdestinationManager
func New(destType string, o Opts) DestinationManager {
	if slices.Contains(Destinations, destType) {

		managerType := STREAM
		if slices.Contains(KVStoreDestinations, destType) {
			managerType = KV
		}

		customManager := &CustomManagerT{
			timeout:                  o.Timeout,
			destType:                 destType,
			managerType:              managerType,
			client:                   make(map[string]*clientHolder),
			clientMu:                 make(map[string]*sync.RWMutex),
			config:                   make(map[string]backendconfig.DestinationT),
			breaker:                  make(map[string]*breakerHolder),
			backendConfigInitialized: make(chan struct{}),
		}

		if !skipBackendConfigSubscriber {
			rruntime.Go(func() {
				customManager.backendConfigSubscriber()
			})
		} else {
			close(customManager.backendConfigInitialized)
		}

		return customManager
	}

	return nil
}

func (customManager *CustomManagerT) BackendConfigInitialized() <-chan struct{} {
	return customManager.backendConfigInitialized
}

func (customManager *CustomManagerT) backendConfigSubscriber() {
	var once sync.Once
	ch := backendconfig.DefaultBackendConfig.Subscribe(context.TODO(), "backendConfig")
	for data := range ch {
		config := data.Data.(map[string]backendconfig.ConfigT)
		for _, wConfig := range config {
			for _, source := range wConfig.Sources {
				for _, destination := range source.Destinations {
					if destination.DestinationDefinition.Name == customManager.destType && destination.Enabled {
						err := customManager.onNewDestination(destination)
						if err != nil {
							pkgLogger.Errorf(
								"[CDM %s] Error while subscribing to BackendConfig for destination id: %s",
								customManager.destType, destination.ID,
							)
						}
					}
				}
			}
		}
		once.Do(func() {
			close(customManager.backendConfigInitialized)
		})
	}
}

func (customManager *CustomManagerT) genComparisonConfig(config interface{}) map[string]interface{} {
	relevantConfigs := make(map[string]interface{})
	configMap, ok := config.(map[string]interface{})
	if !ok {
		pkgLogger.Errorf("[CDM] DestType: %s. Destination config is not of expected type (map). Returning empty map", customManager.destType)
		return map[string]interface{}{}
	}

	for k, v := range configMap {
		if k != "eventDeliveryTS" && k != "eventDelivery" {
			relevantConfigs[k] = v
		}
	}
	return relevantConfigs
}
