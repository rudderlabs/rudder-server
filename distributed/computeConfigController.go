package distributed

import (
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func GetCustomerList() []CustomerT {
	return customers
}

type CustomerT struct {
	Name        string
	WorkspaceID string
}

type CustomerComputeConfig struct {
	Pghost       string
	ComputeShare float32
}

var customerComputeConfigs map[string]CustomerComputeConfig
var customers []CustomerT
var (
	configReceived     bool
	configReceivedLock sync.RWMutex
	pkgLogger          logger.LoggerI = logger.NewLogger().Child("compute-config")
)

func GetComputeConfig(customer string) CustomerComputeConfig {
	return customerComputeConfigs[customer]
}

func GetAllCustomersComputeConfig() map[string]CustomerComputeConfig {
	return customerComputeConfigs
}

func GetWorkspaceIDs() []string {
	return make([]string, 0)
}

func Setup() {
	customerComputeConfigs = make(map[string]CustomerComputeConfig)
	rruntime.Go(func() {
		prepareComputeConfig()
	})
}

func prepareComputeConfig() {
	//TODO clean up
	time.Sleep(5 * time.Second)
	configReceivedLock.Lock()
	defer configReceivedLock.Unlock()
	configReceived = true

	if misc.IsMultiTenant() {
		customers = []CustomerT{{Name: "acorns", WorkspaceID: config.GetWorkspaceID()}, {Name: "joybird", WorkspaceID: config.GetWorkspaceID()}}
		customerComputeConfigs["acorns"] = CustomerComputeConfig{
			Pghost:       "127.0.0.1",
			ComputeShare: 0.9,
		}
		customerComputeConfigs["joybird"] = CustomerComputeConfig{
			Pghost:       "127.0.0.1",
			ComputeShare: 0.1,
		}
	} else {
		customers = []CustomerT{{Name: "acorns", WorkspaceID: config.GetWorkspaceID()}}
		customerComputeConfigs["workspaceID"] = CustomerComputeConfig{
			Pghost:       "127.0.0.1",
			ComputeShare: 1,
		}
	}
}

func WaitForComputeConfig() {
	for {
		configReceivedLock.RLock()
		if configReceived {
			configReceivedLock.RUnlock()
			break
		}
		configReceivedLock.RUnlock()
		pkgLogger.Info("Waiting for preparing compute config")
		time.Sleep(time.Second * 5)
	}
}
