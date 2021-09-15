package distributed

import (
	"time"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func GetCustomerList() []string {
	return customers
}

type CustomerComputeConfig struct {
	Pghost       string
	ComputeShare float32
}

var customerComputeConfigs map[string]CustomerComputeConfig
var customers []string

func GetComputeConfig(customer string) CustomerComputeConfig {
	return customerComputeConfigs[customer]

}

func GetAllCustomersComputeConfig() map[string]CustomerComputeConfig {
	return customerComputeConfigs
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
		customerComputeConfigs["1lhWcl2VbO9c47vkvXIsDYszTpy"] = CustomerComputeConfig{
			Pghost:       "127.0.0.1",
			ComputeShare: 1,
		}
	}
}
