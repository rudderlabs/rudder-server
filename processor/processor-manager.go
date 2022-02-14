package processor

import (
	"context"
	"fmt"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var (
	manager *HandleT
)

//type ProcessorManagerI interface {
//	Pause()
//	Resume()
//}

//type ProcessorManagerT struct {
//	Processor *HandleT
//}

func ProcessorManagerSetup(processor *HandleT) {
	pkgLogger.Info("setting up ProcessorManager.")
	//pm := new(ProcessorManagerT)

	manager = processor
	//pm.Processor = processor
}

func GetProcessorManager() (*HandleT, error) {
	if manager == nil {
		return nil, fmt.Errorf("processorManager is not initialized. Retry after sometime")
	}

	return manager, nil
}

//func (pm *ProcessorManagerT) Pause() {
//	pm.Processor.Pause()
//}
//
//func (pm *ProcessorManagerT) Resume() {
//	pm.Processor.Resume()
//}

type Manager interface {
	Start(ctx context.Context)
	Shutdown()
	Setup(backendConfig backendconfig.BackendConfig, gatewayDB jobsdb.JobsDB, routerDB jobsdb.JobsDB,
		batchRouterDB jobsdb.JobsDB, errorDB jobsdb.JobsDB, clearDB *bool, reporting types.ReportingI)
}
