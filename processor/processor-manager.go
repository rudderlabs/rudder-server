package processor

import "fmt"

var (
	globalManager *managerImpl
)

type managerImpl struct {
	Processor *HandleT
}

func ManagerSetup(processor *HandleT) {
	pkgLogger.Info("setting up ProcessorManager.")
	pm := new(managerImpl)

	globalManager = pm
	pm.Processor = processor
}

func GetProcessorManager() (*managerImpl, error) {
	if globalManager == nil {
		return nil, fmt.Errorf("processorManager is not initialized. Retry after sometime")
	}
	return globalManager, nil
}

func (pm *managerImpl) Pause() {
	pm.Processor.Pause()
}

func (pm *managerImpl) Resume() {
	pm.Processor.Resume()
}
