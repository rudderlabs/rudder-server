package processor

import "fmt"

var ProcessorManager ProcessorManagerI

type ProcessorManagerI interface {
	Pause()
	Resume()
}

type ProcessorManagerT struct {
	Processor *HandleT
}

func ProcessorManagerSetup(processor *HandleT) {
	pkgLogger.Info("setting up ProcessorManager.")
	pm := new(ProcessorManagerT)

	ProcessorManager = pm
	pm.Processor = processor
}

func GetProcessorManager() (ProcessorManagerI, error) {
	if ProcessorManager == nil {
		return nil, fmt.Errorf("processorManager is not initialized. Retry after sometime")
	}

	return ProcessorManager, nil
}

func (pm *ProcessorManagerT) Pause() {
	pm.Processor.Pause()
}

func (pm *ProcessorManagerT) Resume() {
	pm.Processor.Resume()
}
