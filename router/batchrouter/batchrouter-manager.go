package batchrouter

import "fmt"

var BatchRoutersManager BatchRoutersManagerI

type BatchRoutersManagerI interface {
	AddBatchRouter(router *HandleT)
	PauseAll()
	Pause(destType string)
	ResumeAll()
	Resume(destType string)
	SetBatchRoutersReady()
	AreBatchRoutersReady() bool
}

type BatchRoutersManagerT struct {
	BatchRouters      map[string]*HandleT
	BatchRoutersReady bool
}

func BatchRoutersManagerSetup() {
	pkgLogger.Info("setting up BatchRoutersManager.")
	brm := new(BatchRoutersManagerT)

	BatchRoutersManager = brm
	brm.BatchRouters = make(map[string]*HandleT)
}

func GetBatchRoutersManager() (BatchRoutersManagerI, error) {
	if BatchRoutersManager == nil {
		return nil, fmt.Errorf("routersManager is not initialized. Retry after sometime")
	}

	return BatchRoutersManager, nil
}

func (brm *BatchRoutersManagerT) SetBatchRoutersReady() {
	brm.BatchRoutersReady = true
}

func (brm *BatchRoutersManagerT) AreBatchRoutersReady() bool {
	return brm.BatchRoutersReady
}

func (brm *BatchRoutersManagerT) AddBatchRouter(brt *HandleT) {
	brm.BatchRouters[brt.destType] = brt
}

func (brm *BatchRoutersManagerT) Pause(destType string) {
	brm.BatchRouters[destType].Pause()
}

func (brm *BatchRoutersManagerT) Resume(destType string) {
	brm.BatchRouters[destType].Resume()
}

func (brm *BatchRoutersManagerT) PauseAll() {
	for k := range brm.BatchRouters {
		brm.Pause(k)
	}
}

func (brm *BatchRoutersManagerT) ResumeAll() {
	for k := range brm.BatchRouters {
		brm.Resume(k)
	}
}
