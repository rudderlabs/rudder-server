package router

import "fmt"

var RoutersManager RoutersManagerI

type RoutersManagerI interface {
	AddRouter(router *HandleT)
	PauseAll()
	Pause(destType string)
	ResumeAll()
	Resume(destType string)
	SetRoutersReady()
	AreRoutersReady() bool
}

type RoutersManagerT struct {
	Routers      map[string]*HandleT
	RoutersReady bool
}

func RoutersManagerSetup() {
	pkgLogger.Info("setting up RoutersManager.")
	rm := new(RoutersManagerT)

	RoutersManager = rm
	rm.Routers = make(map[string]*HandleT)
}

func GetRoutersManager() (RoutersManagerI, error) {
	if RoutersManager == nil {
		return nil, fmt.Errorf("routersManager is not initialized. Retry after sometime")
	}

	return RoutersManager, nil
}

func (rm *RoutersManagerT) SetRoutersReady() {
	rm.RoutersReady = true
}

func (rm *RoutersManagerT) AreRoutersReady() bool {
	return rm.RoutersReady
}

func (rm *RoutersManagerT) AddRouter(router *HandleT) {
	rm.Routers[router.destName] = router
}

func (rm *RoutersManagerT) Pause(destType string) {
	rm.Routers[destType].Pause()
}

func (rm *RoutersManagerT) Resume(destType string) {
	rm.Routers[destType].Resume()
}

func (rm *RoutersManagerT) PauseAll() {
	for k := range rm.Routers {
		rm.Pause(k)
	}
}

func (rm *RoutersManagerT) ResumeAll() {
	for k := range rm.Routers {
		rm.Resume(k)
	}
}
