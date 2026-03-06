package crash

import (
	"net/http"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

var (
	defaultMu      sync.RWMutex
	defaultHandler panicHandler = &NOOP{}
)

type panicHandler interface {
	Notify(team string) func()
	Handler(h http.Handler) http.Handler
}

type PanicWrapperOpts struct {
	AppVersion   string
	ReleaseStage string
	AppType      string
}

func getDefault() panicHandler {
	defaultMu.RLock()
	defer defaultMu.RUnlock()
	return defaultHandler
}

func Configure(logger logger.Logger, opts PanicWrapperOpts) {
	defaultMu.Lock()
	defer defaultMu.Unlock()
	defaultHandler = UsingLogger(logger, opts)
}

func NotifyWarehouse(fn func() error) func() error {
	return func() error {
		defer getDefault().Notify("Warehouse")()
		return fn()
	}
}

func Wrapper(fn func() error) func() error {
	return func() error {
		defer getDefault().Notify("Core")()
		return fn()
	}
}

func Notify(team string) func() {
	return getDefault().Notify(team)
}

func Handler(h http.Handler) http.Handler {
	return getDefault().Handler(h)
}
