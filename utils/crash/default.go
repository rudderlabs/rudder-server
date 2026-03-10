package crash

import (
	"net/http"
	"sync/atomic"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

var defaultHandler atomic.Pointer[panicHandler]

func init() {
	var noop panicHandler = &NOOP{}
	defaultHandler.Store(&noop)
}

func getDefault() panicHandler {
	return *defaultHandler.Load()
}

type panicHandler interface {
	Notify(team string) func()
	Handler(h http.Handler) http.Handler
}

type PanicWrapperOpts struct {
	AppVersion   string
	ReleaseStage string
	AppType      string
}

func Configure(logger logger.Logger, opts PanicWrapperOpts) {
	var h panicHandler = UsingLogger(logger, opts)
	defaultHandler.Store(&h)
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
