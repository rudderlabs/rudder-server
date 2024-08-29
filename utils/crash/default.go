package crash

import (
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

var Default panicHandler = &NOOP{}

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
	Default = UsingLogger(logger, opts)
}

func NotifyWarehouse(fn func() error) func() error {
	return func() error {
		defer Default.Notify("Warehouse")()
		return fn()
	}
}

func Wrapper(fn func() error) func() error {
	return func() error {
		defer Default.Notify("Core")()
		return fn()
	}
}

func Notify(team string) func() {
	return Default.Notify(team)
}

func Handler(h http.Handler) http.Handler {
	return Default.Handler(h)
}
