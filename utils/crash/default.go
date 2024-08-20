package crash

import (
	"github.com/rudderlabs/rudder-go-kit/logger"
)

var Default panicHandler = &NOOP{}

type panicHandler interface {
	Notify(team string) func()
}

type PanicWrapperOpts struct {
	AppVersion   string
	ReleaseStage string
	AppType      string
}

func Configure(logger logger.Logger, opts PanicWrapperOpts) {
	Default = UsingLogger(logger, opts)
}

func WrapperForWarehouse(fn func() error) func() error {
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
