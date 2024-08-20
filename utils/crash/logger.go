package crash

import (
	"runtime"
	"runtime/debug"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

type PanicLogger struct {
	notifyOnce sync.Once
	logger     logger.Logger

	opts PanicWrapperOpts
}

func UsingLogger(logger logger.Logger, opts PanicWrapperOpts) *PanicLogger {
	return &PanicLogger{
		logger: logger.Child("panic"),
		opts:   opts,
	}
}

func (p *PanicLogger) Notify(team string) func() {
	return func() {
		if r := recover(); r != nil {
			p.notifyOnce.Do(func() {
				p.logger.Fatalw("Panic detected. Application will crash.",
					"stack", string(debug.Stack()),
					"panic", r,
					"team", team,
					"goRoutines", runtime.NumGoroutine(),
					"version", p.opts.AppVersion,
					"releaseStage", p.opts.ReleaseStage,
					"appType", p.opts.AppType,
				)
				logger.Sync()
				panic(r)
			})
		}
	}
}
