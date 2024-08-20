package crash

import (
	"net/http"
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

// UsingLogger uses the provided logger to log panics.
func UsingLogger(logger logger.Logger, opts PanicWrapperOpts) *PanicLogger {
	return &PanicLogger{
		logger: logger.Child("panic"),
		opts:   opts,
	}
}

// Notify returns a function that should be deferred to capture panics.
// It will do a Fatal log with the stack trace and panic value.
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
			})
			panic(r)
		}
	}
}

// Handler creates an http Handler that captures any panics that
// happen. It then repanics so that the default http Server panic handler can
// handle the panic too.
func (p *PanicLogger) Handler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer p.Notify("")()
		h.ServeHTTP(w, r)
	})
}
