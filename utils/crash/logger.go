package crash

import (
	"net/http"
	"runtime"
	"runtime/debug"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

type panicLogger struct {
	notifyOnce sync.Once
	logger     logger.Logger

	opts PanicWrapperOpts
}

// UsingLogger uses the provided logger to log panics.
func UsingLogger(logger logger.Logger, opts PanicWrapperOpts) *panicLogger {
	return &panicLogger{
		logger: logger.Child("panic"),
		opts:   opts,
	}
}

// Notify returns a function that should be deferred to capture panics.
// It will do a Fatal log with the stack trace and panic value.
func (p *panicLogger) Notify(team string) func() {
	return func() {
		if r := recover(); r != nil {
			p.notifyOnce.Do(func() {
				p.logger.Fataln("Panic detected. Application will crash.",
					logger.NewStringField("stack", string(debug.Stack())),
					logger.NewField("panic", r),
					logger.NewStringField("team", team),
					logger.NewIntField("goRoutines", int64(runtime.NumGoroutine())),
					logger.NewStringField("version", p.opts.AppVersion),
					logger.NewStringField("releaseStage", p.opts.ReleaseStage),
					logger.NewStringField("appType", p.opts.AppType),
				)
			})
			panic(r)
		}
	}
}

// Handler creates an http Handler that captures any panics that
// happen. It then repanics so that the default http Server panic handler can
// handle the panic too.
func (p *panicLogger) Handler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer p.Notify("")()
		h.ServeHTTP(w, r)
	})
}
