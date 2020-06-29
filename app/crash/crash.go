// Package crash handles unexpected panics by running registered crash handlers.
package crash

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

// PanicInformation describes a panic.
type PanicInformation struct {
	ReceivedAt time.Time       `json:"received_at"`
	Error      error           `json:"error"`
	Trace      string          `json:"stack"`
	Context    context.Context `json:"context"`
}

// Handler functions handle errors received by errorSignal.
// They can be used for a number of purposes, such as logging the error, send it to external monitoring services,
// or generate crash reports
type Handler func(info PanicInformation)

// Manager will provide Defer functions that capture panics, and handles them using registered handlers.
type Manager struct {
	// array of registered handlers
	handlers []Handler

	// errorSignal is a channel that lets goroutines catch their own panics and safely forward them to one central listener.
	errorSignal chan PanicInformation

	// Report collects information that should be persisted in case of a crash
	Report *Report
}

// Default manager provides the default crash behavior, with Log and Bugsnag handlers.
var Default *Manager

// New creates a new Crash manager,
func New() *Manager {
	return &Manager{
		errorSignal: make(chan PanicInformation),
		Report: &Report{
			Metadata: make(map[string]interface{}),
		},
	}
}

func init() {
	// Setup integration with https://www.bugsnag.com error monitoring service
	SetupBugsnag()

	Default = New()
	Default.RegisterHandler(Default.Report.ReportHandler)
	Default.RegisterHandler(BugsnagHandler)
	Default.RegisterHandler(LogHandler)
}

// RegisterHandler registers a function that will run by crash module after an unhandled panic is encountered.
// Handler function will receive the PanicInformation of the panic.
func (m *Manager) RegisterHandler(handler Handler) {
	m.handlers = append(m.handlers, handler)
}

func stackTrace() string {
	stack := make([]byte, 1024*8)
	stack = stack[:runtime.Stack(stack, false)]
	return string(stack)
}

// sendError sends panic information up the Signal channel.
func (m *Manager) sendError(ctx context.Context, err interface{}) {
	m.errorSignal <- PanicInformation{
		Error:      fmt.Errorf("%v", err),
		ReceivedAt: time.Now(),
		Trace:      stackTrace(),
		Context:    ctx,
	}
}

// DeferWithContext is a deferred function that recovers from a panic and sends it
// through the Signal channel, along with provided context.
func (m *Manager) DeferWithContext(ctx context.Context) {
	if err := recover(); err != nil {
		m.sendError(ctx, err)
	}
}

// Defer is a deferred function that recovers from a panic and sends it
// through the Signal channel, using context.Background() context.
func (m *Manager) Defer() {
	if err := recover(); err != nil {
		m.sendError(context.Background(), err)
	}
}

// MonitorPanics watches the Signal channel for any panics caught by crash.Defer functions,
// and passes them to any registered crash.Handler functions.
// It will exit the application after all handlers have run.
func (m *Manager) MonitorPanics() {
	var pi PanicInformation
	for {
		pi = <-m.errorSignal
		for _, handler := range m.handlers {
			handler(pi)
		}
		os.Exit(2)
	}
}

// LogHandler will log PanicInformations Error and Stack to stderr
func LogHandler(pi PanicInformation) {
	logger.Error(pi.Error)
	logger.Error(pi.Trace)
}
