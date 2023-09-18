/*
Logger Interface Use instance of logger instead of exported functions

usage example

   import (
	   "errors"
	   "github.com/rudderlabs/rudder-go-kit/config"
	   "github.com/rudderlabs/rudder-go-kit/logger"
   )

   var c = config.New()
   var loggerFactory = logger.NewFactory(c)
   var log logger.Logger = loggerFactory.NewLogger()
   ...
   log.Error(...)

or if you want to use the default logger factory (not advised):

   var log logger.Logger = logger.NewLogger()
   ...
   log.Error(...)

*/
//go:generate mockgen -destination=mock_logger/mock_logger.go -package mock_logger github.com/rudderlabs/rudder-go-kit/logger Logger
package logger

import (
	"bytes"
	"io"
	"net/http"
	"runtime"
	"strings"

	"go.uber.org/zap"
)

/*
Using levels(like Debug, Info etc.) in logging is a way to categorize logs based on their importance.
The idea is to have the option of running the application in different logging levels based on
how verbose we want the logging to be.
For example, using Debug level of logging, logs everything and it might slow the application, so we run application
in DEBUG level for local development or when we want to look through the entire flow of events in detail.
We use 4 logging levels here Debug, Info, Warn and Error.
*/

type Logger interface {
	// IsDebugLevel Returns true is debug lvl is enabled
	IsDebugLevel() bool

	// Debug level logging. Most verbose logging level.
	Debug(args ...interface{})

	// Debugf does debug level logging similar to fmt.Printf. Most verbose logging level
	Debugf(format string, args ...interface{})

	// Debugw does debug level structured logging. Most verbose logging level
	Debugw(msg string, keysAndValues ...interface{})

	// Info level logging. Use this to log the state of the application. Dont use Logger.Info in the flow of individual events. Use Logger.Debug instead.
	Info(args ...interface{})

	// Infof does info level logging similar to fmt.Printf. Use this to log the state of the application. Dont use Logger.Info in the flow of individual events. Use Logger.Debug instead.
	Infof(format string, args ...interface{})

	// Infof does info level structured logging. Use this to log the state of the application. Dont use Logger.Info in the flow of individual events. Use Logger.Debug instead.
	Infow(msg string, keysAndValues ...interface{})

	// Warn level logging. Use this to log warnings
	Warn(args ...interface{})

	// Warnf does warn level logging similar to fmt.Printf. Use this to log warnings
	Warnf(format string, args ...interface{})

	// Warnf does warn level structured logging. Use this to log warnings
	Warnw(msg string, keysAndValues ...interface{})

	// Error level logging. Use this to log errors which dont immediately halt the application.
	Error(args ...interface{})

	// Errorf does error level logging similar to fmt.Printf. Use this to log errors which dont immediately halt the application.
	Errorf(format string, args ...interface{})

	// Errorf does error level structured logging. Use this to log errors which dont immediately halt the application.
	Errorw(msg string, keysAndValues ...interface{})

	// Fatal level logging. Use this to log errors which crash the application.
	Fatal(args ...interface{})

	// Fatalf does fatal level logging similar to fmt.Printf. Use this to log errors which crash the application.
	Fatalf(format string, args ...interface{})

	// Fatalf does fatal level structured logging. Use this to log errors which crash the application.
	Fatalw(format string, keysAndValues ...interface{})

	LogRequest(req *http.Request)

	// Child creates a child logger with the given name
	Child(s string) Logger

	// With adds the provided key value pairs to the logger context
	With(args ...interface{}) Logger
}

type logger struct {
	logConfig *factoryConfig
	name      string
	zap       *zap.SugaredLogger
	parent    *logger
}

func (l *logger) Child(s string) Logger {
	if s == "" {
		return l
	}
	cp := *l
	cp.parent = l
	if l.name == "" {
		cp.name = s
	} else {
		cp.name = strings.Join([]string{l.name, s}, ".")
	}
	if l.logConfig.enableNameInLog {
		cp.zap = l.zap.Named(s)
	}
	return &cp
}

// With adds a variadic number of fields to the logging context. It accepts a mix of strongly-typed Field objects and loosely-typed key-value pairs. When processing pairs, the first element of the pair is used as the field key and the second as the field value.
func (l *logger) With(args ...interface{}) Logger {
	cp := *l
	cp.zap = l.zap.With(args...)
	return &cp
}

func (l *logger) getLoggingLevel() int {
	return l.logConfig.getOrSetLogLevel(l.name, l.parent.getLoggingLevel)
}

// IsDebugLevel Returns true is debug lvl is enabled
func (l *logger) IsDebugLevel() bool {
	return levelDebug >= l.getLoggingLevel()
}

// Debug level logging.
// Most verbose logging level.
func (l *logger) Debug(args ...interface{}) {
	if levelDebug >= l.getLoggingLevel() {
		l.zap.Debug(args...)
	}
}

// Info level logging.
// Use this to log the state of the application. Dont use Logger.Info in the flow of individual events. Use Logger.Debug instead.
func (l *logger) Info(args ...interface{}) {
	if levelInfo >= l.getLoggingLevel() {
		l.zap.Info(args...)
	}
}

// Warn level logging.
// Use this to log warnings
func (l *logger) Warn(args ...interface{}) {
	if levelWarn >= l.getLoggingLevel() {
		l.zap.Warn(args...)
	}
}

// Error level logging.
// Use this to log errors which dont immediately halt the application.
func (l *logger) Error(args ...interface{}) {
	if levelError >= l.getLoggingLevel() {
		l.zap.Error(args...)
	}
}

// Fatal level logging.
// Use this to log errors which crash the application.
func (l *logger) Fatal(args ...interface{}) {
	l.zap.Error(args...)

	// If enableStackTrace is true, Zaplogger will take care of writing stacktrace to the file.
	// Else, we are force writing the stacktrace to the file.
	if !l.logConfig.enableStackTrace.Load() {
		byteArr := make([]byte, 2048)
		n := runtime.Stack(byteArr, false)
		stackTrace := string(byteArr[:n])
		l.zap.Error(stackTrace)
	}
	_ = l.zap.Sync()
}

// Debugf does debug level logging similar to fmt.Printf.
// Most verbose logging level
func (l *logger) Debugf(format string, args ...interface{}) {
	if levelDebug >= l.getLoggingLevel() {
		l.zap.Debugf(format, args...)
	}
}

// Infof does info level logging similar to fmt.Printf.
// Use this to log the state of the application. Dont use Logger.Info in the flow of individual events. Use Logger.Debug instead.
func (l *logger) Infof(format string, args ...interface{}) {
	if levelInfo >= l.getLoggingLevel() {
		l.zap.Infof(format, args...)
	}
}

// Warnf does warn level logging similar to fmt.Printf.
// Use this to log warnings
func (l *logger) Warnf(format string, args ...interface{}) {
	if levelWarn >= l.getLoggingLevel() {
		l.zap.Warnf(format, args...)
	}
}

// Errorf does error level logging similar to fmt.Printf.
// Use this to log errors which dont immediately halt the application.
func (l *logger) Errorf(format string, args ...interface{}) {
	if levelError >= l.getLoggingLevel() {
		l.zap.Errorf(format, args...)
	}
}

// Fatalf does fatal level logging similar to fmt.Printf.
// Use this to log errors which crash the application.
func (l *logger) Fatalf(format string, args ...interface{}) {
	l.zap.Errorf(format, args...)

	// If enableStackTrace is true, Zaplogger will take care of writing stacktrace to the file.
	// Else, we are force writing the stacktrace to the file.
	if !l.logConfig.enableStackTrace.Load() {
		byteArr := make([]byte, 2048)
		n := runtime.Stack(byteArr, false)
		stackTrace := string(byteArr[:n])
		l.zap.Error(stackTrace)
	}
	_ = l.zap.Sync()
}

// Debugw does debug level structured logging.
// Most verbose logging level
func (l *logger) Debugw(msg string, keysAndValues ...interface{}) {
	if levelDebug >= l.getLoggingLevel() {
		l.zap.Debugw(msg, keysAndValues...)
	}
}

// Infof does info level structured logging.
// Use this to log the state of the application. Dont use Logger.Info in the flow of individual events. Use Logger.Debug instead.
func (l *logger) Infow(msg string, keysAndValues ...interface{}) {
	if levelInfo >= l.getLoggingLevel() {
		l.zap.Infow(msg, keysAndValues...)
	}
}

// Warnf does warn level structured logging.
// Use this to log warnings
func (l *logger) Warnw(msg string, keysAndValues ...interface{}) {
	if levelWarn >= l.getLoggingLevel() {
		l.zap.Warnw(msg, keysAndValues...)
	}
}

// Errorf does error level structured logging.
// Use this to log errors which dont immediately halt the application.
func (l *logger) Errorw(msg string, keysAndValues ...interface{}) {
	if levelError >= l.getLoggingLevel() {
		l.zap.Errorw(msg, keysAndValues...)
	}
}

// Fatalf does fatal level structured logging.
// Use this to log errors which crash the application.
func (l *logger) Fatalw(msg string, keysAndValues ...interface{}) {
	l.zap.Errorw(msg, keysAndValues...)

	// If enableStackTrace is true, Zaplogger will take care of writing stacktrace to the file.
	// Else, we are force writing the stacktrace to the file.
	if !l.logConfig.enableStackTrace.Load() {
		byteArr := make([]byte, 2048)
		n := runtime.Stack(byteArr, false)
		stackTrace := string(byteArr[:n])
		l.zap.Error(stackTrace)
	}
	_ = l.zap.Sync()
}

// LogRequest reads and logs the request body and resets the body to original state.
func (l *logger) LogRequest(req *http.Request) {
	if levelEvent >= l.getLoggingLevel() {
		defer func() { _ = req.Body.Close() }()
		bodyBytes, _ := io.ReadAll(req.Body)
		bodyString := string(bodyBytes)
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		// print raw request body for debugging purposes
		l.zap.Debug("Request Body: ", bodyString)
	}
}
