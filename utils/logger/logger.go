/*
Logger Interface Use instance of logger instead of exported functions

usage example

import "github.com/rudderlabs/rudder-server/utils/logger"

var	log logger.LoggerI  = &logger.LoggerT{}
			or
var	log logger.LoggerI = logger.NewLogger()

...

log.Error(...)
*/
//go:generate mockgen -destination=../../mocks/utils/logger/mock_logger.go -package mock_logger github.com/rudderlabs/rudder-server/utils/logger LoggerI
package logger

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"runtime"

	"github.com/rudderlabs/rudder-server/config"
	"go.uber.org/zap"
)

/*
Using levels(like Debug, Info etc.) in logging is a way to categorize logs based on their importance.
The idea is to have the option of running the application in different logging levels based on
how verbose we want the logging to be.
For example, using Debug level of logging, logs everything and it might slow the application, so we run application
in DEBUG level for local development or when we want to look through the entire flow of events in detail.
We use 4 logging levels here Debug, Info, Error and Fatal.
*/

type LoggerI interface {
	IsDebugLevel() bool
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Fatal(args ...interface{})
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	LogRequest(req *http.Request)
}

type LoggerT struct{}

const (
	levelEvent = iota // Logs Event
	levelDebug        // Most verbose logging level
	levelInfo         // Logs about state of the application
	levelWarn         // Logs about warnings
	levelError        // Logs about errors which dont immediately halt the application
	levelFatal        // Logs which crashes the application
)

var levelMap = map[string]int{
	"EVENT": levelEvent,
	"DEBUG": levelDebug,
	"INFO":  levelInfo,
	"WARN":  levelWarn,
	"ERROR": levelError,
	"FATAL": levelFatal,
}

var (
	enableConsole       bool
	enableFile          bool
	consoleJsonFormat   bool
	fileJsonFormat      bool
	level               int
	enableTimestamp     bool
	enableFileNameInLog bool
	enableStackTrace    bool
	logFileLocation     string
	logFileSize         int
)

var (
	Log *zap.SugaredLogger
	log = NewLogger()
)

func loadConfig() {
	level = levelMap[config.GetEnv("LOG_LEVEL", "INFO")]
	enableConsole = config.GetBool("Logger.enableConsole", true)
	enableFile = config.GetBool("Logger.enableFile", false)
	consoleJsonFormat = config.GetBool("Logger.consoleJsonFormat", false)
	fileJsonFormat = config.GetBool("Logger.fileJsonFormat", false)
	logFileLocation = config.GetString("Logger.logFileLocation", "/tmp/rudder_log.log")
	logFileSize = config.GetInt("Logger.logFileSize", 100)
	enableTimestamp = config.GetBool("Logger.enableTimestamp", true)
	enableFileNameInLog = config.GetBool("Logger.enableFileNameInLog", false)
	enableStackTrace = config.GetBool("Logger.enableStackTrace", false)
}

var options []zap.Option

func NewLogger() LoggerI {
	return &LoggerT{}
}

// Setup sets up the logger initially
func (l *LoggerT) Setup() {
	loadConfig()
	Log = configureLogger()
}

//IsDebugLevel Returns true is debug lvl is enabled
func (l *LoggerT) IsDebugLevel() bool {
	return levelDebug >= level
}

// Deprecated! Use instance of LoggerT instead
func IsDebugLevel() bool {
	return log.IsDebugLevel()
}

// Debug level logging.
// Most verbose logging level.
func (l *LoggerT) Debug(args ...interface{}) {
	Log.Debug(args...)
}

// Deprecated! Use instance of LoggerT instead
func Debug(args ...interface{}) {
	log.Debug(args...)
}

// Info level logging.
// Use this to log the state of the application. Dont use Logger.Info in the flow of individual events. Use Logger.Debug instead.
func (l *LoggerT) Info(args ...interface{}) {
	Log.Info(args...)
}

// Deprecated! Use instance of LoggerT instead
func Info(args ...interface{}) {
	log.Info(args...)
}

// Warn level logging.
// Use this to log warnings
func (l *LoggerT) Warn(args ...interface{}) {
	Log.Warn(args...)
}

// Deprecated! Use instance of LoggerT instead
func Warn(args ...interface{}) {
	log.Warn(args...)
}

// Error level logging.
// Use this to log errors which dont immediately halt the application.
func (l *LoggerT) Error(args ...interface{}) {
	Log.Error(args...)
}

// Deprecated! Use instance of LoggerT instead
func Error(args ...interface{}) {
	log.Error(args...)
}

// Fatal level logging.
// Use this to log errors which crash the application.
func (l *LoggerT) Fatal(args ...interface{}) {
	if levelFatal >= level {
		Log.Error(args...)

		//If enableStackTrace is true, Zaplogger will take care of writing stacktrace to the file.
		//Else, we are force writing the stacktrace to the file.
		if !enableStackTrace {
			byteArr := make([]byte, 2048)
			n := runtime.Stack(byteArr, false)
			stackTrace := string(byteArr[:n])
			Log.Error(stackTrace)
		}
		Log.Sync()
	}
}

// Deprecated! Use instance of LoggerT instead
func Fatal(args ...interface{}) {
	log.Fatal(args...)
}

// Debugf does debug level logging similar to fmt.Printf.
// Most verbose logging level
func (l *LoggerT) Debugf(format string, args ...interface{}) {
	Log.Debugf(format, args...)
}

// Deprecated! Use instance of LoggerT instead
func Debugf(format string, args ...interface{}) {
	log.Debugf(format, args...)
}

// Infof does info level logging similar to fmt.Printf.
// Use this to log the state of the application. Dont use Logger.Info in the flow of individual events. Use Logger.Debug instead.
func (l *LoggerT) Infof(format string, args ...interface{}) {
	Log.Infof(format, args...)
}

// Deprecated! Use instance of LoggerT instead
func Infof(format string, args ...interface{}) {
	log.Infof(format, args...)
}

// Warnf does warn level logging similar to fmt.Printf.
// Use this to log warnings
func (l *LoggerT) Warnf(format string, args ...interface{}) {
	Log.Warnf(format, args...)
}

// Deprecated! Use instance of LoggerT instead
func Warnf(format string, args ...interface{}) {
	log.Warnf(format, args...)
}

// Errorf does error level logging similar to fmt.Printf.
// Use this to log errors which dont immediately halt the application.
func (l *LoggerT) Errorf(format string, args ...interface{}) {
	Log.Errorf(format, args...)
}

// Deprecated! Use instance of LoggerT instead
func Errorf(format string, args ...interface{}) {
	log.Errorf(format, args...)
}

// Fatalf does fatal level logging similar to fmt.Printf.
// Use this to log errors which crash the application.
func (l *LoggerT) Fatalf(format string, args ...interface{}) {
	if levelFatal >= level {
		Log.Errorf(format, args...)

		//If enableStackTrace is true, Zaplogger will take care of writing stacktrace to the file.
		//Else, we are force writing the stacktrace to the file.
		if !enableStackTrace {
			byteArr := make([]byte, 2048)
			n := runtime.Stack(byteArr, false)
			stackTrace := string(byteArr[:n])
			Log.Error(stackTrace)
		}
		Log.Sync()
	}
}

// Deprecated! Use instance of LoggerT instead
func Fatalf(format string, args ...interface{}) {
	log.Fatalf(format, args...)
}

// LogRequest reads and logs the request body and resets the body to original state.
func (l *LoggerT) LogRequest(req *http.Request) {
	if levelEvent >= level {
		defer req.Body.Close()
		bodyBytes, _ := ioutil.ReadAll(req.Body)
		bodyString := string(bodyBytes)
		req.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
		//print raw request body for debugging purposes
		Log.Debug("Request Body: ", bodyString)
	}
}

// Deprecated! Use instance of LoggerT instead
func LogRequest(req *http.Request) {
	log.LogRequest(req)
}
