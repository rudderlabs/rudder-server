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

var Log *zap.SugaredLogger

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

// Setup sets up the logger initially
func Setup() {
	loadConfig()
	Log = configureLogger()
}

func IsDebugLevel() bool {
	return levelDebug >= level
}

// Debug level logging.
// Most verbose logging level.
func Debug(args ...interface{}) {
	Log.Debug(args...)
}

// Info level logging.
// Use this to log the state of the application. Dont use Logger.Info in the flow of individual events. Use Logger.Debug instead.
func Info(args ...interface{}) {
	Log.Info(args...)
}

// Warn level logging.
// Use this to log warnings
func Warn(args ...interface{}) {
	Log.Warn(args...)
}

// Error level logging.
// Use this to log errors which dont immediately halt the application.
func Error(args ...interface{}) {
	Log.Error(args...)
}

// Fatal level logging.
// Use this to log errors which crash the application.
func Fatal(args ...interface{}) {
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

// Debugf does debug level logging similar to fmt.Printf.
// Most verbose logging level
func Debugf(format string, args ...interface{}) {
	Log.Debugf(format, args...)
}

// Infof does info level logging similar to fmt.Printf.
// Use this to log the state of the application. Dont use Logger.Info in the flow of individual events. Use Logger.Debug instead.
func Infof(format string, args ...interface{}) {
	Log.Infof(format, args...)
}

// Warnf does warn level logging similar to fmt.Printf.
// Use this to log warnings
func Warnf(format string, args ...interface{}) {
	Log.Warnf(format, args...)
}

// Errorf does error level logging similar to fmt.Printf.
// Use this to log errors which dont immediately halt the application.
func Errorf(format string, args ...interface{}) {
	Log.Errorf(format, args...)
}

// Fatalf does fatal level logging similar to fmt.Printf.
// Use this to log errors which crash the application.
func Fatalf(format string, args ...interface{}) {
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

// LogRequest reads and logs the request body and resets the body to original state.
func LogRequest(req *http.Request) {
	if levelEvent >= level {
		defer req.Body.Close()
		bodyBytes, _ := ioutil.ReadAll(req.Body)
		bodyString := string(bodyBytes)
		req.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
		//print raw request body for debugging purposes
		Log.Debug("Request Body: ", bodyString)
	}
}
