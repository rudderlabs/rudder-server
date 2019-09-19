package logger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/rudderlabs/rudder-server/config"
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
	levelDebug = iota + 1 // Most verbose logging level
	levelInfo             // Logs about state of the application
	levelError            // Logs about errors which dont immediately halt the application
	levelFatal            // Logs which crashes the application
)

var levelMap = map[string]int{
	"DEBUG": levelDebug,
	"INFO":  levelInfo,
	"ERROR": levelError,
	"FATAL": levelFatal,
}

var level int

// Setup sets up the logger initially
func Setup() {
	level = levelMap[config.GetEnv("LOG_LEVEL", "INFO")]
}

// Debug level logging.
// Most verbose logging level.
func Debug(args ...interface{}) (int, error) {
	if levelDebug >= level {
		fmt.Print("DEBUG: ")
		return fmt.Println(args...)
	}
	return 0, nil
}

// Info level logging.
// Use this to log the state of the application. Dont use Logger.Info in the flow of individual events. Use Logger.Debug instead.
func Info(args ...interface{}) (int, error) {
	if levelInfo >= level {
		fmt.Print("INFO: ")
		return fmt.Println(args...)
	}
	return 0, nil
}

// Error level logging.
// Use this to log errors which dont immediately halt the application.
func Error(args ...interface{}) (int, error) {
	if levelError >= level {
		fmt.Print("ERROR: ")
		return fmt.Println(args...)
	}
	return 0, nil
}

// Fatal level logging.
// Use this to log errors which crash the application.
func Fatal(args ...interface{}) (int, error) {
	if levelFatal >= level {
		fmt.Print("FATAL: ")
		return fmt.Println(args...)
	}
	return 0, nil
}

// Debugf does debug level logging similar to fmt.Printf.
// Most verbose logging level
func Debugf(format string, args ...interface{}) (int, error) {
	if levelDebug >= level {
		fmt.Print("DEBUG: ")
		return fmt.Printf(format, args...)
	}
	return 0, nil
}

// Infof does info level logging similar to fmt.Printf.
// Use this to log the state of the application. Dont use Logger.Info in the flow of individual events. Use Logger.Debug instead.
func Infof(format string, args ...interface{}) (int, error) {
	if levelInfo >= level {
		fmt.Print("INFO: ")
		return fmt.Printf(format, args...)
	}
	return 0, nil
}

// Errorf does error level logging similar to fmt.Printf.
// Use this to log errors which dont immediately halt the application.
func Errorf(format string, args ...interface{}) (int, error) {
	if levelError >= level {
		fmt.Print("ERROR: ")
		return fmt.Printf(format, args...)
	}
	return 0, nil
}

// Fatalf does fatal level logging similar to fmt.Printf.
// Use this to log errors which crash the application.
func Fatalf(format string, args ...interface{}) (int, error) {
	if levelFatal >= level {
		fmt.Print("FATAL: ")
		return fmt.Printf(format, args...)
	}
	return 0, nil
}

// LogRequest reads and logs the request body and resets the body to original state.
func LogRequest(req *http.Request) (int, error) {
	if levelDebug >= level {
		defer req.Body.Close()
		fmt.Print("DEBUG: Request Body: ")
		bodyBytes, _ := ioutil.ReadAll(req.Body)
		bodyString := string(bodyBytes)
		req.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
		//print raw request body for debugging purposes
		return fmt.Println(bodyString)
	}
	return 0, nil
}
