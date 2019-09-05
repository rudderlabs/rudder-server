package logger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/rudderlabs/rudder-server/config"
)

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
	level = levelMap[config.GetEnv("LOG_LEVEL", "DEBUG")]
}

// Debug level logging
func Debug(args ...interface{}) (int, error) {
	if levelDebug >= level {
		fmt.Print("DEBUG: ")
		return fmt.Println(args...)
	}
	return 0, nil
}

// Info level logging
func Info(args ...interface{}) (int, error) {
	if levelInfo >= level {
		fmt.Print("INFO: ")
		return fmt.Println(args...)
	}
	return 0, nil
}

// Error level logging
func Error(args ...interface{}) (int, error) {
	if levelError >= level {
		fmt.Print("ERROR: ")
		return fmt.Println(args...)
	}
	return 0, nil
}

// Fatal level logging
func Fatal(args ...interface{}) (int, error) {
	if levelFatal >= level {
		fmt.Print("FATAL: ")
		return fmt.Println(args...)
	}
	return 0, nil
}

// Debugf does debug level logging similar to fmt.Printf
func Debugf(format string, args ...interface{}) (int, error) {
	if levelDebug >= level {
		fmt.Print("DEBUG: ")
		return fmt.Printf(format, args...)
	}
	return 0, nil
}

// Infof does debug level logging similar to fmt.Printf
func Infof(format string, args ...interface{}) (int, error) {
	if levelInfo >= level {
		fmt.Print("INFO: ")
		return fmt.Printf(format, args...)
	}
	return 0, nil
}

// Errorf does debug level logging similar to fmt.Printf
func Errorf(format string, args ...interface{}) (int, error) {
	if levelError >= level {
		fmt.Print("ERROR: ")
		return fmt.Printf(format, args...)
	}
	return 0, nil
}

// Fatalf does debug level logging similar to fmt.Printf
func Fatalf(format string, args ...interface{}) (int, error) {
	if levelFatal >= level {
		fmt.Print("FATAL: ")
		return fmt.Printf(format, args...)
	}
	return 0, nil
}

// LogRequest reads and logs the request body and resets the body to original state
func LogRequest(req *http.Request) (int, error) {
	defer req.Body.Close()
	if levelDebug >= level {
		fmt.Print("DEBUG: Request Body: ")
		bodyBytes, _ := ioutil.ReadAll(req.Body)
		bodyString := string(bodyBytes)
		req.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
		//print raw response body for debugging purposes
		return fmt.Println(bodyString)
	}
	return 0, nil
}
