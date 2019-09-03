package logger

import "fmt"

const (
	levelDebug = iota + 1
	levelInfo
	levelError
	levelFatal
)

var level = levelInfo

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
