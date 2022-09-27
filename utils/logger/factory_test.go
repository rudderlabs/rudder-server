package logger_test

import (
	"bufio"
	"os"
	"testing"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
	"github.com/zenizh/go-capturer"
)

func Test_Config_Default(t *testing.T) {
	c := config.New()
	stdout := capturer.CaptureStdout(func() {
		loggerFactory := logger.NewFactory(c, constantClockOpt)
		logger := loggerFactory.NewLogger()
		logger.Info("hello world")
		loggerFactory.Sync()
	})
	require.Contains(t, stdout, "2077-01-23T10:15:13.000Z")
	require.Contains(t, stdout, "INFO")
	require.Contains(t, stdout, "hello world")
}

func Test_Config_FileOutput(t *testing.T) {
	tmpDir := t.TempDir()
	c := config.New()
	c.Set("Logger.enableTimestamp", false)
	c.Set("Logger.enableConsole", false)
	c.Set("Logger.enableFile", true)
	c.Set("Logger.enableFileNameInLog", false)
	c.Set("Logger.enableLoggerNameInLog", false)
	c.Set("Logger.logFileLocation", tmpDir+"out.log")

	stdout := capturer.CaptureStdout(func() {
		loggerFactory := logger.NewFactory(c, constantClockOpt)
		logger := loggerFactory.NewLogger()
		logger.Info("hello world")
	})
	require.Empty(t, stdout, "it should not log anything to stdout")

	fileOut, err := os.ReadFile(tmpDir + "out.log")

	require.NoError(t, err, "file should exist")
	require.Equal(t, "INFO	hello world\n", string(fileOut))
}

func TestLogLevelFromConfig(t *testing.T) {
	fileName := t.TempDir() + "out.log"
	f, err := os.Create(fileName)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	c := config.New()
	// start with default log level WARN
	c.Set("LOG_LEVEL", "INFO")
	c.Set("Logger.enableConsole", false)
	c.Set("Logger.enableFile", true)
	c.Set("Logger.enableFileNameInLog", false)
	c.Set("Logger.enableLoggerNameInLog", false)
	c.Set("Logger.logFileLocation", fileName)

	c.Set("Logger.moduleLevels", "1=DEBUG:1.2=WARN:1.2.3=ERROR")
	loggerFactory := logger.NewFactory(c, constantClockOpt)
	rootLogger := loggerFactory.NewLogger()
	lvl1Logger := rootLogger.Child("1")
	lvl2Logger := lvl1Logger.Child("2")
	lvl3Logger := lvl2Logger.Child("3")

	rootLogger.Info("hello world")
	scanner := bufio.NewScanner(f)
	require.True(t, scanner.Scan(), "it should print a log statement")
	require.Equal(t, "2077-01-23T10:15:13.000Z	INFO	hello world", scanner.Text())

	lvl1Logger.Debug("hello world")
	require.True(t, scanner.Scan(), "it should print a log statement")
	require.Equal(t, "2077-01-23T10:15:13.000Z	DEBUG	hello world", scanner.Text())

	lvl2Logger.Warn("hello world")
	require.True(t, scanner.Scan(), "it should print a log statement")
	require.Equal(t, "2077-01-23T10:15:13.000Z	WARN	hello world", scanner.Text())

	lvl3Logger.Error("hello world")
	require.True(t, scanner.Scan(), "it should print a log statement")
	require.Equal(t, "2077-01-23T10:15:13.000Z	ERROR	hello world", scanner.Text())

	require.Equal(t, map[string]int{"1": 1, "1.2": 3, "1.2.3": 4}, loggerFactory.GetLoggingConfig())
}

func Test_SetLogLevel(t *testing.T) {
	fileName := t.TempDir() + "out.log"
	f, err := os.Create(fileName)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	c := config.New()
	// start with default log level WARN
	c.Set("LOG_LEVEL", "WARN")
	c.Set("Logger.enableConsole", false)
	c.Set("Logger.enableFile", true)
	c.Set("Logger.enableFileNameInLog", false)
	c.Set("Logger.enableLoggerNameInLog", false)
	c.Set("Logger.logFileLocation", fileName)
	loggerFactory := logger.NewFactory(c, constantClockOpt)
	rootLogger := loggerFactory.NewLogger()

	rootLogger.Info("hello world")
	require.False(t, bufio.NewScanner(f).Scan(), "it should not print a log statement for a level lower than WARN")

	rootLogger.Warn("hello world")
	scanner := bufio.NewScanner(f)
	require.True(t, scanner.Scan(), "it should print a log statement")
	require.Equal(t, "2077-01-23T10:15:13.000Z	WARN	hello world", scanner.Text())

	// change level to INFO
	require.NoError(t, loggerFactory.SetLogLevel("", "INFO"))
	rootLogger.Info("hello world")
	require.True(t, scanner.Scan(), "it should print a log statement")
	require.Equal(t, "2077-01-23T10:15:13.000Z	INFO	hello world", scanner.Text())

	otherLogger := rootLogger.Child("other")
	otherLogger.Info("other hello world")
	require.True(t, scanner.Scan(), "it should print a log statement")
	require.Equal(t, "2077-01-23T10:15:13.000Z	INFO	other hello world", scanner.Text())

	require.NoError(t, loggerFactory.SetLogLevel("other", "DEBUG"))
	otherLogger.Debug("other hello world")
	require.True(t, scanner.Scan(), "it should print a log statement")
	require.Equal(t, "2077-01-23T10:15:13.000Z	DEBUG	other hello world", scanner.Text())
	rootLogger.Debug("other hello world")
	require.False(t, scanner.Scan(), "it should not print a log statement for a level lower than INFO")
}

func Test_Config_Suppressed_Logs(t *testing.T) {
	fileName := t.TempDir() + "out.log"
	f, err := os.Create(fileName)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	c := config.New()
	// start with default log level WARN
	c.Set("LOG_LEVEL", "FATAL")
	c.Set("Logger.enableConsole", false)
	c.Set("Logger.enableFile", true)
	c.Set("Logger.enableFileNameInLog", false)
	c.Set("Logger.logFileLocation", fileName)
	loggerFactory := logger.NewFactory(c, constantClockOpt)
	rootLogger := loggerFactory.NewLogger()

	rootLogger.Debug("hello world")
	rootLogger.Debugf("hello %s", "world")
	rootLogger.Debugw("hello world", "key", "value")
	require.False(t, bufio.NewScanner(f).Scan(), "it should not print a log statement for a level lower than FATAL")

	rootLogger.Info("hello world")
	rootLogger.Infof("hello %s", "world")
	rootLogger.Infow("hello world", "key", "value")
	require.False(t, bufio.NewScanner(f).Scan(), "it should not print a log statement for a level lower than FATAL")

	rootLogger.Warn("hello world")
	rootLogger.Warnf("hello %s", "world")
	rootLogger.Warnw("hello world", "key", "value")
	require.False(t, bufio.NewScanner(f).Scan(), "it should not print a log statement for a level lower than FATAL")

	rootLogger.Error("hello world")
	rootLogger.Errorf("hello %s", "world")
	rootLogger.Errorw("hello world", "key", "value")
	require.False(t, bufio.NewScanner(f).Scan(), "it should not print a log statement for a level lower than FATAL")
}
