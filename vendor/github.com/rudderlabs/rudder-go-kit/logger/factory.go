package logger

import (
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/rudderlabs/rudder-go-kit/config"
)

// default factory instance
var Default *Factory

func init() {
	Default = NewFactory(config.Default)
}

// Reset resets the default logger factory.
// Shall only be used by tests, until we move to a proper DI framework
func Reset() {
	Default = NewFactory(config.Default)
}

// NewFactory creates a new logger factory
func NewFactory(config *config.Config, options ...Option) *Factory {
	f := &Factory{}
	f.config = newConfig(config)
	for _, option := range options {
		option.apply(f)
	}
	f.zap = newZapLogger(config, f.config)
	return f
}

// Factory is a factory for creating new loggers
type Factory struct {
	config *factoryConfig
	zap    *zap.SugaredLogger
}

// NewLogger creates a new logger using the default logger factory
func NewLogger() Logger {
	return Default.NewLogger()
}

// NewLogger creates a new logger
func (f *Factory) NewLogger() Logger {
	return &logger{
		logConfig: f.config,
		zap:       f.zap,
	}
}

// GetLoggingConfig returns the log levels for default logger factory
func GetLoggingConfig() map[string]int {
	return Default.GetLoggingConfig()
}

// GetLoggingConfig returns the log levels
func (f *Factory) GetLoggingConfig() map[string]int {
	return f.config.levelConfigCache.m
}

// SetLogLevel sets the log level for a module for the default logger factory
func SetLogLevel(name, levelStr string) error {
	return Default.SetLogLevel(name, levelStr)
}

// SetLogLevel sets the log level for a module
func (f *Factory) SetLogLevel(name, levelStr string) error {
	err := f.config.SetLogLevel(name, levelStr)
	if err != nil {
		f.zap.Info(f.config.levelConfig)
	}
	return err
}

// Sync flushes the loggers' output buffers for the default logger factory
func Sync() {
	Default.Sync()
}

// Sync flushes the loggers' output buffers
func (f *Factory) Sync() {
	_ = f.zap.Sync()
}

func newConfig(config *config.Config) *factoryConfig {
	fc := &factoryConfig{
		levelConfig:      &syncMap[string, int]{m: make(map[string]int)},
		levelConfigCache: &syncMap[string, int]{m: make(map[string]int)},
	}
	fc.rootLevel = levelMap[config.GetString("LOG_LEVEL", "INFO")]
	fc.enableNameInLog = config.GetBool("Logger.enableLoggerNameInLog", true)
	fc.enableStackTrace = config.GetReloadableBoolVar(false, "Logger.enableStackTrace")
	config.GetBool("Logger.enableLoggerNameInLog", true)

	// colon separated key value pairs
	// Example: "router.GA=DEBUG:warehouse.REDSHIFT=DEBUG"
	levelConfigStr := strings.TrimSpace(config.GetString("Logger.moduleLevels", ""))
	if levelConfigStr != "" {
		moduleLevelKVs := strings.Split(levelConfigStr, ":")
		for _, moduleLevelKV := range moduleLevelKVs {
			pair := strings.SplitN(moduleLevelKV, "=", 2)
			if len(pair) < 2 {
				continue
			}
			module := strings.TrimSpace(pair[0])
			if module == "" {
				continue
			}
			levelStr := strings.TrimSpace(pair[1])
			level, ok := levelMap[levelStr]
			if !ok {
				continue
			}
			fc.levelConfig.set(module, level)
		}
	}
	return fc
}

// newZapLogger configures the zap logger based on the config provide in config.toml
func newZapLogger(config *config.Config, fc *factoryConfig) *zap.SugaredLogger {
	var cores []zapcore.Core
	if config.GetBool("Logger.enableConsole", true) {
		writer := zapcore.Lock(os.Stdout)
		core := zapcore.NewCore(zapEncoder(config, config.GetBool("Logger.consoleJsonFormat", false)), writer, zapcore.DebugLevel)
		cores = append(cores, core)
	}
	if config.GetBool("Logger.enableFile", false) {
		writer := zapcore.AddSync(&lumberjack.Logger{
			Filename:  config.GetString("Logger.logFileLocation", "/tmp/rudder_log.log"),
			MaxSize:   config.GetInt("Logger.logFileSize", 100),
			Compress:  true,
			LocalTime: true,
		})
		core := zapcore.NewCore(zapEncoder(config, config.GetBool("Logger.fileJsonFormat", false)), writer, zapcore.DebugLevel)
		cores = append(cores, core)
	}
	combinedCore := zapcore.NewTee(cores...)
	var options []zap.Option
	if config.GetBool("Logger.enableFileNameInLog", true) {
		options = append(options, zap.AddCaller(), zap.AddCallerSkip(1))
	}
	if config.GetBool("Logger.enableStackTrace", false) {
		// enables stack track for log level error
		options = append(options, zap.AddStacktrace(zap.ErrorLevel))
	}

	if fc.clock != nil {
		options = append(options, zap.WithClock(fc.clock))
	}

	zapLogger := zap.New(combinedCore, options...)
	return zapLogger.Sugar()
}

// zapEncoder configures the output of the log
func zapEncoder(config *config.Config, json bool) zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	if config.GetBool("Logger.enableTimestamp", true) {
		encoderConfig.TimeKey = "ts"
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	} else {
		encoderConfig.TimeKey = ""
	}
	if json {
		return zapcore.NewJSONEncoder(encoderConfig)
	}
	return zapcore.NewConsoleEncoder(encoderConfig)
}
