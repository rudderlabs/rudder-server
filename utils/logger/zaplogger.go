package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// getEncoderConfig configures the output of the log
func getEncoderConfig(isJson bool) zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	if enableTimestamp {
		encoderConfig.TimeKey = "ts"
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	} else {
		encoderConfig.TimeKey = ""
	}
	if isJson {
		return zapcore.NewJSONEncoder(encoderConfig)
	}
	return zapcore.NewConsoleEncoder(encoderConfig)
}

// configureLogger configures the zap logger based on the config provide in config.toml
func configureLogger() *zap.SugaredLogger {
	var cores []zapcore.Core
	if enableConsole {
		writer := zapcore.Lock(os.Stdout)
		core := zapcore.NewCore(getEncoderConfig(consoleJsonFormat), writer, zapcore.DebugLevel)
		cores = append(cores, core)
	}
	if enableFile {
		writer := zapcore.AddSync(&lumberjack.Logger{
			Filename:  logFileLocation,
			MaxSize:   logFileSize,
			Compress:  true,
			LocalTime: true,
		})
		core := zapcore.NewCore(getEncoderConfig(fileJsonFormat), writer, zapcore.DebugLevel)
		cores = append(cores, core)
	}
	combinedCore := zapcore.NewTee(cores...)
	if enableFileNameInLog {
		options = append(options, zap.AddCaller(), zap.AddCallerSkip(1))
	}
	if enableStackTrace {
		// enables stack track for log level error
		options = append(options, zap.AddStacktrace(zap.ErrorLevel))
	}
	zapLogger := zap.New(combinedCore, options...)
	return zapLogger.Sugar()
}
