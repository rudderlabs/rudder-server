package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func getZapLevel(level int) zapcore.Level {
	switch level {
	case levelDebug:
		return zapcore.DebugLevel
	case levelInfo:
		return zapcore.InfoLevel
	case levelError:
		return zapcore.ErrorLevel
	case levelFatal:
		return zapcore.FatalLevel
	}
	return zapcore.DebugLevel
}

func getEncoderConfig(isJson bool) zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
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
