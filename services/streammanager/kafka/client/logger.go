package client

type logger interface {
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

type KafkaLogger struct {
	Logger        logger
	IsErrorLogger bool
}

func (l *KafkaLogger) Printf(format string, args ...interface{}) {
	if l.IsErrorLogger {
		l.Logger.Errorf(format, args...)
	} else {
		l.Logger.Infof(format, args...)
	}
}
