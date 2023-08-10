package logfield

import (
	"sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

const (
	UploadJobID                = "uploadJobID"
	UploadStatus               = "uploadStatus"
	UseRudderStorage           = "useRudderStorage"
	SourceType                 = "sourceType"
	DestinationType            = "destinationType"
	DestinationRevisionID      = "destinationRevisionID"
	DestinationValidationsStep = "step"
	WorkspaceID                = "workspaceID"
	Namespace                  = "namespace"
	Schema                     = "schema"
	ColumnName                 = "columnName"
	ColumnType                 = "columnType"
	Priority                   = "priority"
	Retried                    = "retried"
	Attempt                    = "attempt"
	LoadFileType               = "loadFileType"
	ErrorMapping               = "errorMapping"
	DestinationCredsValid      = "destinationCredsValid"
	QueryExecutionTime         = "queryExecutionTime"
	StagingTableName           = "stagingTableName"
)

// TODO finish converting constants to functions to experiment

type Field func(v interface{}) KeyValue

type KeyValue struct {
	k string
	v interface{}
}

var (
	UploadID      Field = func(v interface{}) KeyValue { return KeyValue{"uploadID", v} }
	SourceID      Field = func(v interface{}) KeyValue { return KeyValue{"sourceID", v} }
	DestinationID Field = func(v interface{}) KeyValue { return KeyValue{"destinationID", v} }
	TableName     Field = func(v interface{}) KeyValue { return KeyValue{"tableName", v} }
	Location      Field = func(v interface{}) KeyValue { return KeyValue{"location", v} }
	Query         Field = func(v interface{}) KeyValue { return KeyValue{"query", v} }
	Error         Field = func(v interface{}) KeyValue {
		if err, ok := v.(error); ok {
			return KeyValue{"error", err.Error()}
		} else {
			return KeyValue{"error", v}
		}
	}

	// Generic should be used only when there is no other option and it doesn't make sense to create a new Field
	Generic = func(k string) Field {
		return func(v interface{}) KeyValue {
			return KeyValue{k, v}
		}
	}
)

func NewLogger(component string, l logger.Logger) Logger {
	return Logger{component: component, logger: l}
}

type Logger struct {
	component string
	logger    logger.Logger
	once      sync.Once
}

func (l *Logger) init() {
	l.once.Do(func() {
		if l.logger == nil {
			l.logger = logger.NOP
		}
	})
}

func (l *Logger) Infow(msg string, kvs ...KeyValue) {
	l.init()
	l.logger.Infow(msg, l.keyAndValues(kvs)...)
}

func (l *Logger) Debugw(msg string, kvs ...KeyValue) {
	l.init()
	l.logger.Debugw(msg, l.keyAndValues(kvs)...)
}

func (l *Logger) Errorw(msg string, kvs ...KeyValue) {
	l.init()
	l.logger.Errorw(msg, l.keyAndValues(kvs)...)
}

func (l *Logger) Warnw(msg string, kvs ...KeyValue) {
	l.init()
	l.logger.Warnw(msg, l.keyAndValues(kvs)...)
}

func (l *Logger) Fatalw(msg string, kvs ...KeyValue) {
	l.init()
	l.logger.Fatalw(msg, l.keyAndValues(kvs)...)
}

func (l *Logger) keyAndValues(kvs []KeyValue) []interface{} {
	length := len(kvs) * 2
	if l.component != "" {
		length += 2
	}
	s := make([]interface{}, 0, length)
	for _, kv := range kvs {
		s = append(s, kv.k, kv.v)
	}
	if l.component != "" {
		s = append(s, "component", l.component)
	}
	return s
}
