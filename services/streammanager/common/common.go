package common

import "encoding/json"

type StreamProducer interface {
	Produce(jsonData json.RawMessage, destConfig interface{}) (int, string, string)
}

type ClosableStreamProducer interface {
	CloseProducer() error
}
