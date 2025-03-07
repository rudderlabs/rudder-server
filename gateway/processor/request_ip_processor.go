package processor

import (
	"fmt"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-schemas/go/stream"
)

type RequestIPProcessor struct {
	BaseProcessor
	OnError func(stream.MessageProperties, error)
}

func (p *RequestIPProcessor) Process(payload []byte, properties stream.MessageProperties) ([]byte, error) {
	var err error
	payload, err = fillRequestIP(payload, properties.RequestIP)
	if err != nil {
		p.OnError(properties, fmt.Errorf("filling request_ip: %w", err))
		return nil, fmt.Errorf("filling request_ip: %w", err)
	}
	return p.ProcessNext(payload, properties)
}

func fillRequestIP(event []byte, ip string) ([]byte, error) {
	if !gjson.GetBytes(event, "request_ip").Exists() {
		return sjson.SetBytes(event, "request_ip", ip)
	}
	return event, nil
}
