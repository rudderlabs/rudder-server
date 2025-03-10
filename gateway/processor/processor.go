package processor

import "github.com/rudderlabs/rudder-schemas/go/stream"

type Processor interface {
	SetNext(processor Processor)
	Process(payload []byte, properties stream.MessageProperties) ([]byte, error)
}

type BaseProcessor struct {
	next Processor
}

func (p *BaseProcessor) SetNext(processor Processor) {
	p.next = processor
}

func (p *BaseProcessor) ProcessNext(payload []byte, properties stream.MessageProperties) ([]byte, error) {
	if p.next != nil {
		return p.next.Process(payload, properties)
	}
	return payload, nil
}
