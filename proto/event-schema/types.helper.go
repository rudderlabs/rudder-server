package proto

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

func (esk *EventSchemaKey) MustMarshal() ([]byte, error) {
	m, err := proto.MarshalOptions{}.Marshal(esk)
	if err != nil {
		return []byte{}, fmt.Errorf("marshalling event schema key failed: %w", err)
	}
	return m, nil
}

func (esm *EventSchemaMessage) MustMarshal() ([]byte, error) {
	m, err := proto.MarshalOptions{}.Marshal(esm)
	if err != nil {
		return []byte{}, fmt.Errorf("marshalling event schema message failed: %w", err)
	}
	return m, nil
}

// UnmarshalEventSchemaMessage creates a new event schema message from the provided protobuf bytes.
func UnmarshalEventSchemaMessage(raw []byte) (*EventSchemaMessage, error) {
	p := &EventSchemaMessage{}
	if err := proto.Unmarshal(raw, p); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event schema message: %w", err)
	}
	return p, nil
}
