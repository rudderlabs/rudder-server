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
