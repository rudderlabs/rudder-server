package proto

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
)

func (esk *EventSchemaKey) MustMarshal() []byte {
	m, err := proto.MarshalOptions{}.Marshal(esk)
	if err != nil {
		panic(fmt.Errorf("marshalling event schema key failed: %w", err))
	}
	return m
}

func (esm *EventSchemaMessage) MustMarshal() []byte {
	m, err := proto.MarshalOptions{}.Marshal(esm)
	if err != nil {
		panic(fmt.Errorf("marshalling event schema message failed: %w", err))
	}
	return m
}

// UnmarshalEventSchemaMessage creates a new event schema message from the provided protobuf bytes.
func UnmarshalEventSchemaMessage(raw []byte) (*EventSchemaMessage, error) {
	p := &EventSchemaMessage{}
	if err := proto.Unmarshal(raw, p); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event schema message: %w", err)
	}
	return p, nil
}

// SchemaHash returns a hash of the schema. Keys are sorted lexicographically during hashing.
func SchemaHash(schema map[string]string) string {
	keys := make([]string, 0, len(schema))
	for k := range schema {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteString(":")
		sb.WriteString(schema[k])
		sb.WriteString(",")
	}
	md5Sum := md5.Sum([]byte(sb.String()))
	schemaHash := hex.EncodeToString(md5Sum[:])
	return schemaHash
}

// Merge merges the other event schema message into this one.
func (sm *EventSchemaMessage) Merge(other *EventSchemaMessage) {
	sm.BatchCount += other.BatchCount + 1
	if len(other.Sample) < len(sm.Sample) { // keep the smallest sample
		sm.Sample = other.Sample
	}
	if other.ObservedAt.AsTime().After(sm.ObservedAt.AsTime()) { // keep the latest observed time
		sm.ObservedAt = other.ObservedAt
	}
}
