package batcher

import (
	"github.com/rudderlabs/rudder-server/jobsdb"
	proto "github.com/rudderlabs/rudder-server/proto/event-schema"
	"github.com/rudderlabs/rudder-server/schema-forwarder/internal/transformer"
)

// A batch of jobs that share the same schema.
type EventSchemaMessageBatch struct {
	Index   int
	Message *proto.EventSchemaMessage
	Jobs    []*jobsdb.JobT
}

// NewEventSchemaMessageBatcher creates a new batcher.
func NewEventSchemaMessageBatcher(transformer transformer.Transformer) *EventSchemaMessageBatcher {
	return &EventSchemaMessageBatcher{
		transformer: transformer,
		batchIndex:  make(map[batchKey]*EventSchemaMessageBatch),
	}
}

// EventSchemaMessageBatcher batches jobs by their schema.
type EventSchemaMessageBatcher struct {
	transformer transformer.Transformer

	batchOrder []batchKey
	batchIndex map[batchKey]*EventSchemaMessageBatch
}

// Add adds a job to the batcher after transforming it to an [EventSchemaMessage].
// If the message is already in the batcher, the two messages will be merged to one.
func (sb *EventSchemaMessageBatcher) Add(job *jobsdb.JobT) error {
	msg, err := sb.transformer.Transform(job)
	if err != nil {
		return err
	}
	key := batchKey{
		writeKey:        msg.Key.WriteKey,
		eventType:       msg.Key.EventType,
		eventIdentifier: msg.Key.EventIdentifier,
		hash:            msg.Hash,
	}
	if _, ok := sb.batchIndex[key]; !ok {
		sb.batchOrder = append(sb.batchOrder, key)
		sb.batchIndex[key] = &EventSchemaMessageBatch{
			Message: msg,
			Jobs:    []*jobsdb.JobT{job},
		}
	} else {
		sb.batchIndex[key].Jobs = append(sb.batchIndex[key].Jobs, job)
		sb.batchIndex[key].Message.Merge(msg)
	}
	return nil
}

// GetMessageBatches returns the message batches in the order they were added.
func (sb *EventSchemaMessageBatcher) GetMessageBatches() []*EventSchemaMessageBatch {
	batches := make([]*EventSchemaMessageBatch, len(sb.batchOrder))
	for i, key := range sb.batchOrder {
		batches[i] = sb.batchIndex[key]
		batches[i].Index = i
	}
	return batches
}

// batchKey is the key used for batching.
type batchKey struct {
	writeKey        string
	eventType       string
	eventIdentifier string
	hash            string
}
