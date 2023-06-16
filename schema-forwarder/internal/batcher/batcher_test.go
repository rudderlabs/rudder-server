package batcher_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/rudderlabs/rudder-server/jobsdb"
	proto "github.com/rudderlabs/rudder-server/proto/event-schema"
	"github.com/rudderlabs/rudder-server/schema-forwarder/internal/batcher"
)

func TestEventSchemaMessageBatcher(t *testing.T) {
	t.Run("Add same message twice", func(t *testing.T) {
		mockTransformer := &mockTransformer{}
		mockTransformer.msg = &proto.EventSchemaMessage{
			Key: &proto.EventSchemaKey{
				WriteKey:        "write-key",
				EventType:       "event-type",
				EventIdentifier: "event-identifier",
			},
			Hash:       "hash",
			ObservedAt: timestamppb.Now(),
		}

		b := batcher.NewEventSchemaMessageBatcher(mockTransformer)

		require.NoError(t, b.Add(&jobsdb.JobT{JobID: 1}))
		require.NoError(t, b.Add(&jobsdb.JobT{JobID: 2}))

		batches := b.GetMessageBatches()
		require.Len(t, batches, 1)
		require.Len(t, batches[0].Jobs, 2)
		require.EqualValues(t, 1, batches[0].Message.BatchCount)
	})

	t.Run("Add different hash", func(t *testing.T) {
		mockTransformer := &mockTransformer{}
		mockTransformer.msg = &proto.EventSchemaMessage{
			Key: &proto.EventSchemaKey{
				WriteKey:        "write-key",
				EventType:       "event-type",
				EventIdentifier: "event-identifier",
			},
			Hash:       "hash-1",
			ObservedAt: timestamppb.Now(),
		}

		b := batcher.NewEventSchemaMessageBatcher(mockTransformer)

		require.NoError(t, b.Add(&jobsdb.JobT{JobID: 1}))

		mockTransformer.msg = &proto.EventSchemaMessage{
			Key: &proto.EventSchemaKey{
				WriteKey:        "write-key",
				EventType:       "event-type",
				EventIdentifier: "event-identifier",
			},
			Hash:       "hash-2",
			ObservedAt: timestamppb.Now(),
		}

		require.NoError(t, b.Add(&jobsdb.JobT{JobID: 2}))

		batches := b.GetMessageBatches()
		require.Len(t, batches, 2)

		require.Len(t, batches[0].Jobs, 1)
		require.EqualValues(t, 1, batches[0].Jobs[0].JobID)
		require.EqualValues(t, 0, batches[0].Message.BatchCount)

		require.Len(t, batches[1].Jobs, 1)
		require.EqualValues(t, 2, batches[1].Jobs[0].JobID)
		require.EqualValues(t, 0, batches[1].Message.BatchCount)
	})

	t.Run("Transformer error", func(t *testing.T) {
		mockTransformer := &mockTransformer{fail: true}
		b := batcher.NewEventSchemaMessageBatcher(mockTransformer)
		require.Error(t, b.Add(&jobsdb.JobT{JobID: 2}))
	})
}

type mockTransformer struct {
	fail bool
	msg  *proto.EventSchemaMessage
}

func (*mockTransformer) Start() {}
func (*mockTransformer) Stop()  {}

func (mt *mockTransformer) Transform(job *jobsdb.JobT) (*proto.EventSchemaMessage, error) {
	if mt.fail {
		return nil, errors.New("failed")
	}
	return mt.msg, nil
}
