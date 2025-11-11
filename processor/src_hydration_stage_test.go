package processor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonparser"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/processor/types"
)

func TestSrcHydrationStage(t *testing.T) {
	t.Run("Test events are properly hydrated when source supports hydration", func(t *testing.T) {
		// Create test events
		events := []types.TransformerEvent{
			{
				Message: map[string]interface{}{
					"type":      "track",
					"event":     "Product Viewed",
					"productId": "12345",
				},
				Metadata: types.Metadata{
					MessageID: "message-1",
					SourceID:  fblaSourceId,
				},
			},
			{
				Message: map[string]interface{}{
					"type":      "track",
					"event":     "Product Added",
					"productId": "67890",
				},
				Metadata: types.Metadata{
					MessageID: "message-2",
					SourceID:  fblaSourceId,
				},
			},
		}

		// Create expected hydrated events
		hydratedEvents := []types.SrcHydrationEvent{
			{
				ID: "message-1",
				Event: map[string]interface{}{
					"type":        "track",
					"event":       "Product Viewed",
					"productId":   "12345",
					"productName": "Test Product 1",
					"price":       29.99,
				},
			},
			{
				ID: "message-2",
				Event: map[string]interface{}{
					"type":        "track",
					"event":       "Product Added",
					"productId":   "67890",
					"productName": "Test Product 2",
					"price":       39.99,
				},
			},
		}

		// Setup mock transformer clients
		transformerClients := transformer.NewSimpleClients()
		transformerClients.SetSrcHydrationOutput(types.SrcHydrationResponse{
			Batch: hydratedEvents,
		}, nil)
		// Setup test context
		c := &testContext{}
		c.Setup(t)
		defer c.Finish()
		// Create processor handle
		conf := config.New()
		proc := NewHandle(conf, transformerClients)
		c.mockGatewayJobsDB.EXPECT().DeleteExecuting().AnyTimes()
		Setup(proc, c, true, true, t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := proc.config.asyncInit.WaitContext(ctx)
		require.NoError(t, err)

		// Create test message
		message := &srcHydrationMessage{
			partition: "test-partition",
			subJobs: subJob{
				ctx: context.Background(),
			},
			eventSchemaJobsBySourceId: make(map[SourceIDT][]*jobsdb.JobT),
			groupedEventsBySourceId: map[SourceIDT][]types.TransformerEvent{
				SourceIDT(fblaSourceId): events,
			},
			eventsByMessageID: make(map[string]types.SingularEventWithMetadata),
		}

		// Execute the source hydration stage
		result, err := proc.srcHydrationStage("test-partition", message)

		// Assertions
		require.NoError(t, err)
		require.NotNil(t, result)

		// Check that events were hydrated
		hydratedTransformerEvents := result.groupedEventsBySourceId[SourceIDT(fblaSourceId)]
		require.Len(t, hydratedTransformerEvents, 2)

		// Verify first event was hydrated correctly
		require.Equal(t, "message-1", hydratedTransformerEvents[0].Metadata.MessageID)
		require.Equal(t, "Test Product 1", hydratedTransformerEvents[0].Message["productName"])
		require.Equal(t, 29.99, hydratedTransformerEvents[0].Message["price"])

		// Verify second event was hydrated correctly
		require.Equal(t, "message-2", hydratedTransformerEvents[1].Metadata.MessageID)
		require.Equal(t, "Test Product 2", hydratedTransformerEvents[1].Message["productName"])
		require.Equal(t, 39.99, hydratedTransformerEvents[1].Message["price"])
	})

	t.Run("Test events pass through unchanged when source doesn't support hydration", func(t *testing.T) {
		sourceID := SourceIDEnabled
		// Create test events
		events := []types.TransformerEvent{
			{
				Message: map[string]interface{}{
					"type":  "track",
					"event": "Simple Event",
				},
				Metadata: types.Metadata{
					MessageID: "message-1",
					SourceID:  sourceID,
				},
			},
		}

		// Setup mock transformer clients
		transformerClients := transformer.NewSimpleClients()
		// Setup test context
		c := &testContext{}
		c.Setup(t)
		defer c.Finish()
		// Create processor handle
		conf := config.New()
		proc := NewHandle(conf, transformerClients)
		c.mockGatewayJobsDB.EXPECT().DeleteExecuting().AnyTimes()
		Setup(proc, c, true, true, t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := proc.config.asyncInit.WaitContext(ctx)
		require.NoError(t, err)

		// Create test message
		message := &srcHydrationMessage{
			partition: "test-partition",
			subJobs: subJob{
				ctx: context.Background(),
			},
			eventSchemaJobsBySourceId: make(map[SourceIDT][]*jobsdb.JobT),
			groupedEventsBySourceId: map[SourceIDT][]types.TransformerEvent{
				SourceIDT(sourceID): events,
			},
			eventsByMessageID: make(map[string]types.SingularEventWithMetadata),
		}

		// Execute the source hydration stage
		result, err := proc.srcHydrationStage("test-partition", message)

		// Assertions
		require.NoError(t, err)
		require.NotNil(t, result)

		// Check that events passed through unchanged
		resultEvents := result.groupedEventsBySourceId[SourceIDT(sourceID)]
		require.Len(t, resultEvents, 1)
		require.Equal(t, events[0], resultEvents[0])
		require.Equal(t, "Simple Event", resultEvents[0].Message["event"])
	})

	t.Run("Test error when getSourceBySourceID returns an error", func(t *testing.T) {
		// Create test events
		events := []types.TransformerEvent{
			{
				Message: map[string]interface{}{
					"type":  "track",
					"event": "Test Event",
				},
				Metadata: types.Metadata{
					MessageID: "message-1",
					SourceID:  "non-existent-source-id",
				},
			},
		}

		// Setup mock transformer clients
		transformerClients := transformer.NewSimpleClients()
		// Setup test context
		c := &testContext{}
		c.Setup(t)
		defer c.Finish()
		// Create processor handle
		conf := config.New()
		proc := NewHandle(conf, transformerClients)
		c.mockGatewayJobsDB.EXPECT().DeleteExecuting().AnyTimes()
		Setup(proc, c, true, true, t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := proc.config.asyncInit.WaitContext(ctx)
		require.NoError(t, err)

		// Create test message
		message := &srcHydrationMessage{
			partition: "test-partition",
			subJobs: subJob{
				ctx: context.Background(),
			},
			eventSchemaJobsBySourceId: make(map[SourceIDT][]*jobsdb.JobT),
			groupedEventsBySourceId: map[SourceIDT][]types.TransformerEvent{
				SourceIDT("non-existent-source-id"): events,
			},
			eventsByMessageID: make(map[string]types.SingularEventWithMetadata),
		}

		// Execute the source hydration stage
		result, err := proc.srcHydrationStage("test-partition", message)

		// Assertions
		require.Error(t, err)
		require.Nil(t, result)
		require.Contains(t, err.Error(), "source not found")
	})

	t.Run("Test error when the hydration client returns an error during the Hydrate call", func(t *testing.T) {
		// Create test events
		events := []types.TransformerEvent{
			{
				Message: map[string]interface{}{
					"type":      "track",
					"event":     "Product Viewed",
					"productId": "12345",
				},
				Metadata: types.Metadata{
					MessageID: "message-1",
					SourceID:  fblaSourceId,
				},
			},
		}

		// Setup mock transformer clients with error
		transformerClients := transformer.NewSimpleClients()
		transformerClients.SetSrcHydrationOutput(types.SrcHydrationResponse{}, errors.New("hydration error"))

		// Setup test context
		c := &testContext{}
		c.Setup(t)
		defer c.Finish()
		// Create processor handle
		conf := config.New()
		proc := NewHandle(conf, transformerClients)
		c.mockGatewayJobsDB.EXPECT().DeleteExecuting().AnyTimes()
		Setup(proc, c, true, true, t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := proc.config.asyncInit.WaitContext(ctx)
		require.NoError(t, err)

		// Create test message
		message := &srcHydrationMessage{
			partition: "test-partition",
			subJobs: subJob{
				ctx: context.Background(),
			},
			eventSchemaJobsBySourceId: make(map[SourceIDT][]*jobsdb.JobT),
			groupedEventsBySourceId: map[SourceIDT][]types.TransformerEvent{
				SourceIDT(fblaSourceId): events,
			},
			eventsByMessageID: make(map[string]types.SingularEventWithMetadata),
		}

		// Execute the source hydration stage
		result, err := proc.srcHydrationStage("test-partition", message)

		// Assertions
		require.Error(t, err)
		require.Nil(t, result)
		require.Equal(t, "hydration error", err.Error())
	})

	t.Run("Test when JSON marshaling fails for event schema jobs", func(t *testing.T) {
		// Create test events with non-marshallable data
		events := []types.TransformerEvent{
			{
				Message: map[string]interface{}{
					"type":      "track",
					"event":     "Product Viewed",
					"productId": "12345",
					"circular":  make(map[string]interface{}),
				},
				Metadata: types.Metadata{
					MessageID: "message-1",
					SourceID:  fblaSourceId,
				},
			},
		}

		// Create circular reference
		circular := events[0].Message["circular"].(map[string]interface{})
		circular["self"] = circular

		// Create expected hydrated events
		hydratedEvents := []types.SrcHydrationEvent{
			{
				ID: "message-1",
				Event: map[string]interface{}{
					"type":        "track",
					"event":       "Product Viewed",
					"productId":   "12345",
					"productName": "Test Product 1",
					"price":       29.99,
					"circular":    circular,
				},
			},
		}

		// Setup mock transformer clients
		transformerClients := transformer.NewSimpleClients()
		transformerClients.SetSrcHydrationOutput(types.SrcHydrationResponse{
			Batch: hydratedEvents,
		}, nil)

		// Setup test context
		c := &testContext{}
		c.Setup(t)
		defer c.Finish()
		// Create processor handle
		conf := config.New()
		proc := NewHandle(conf, transformerClients)
		c.mockGatewayJobsDB.EXPECT().DeleteExecuting().AnyTimes()
		Setup(proc, c, true, true, t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := proc.config.asyncInit.WaitContext(ctx)
		require.NoError(t, err)

		// Create test message with event schema jobs
		originalEventWithMetadata := types.SingularEventWithMetadata{
			UserID: "test-user",
		}

		message := &srcHydrationMessage{
			partition: "test-partition",
			subJobs: subJob{
				ctx: context.Background(),
			},
			eventSchemaJobsBySourceId: map[SourceIDT][]*jobsdb.JobT{
				SourceIDT(fblaSourceId): {&jobsdb.JobT{}},
			},
			groupedEventsBySourceId: map[SourceIDT][]types.TransformerEvent{
				SourceIDT(fblaSourceId): events,
			},
			eventsByMessageID: map[string]types.SingularEventWithMetadata{
				"message-1": originalEventWithMetadata,
			},
		}

		// Execute the source hydration stage
		require.Panics(t, func() {
			_, _ = proc.srcHydrationStage("test-partition", message)
		})
	})

	t.Run("Test event schema jobs creation", func(t *testing.T) {
		// Create test events
		events := []types.TransformerEvent{
			{
				Message: map[string]interface{}{
					"type":      "track",
					"event":     "Product Viewed",
					"productId": "12345",
				},
				Metadata: types.Metadata{
					MessageID: "message-1",
					SourceID:  fblaSourceId,
				},
			},
		}

		// Create expected hydrated events
		hydratedEvents := []types.SrcHydrationEvent{
			{
				ID: "message-1",
				Event: map[string]interface{}{
					"type":        "track",
					"event":       "Product Viewed",
					"productId":   "12345",
					"productName": "Test Product 1",
					"price":       29.99,
				},
			},
		}

		// Setup mock transformer clients
		transformerClients := transformer.NewSimpleClients()
		transformerClients.SetSrcHydrationOutput(types.SrcHydrationResponse{
			Batch: hydratedEvents,
		}, nil)

		// Setup test context
		c := &testContext{}
		c.Setup(t)
		defer c.Finish()
		// Create processor handle
		conf := config.New()
		proc := NewHandle(conf, transformerClients)
		c.mockGatewayJobsDB.EXPECT().DeleteExecuting().AnyTimes()
		Setup(proc, c, true, true, t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := proc.config.asyncInit.WaitContext(ctx)
		require.NoError(t, err)

		// Create original event with metadata
		originalEventWithMetadata := types.SingularEventWithMetadata{
			UserID:      "test-user",
			Parameters:  []byte(`{"source_id": "test-source"}`),
			CustomVal:   "GW",
			WorkspaceId: "test-workspace",
			UUID:        [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		}

		// Create test message with event schema jobs
		message := &srcHydrationMessage{
			partition: "test-partition",
			subJobs: subJob{
				ctx: context.Background(),
			},
			eventSchemaJobsBySourceId: map[SourceIDT][]*jobsdb.JobT{
				SourceIDT(fblaSourceId): {&jobsdb.JobT{}},
			},
			groupedEventsBySourceId: map[SourceIDT][]types.TransformerEvent{
				SourceIDT(fblaSourceId): events,
			},
			eventsByMessageID: map[string]types.SingularEventWithMetadata{
				"message-1": originalEventWithMetadata,
			},
		}

		// Execute the source hydration stage
		result, err := proc.srcHydrationStage("test-partition", message)

		// Assertions
		require.NoError(t, err)
		require.NotNil(t, result)

		// Check that events were hydrated
		hydratedTransformerEvents := result.groupedEventsBySourceId[SourceIDT(fblaSourceId)]
		require.Len(t, hydratedTransformerEvents, 1)
		require.Equal(t, "Test Product 1", hydratedTransformerEvents[0].Message["productName"])

		// Check that event schema jobs were created
		eventSchemaJobs := result.eventSchemaJobsBySourceId[SourceIDT(fblaSourceId)]
		require.Len(t, eventSchemaJobs, 1)
		require.Equal(t, originalEventWithMetadata.UUID, eventSchemaJobs[0].UUID)
		require.Equal(t, originalEventWithMetadata.UserID, eventSchemaJobs[0].UserID)
		require.Equal(t, originalEventWithMetadata.Parameters, eventSchemaJobs[0].Parameters)
		require.Equal(t, originalEventWithMetadata.CustomVal, eventSchemaJobs[0].CustomVal)
		require.Equal(t, originalEventWithMetadata.WorkspaceId, eventSchemaJobs[0].WorkspaceId)
		require.Equal(t, jsonparser.GetStringOrEmpty(eventSchemaJobs[0].EventPayload, "productName"), "Test Product 1")
	})

	t.Run("Test eventsByMessageID updated with hydrated data", func(t *testing.T) {
		// Create test events
		events := []types.TransformerEvent{
			{
				Message: map[string]interface{}{
					"type":      "track",
					"event":     "Product Viewed",
					"productId": "12345",
				},
				Metadata: types.Metadata{
					MessageID: "message-1",
					SourceID:  fblaSourceId,
				},
			},
		}

		// Create expected hydrated events
		hydratedEvents := []types.SrcHydrationEvent{
			{
				ID: "message-1",
				Event: map[string]interface{}{
					"type":        "track",
					"event":       "Product Viewed",
					"productId":   "12345",
					"productName": "Test Product 1",
					"price":       29.99,
				},
			},
		}

		// Setup mock transformer clients
		transformerClients := transformer.NewSimpleClients()
		transformerClients.SetSrcHydrationOutput(types.SrcHydrationResponse{
			Batch: hydratedEvents,
		}, nil)

		// Setup test context
		c := &testContext{}
		c.Setup(t)
		defer c.Finish()
		// Create processor handle
		conf := config.New()
		proc := NewHandle(conf, transformerClients)
		c.mockGatewayJobsDB.EXPECT().DeleteExecuting().AnyTimes()
		Setup(proc, c, true, true, t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := proc.config.asyncInit.WaitContext(ctx)
		require.NoError(t, err)

		// Create original event with metadata
		originalEventWithMetadata := types.SingularEventWithMetadata{
			UserID:      "test-user",
			Parameters:  []byte(`{"source_id": "test-source"}`),
			CustomVal:   "GW",
			WorkspaceId: "test-workspace",
			UUID:        [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			ReceivedAt:  time.Now(),
		}

		// Create test message with event schema jobs
		message := &srcHydrationMessage{
			partition: "test-partition",
			subJobs: subJob{
				ctx: context.Background(),
			},
			eventSchemaJobsBySourceId: map[SourceIDT][]*jobsdb.JobT{
				SourceIDT(fblaSourceId): {&jobsdb.JobT{}},
			},
			groupedEventsBySourceId: map[SourceIDT][]types.TransformerEvent{
				SourceIDT(fblaSourceId): events,
			},
			eventsByMessageID: map[string]types.SingularEventWithMetadata{
				"message-1": originalEventWithMetadata,
			},
		}

		// Execute the source hydration stage
		result, err := proc.srcHydrationStage("test-partition", message)

		// Assertions
		require.NoError(t, err)
		require.NotNil(t, result)

		// Check that eventsByMessageID was updated with hydrated event data
		updatedEvent, exists := result.eventsByMessageID["message-1"]
		require.True(t, exists)
		require.Equal(t, "Test Product 1", updatedEvent.SingularEvent["productName"])
		require.Equal(t, 29.99, updatedEvent.SingularEvent["price"])
		// Verify other metadata is preserved
		require.Equal(t, originalEventWithMetadata.UserID, updatedEvent.UserID)
		require.Equal(t, originalEventWithMetadata.Parameters, updatedEvent.Parameters)
		require.Equal(t, originalEventWithMetadata.CustomVal, updatedEvent.CustomVal)
		require.Equal(t, originalEventWithMetadata.WorkspaceId, updatedEvent.WorkspaceId)
		require.Equal(t, originalEventWithMetadata.UUID, updatedEvent.UUID)
		require.Equal(t, originalEventWithMetadata.ReceivedAt, updatedEvent.ReceivedAt)
	})
}
