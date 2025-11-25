package processor

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"

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
					JobID:     1,
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
					JobID:     2,
				},
			},
		}

		// Create expected hydrated events
		hydratedEvents := []types.SrcHydrationEvent{
			{
				ID: "1",
				Event: map[string]interface{}{
					"type":        "track",
					"event":       "Product Viewed",
					"productId":   "12345",
					"productName": "Test Product 1",
					"price":       29.99,
				},
			},
			{
				ID: "2",
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
			eventsByMessageID: make(map[string]types.SingularEventWithReceivedAt),
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
			eventsByMessageID: make(map[string]types.SingularEventWithReceivedAt),
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
			eventsByMessageID: make(map[string]types.SingularEventWithReceivedAt),
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
			eventsByMessageID: make(map[string]types.SingularEventWithReceivedAt),
		}

		// Execute the source hydration stage
		result, err := proc.srcHydrationStage("test-partition", message)

		// Assertions
		require.NoError(t, err)
		require.Nil(t, result.groupedEventsBySourceId[SourceIDT(fblaSourceId)])
		require.Len(t, result.reportMetrics, 1)
		sampleEvent, err := jsonrs.Marshal(events[0].Message)
		require.NoError(t, err)

		require.Equal(t, result.reportMetrics[0], &reportingtypes.PUReportedMetric{
			ConnectionDetails: reportingtypes.ConnectionDetails{
				SourceID:       fblaSourceId,
				SourceCategory: "webhook",
			},
			PUDetails: reportingtypes.PUDetails{
				PU:         reportingtypes.SOURCE_HYDRATION,
				TerminalPU: false,
				InitialPU:  false,
			},
			StatusDetail: &reportingtypes.StatusDetail{
				Status:         "failed",
				Count:          1,
				StatusCode:     500,
				SampleResponse: "hydration error",
				SampleEvent:    sampleEvent,
				EventName:      "Product Viewed",
				EventType:      "track",
			},
		})
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
		originalEventWithMetadata := types.SingularEventWithReceivedAt{
			SingularEvent: dummySingularEvent,
		}

		ogEventSchemaJob := &jobsdb.JobT{
			JobID:       1,
			UserID:      "test-user",
			Parameters:  []byte(`{"source_id": "test-source"}`),
			CustomVal:   "GW",
			WorkspaceId: "test-workspace",
			UUID:        [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		}

		message := &srcHydrationMessage{
			partition: "test-partition",
			subJobs: subJob{
				ctx: context.Background(),
			},
			eventSchemaJobsBySourceId: map[SourceIDT][]*jobsdb.JobT{
				SourceIDT(fblaSourceId): {ogEventSchemaJob},
			},
			groupedEventsBySourceId: map[SourceIDT][]types.TransformerEvent{
				SourceIDT(fblaSourceId): events,
			},
			eventsByMessageID: map[string]types.SingularEventWithReceivedAt{
				"message-1": originalEventWithMetadata,
			},
		}

		// Execute the source hydration stage
		msg, err := proc.srcHydrationStage("test-partition", message)
		require.NoError(t, err)

		require.Equal(t, msg.eventSchemaJobsBySourceId[SourceIDT(fblaSourceId)][0], ogEventSchemaJob)
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
					JobID:     1,
				},
			},
		}

		// Create expected hydrated events
		hydratedEvents := []types.SrcHydrationEvent{
			{
				ID: "1",
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
		originalEventWithMetadata := types.SingularEventWithReceivedAt{
			SingularEvent: dummySingularEvent,
		}

		ogEventSchemaJob := &jobsdb.JobT{
			JobID:       1,
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
				SourceIDT(fblaSourceId): {ogEventSchemaJob},
			},
			groupedEventsBySourceId: map[SourceIDT][]types.TransformerEvent{
				SourceIDT(fblaSourceId): events,
			},
			eventsByMessageID: map[string]types.SingularEventWithReceivedAt{
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
		require.Equal(t, ogEventSchemaJob.UUID, eventSchemaJobs[0].UUID)
		require.Equal(t, ogEventSchemaJob.UserID, eventSchemaJobs[0].UserID)
		require.Equal(t, ogEventSchemaJob.Parameters, eventSchemaJobs[0].Parameters)
		require.Equal(t, ogEventSchemaJob.CustomVal, eventSchemaJobs[0].CustomVal)
		require.Equal(t, ogEventSchemaJob.WorkspaceId, eventSchemaJobs[0].WorkspaceId)
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
					JobID:     123,
				},
			},
		}

		// Create expected hydrated events
		hydratedEvents := []types.SrcHydrationEvent{
			{
				ID: "123",
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
		originalEventWithMetadata := types.SingularEventWithReceivedAt{
			SingularEvent: dummySingularEvent,
			ReceivedAt:    time.Now(),
		}

		// Create a test message with event schema jobs
		message := &srcHydrationMessage{
			partition: "test-partition",
			subJobs: subJob{
				ctx: context.Background(),
			},
			groupedEventsBySourceId: map[SourceIDT][]types.TransformerEvent{
				SourceIDT(fblaSourceId): events,
			},
			eventsByMessageID: map[string]types.SingularEventWithReceivedAt{
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
		require.Equal(t, originalEventWithMetadata.ReceivedAt, updatedEvent.ReceivedAt)
	})
}

func TestSrcHydrationStageConcurrency(t *testing.T) {
	// Create multiple sources with events to test concurrent access
	sources := []string{fblaSourceId, fblaSourceId2, fblaSourceId3, fblaSourceId4, fblaSourceId5}

	// Create test events for each source
	groupedEventsBySourceId := make(map[SourceIDT][]types.TransformerEvent)
	eventSchemaJobsBySourceId := make(map[SourceIDT][]*jobsdb.JobT)
	eventsByMessageID := make(map[string]types.SingularEventWithReceivedAt)

	for i, sourceID := range sources {
		events := []types.TransformerEvent{
			{
				Message: map[string]interface{}{
					"type":      "track",
					"event":     "Product Viewed",
					"productId": "12345",
				},
				Metadata: types.Metadata{
					MessageID: "message-" + sourceID,
					SourceID:  sourceID,
					JobID:     int64(i*10 + 1),
				},
			},
			{
				Message: map[string]interface{}{
					"type":      "track",
					"event":     "Product Added",
					"productId": "67890",
				},
				Metadata: types.Metadata{
					MessageID: "message2-" + sourceID,
					SourceID:  sourceID,
					JobID:     int64(i*10 + 2),
				},
			},
		}

		groupedEventsBySourceId[SourceIDT(sourceID)] = events

		// Add event schema jobs
		eventSchemaJobsBySourceId[SourceIDT(sourceID)] = []*jobsdb.JobT{
			{
				JobID:       int64(i*10 + 1),
				UserID:      "test-user",
				Parameters:  []byte(`{"source_id": "` + sourceID + `"}`),
				CustomVal:   "GW",
				WorkspaceId: "test-workspace",
				UUID:        [16]byte{byte(i), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			},
			{
				JobID:       int64(i*10 + 2),
				UserID:      "test-user",
				Parameters:  []byte(`{"source_id": "` + sourceID + `"}`),
				CustomVal:   "GW",
				WorkspaceId: "test-workspace",
				UUID:        [16]byte{byte(i), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			},
		}

		// Add events to eventsByMessageID
		eventsByMessageID["message-"+sourceID] = types.SingularEventWithReceivedAt{
			SingularEvent: map[string]interface{}{
				"type":      "track",
				"event":     "Product Viewed",
				"productId": "12345",
			},
			ReceivedAt: time.Now(),
		}

		eventsByMessageID["message2-"+sourceID] = types.SingularEventWithReceivedAt{
			SingularEvent: map[string]interface{}{
				"type":      "track",
				"event":     "Product Added",
				"productId": "67890",
			},
			ReceivedAt: time.Now(),
		}
	}

	// Create expected hydrated events for each source
	hydratedEvents := make(map[string][]types.SrcHydrationEvent)
	for i, sourceID := range sources {
		hydratedEvents[sourceID] = append(hydratedEvents[sourceID], types.SrcHydrationEvent{
			ID: strconv.FormatInt(int64(i*10+1), 10),
			Event: map[string]interface{}{
				"type":        "track",
				"event":       "Product Viewed",
				"productId":   "12345",
				"productName": "Test Product " + sourceID,
				"price":       float64(i*10) + 29.99,
			},
		})

		hydratedEvents[sourceID] = append(hydratedEvents[sourceID], types.SrcHydrationEvent{
			ID: strconv.FormatInt(int64(i*10+2), 10),
			Event: map[string]interface{}{
				"type":        "track",
				"event":       "Product Added",
				"productId":   "67890",
				"productName": "Test Product " + sourceID,
				"price":       float64(i*10) + 39.99,
			},
		})
	}

	// Setup mock transformer clients
	transformerClients := transformer.NewSimpleClients()
	transformerClients.WithDynamicSrcHydration(func(ctx context.Context, req types.SrcHydrationRequest) (types.SrcHydrationResponse, error) {
		return types.SrcHydrationResponse{
			Batch: hydratedEvents[req.Source.ID],
		}, nil
	})

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
		eventSchemaJobsBySourceId: eventSchemaJobsBySourceId,
		groupedEventsBySourceId:   groupedEventsBySourceId,
		eventsByMessageID:         eventsByMessageID,
	}

	// Execute the source hydration stage
	result, err := proc.srcHydrationStage("test-partition", message)

	// Assertions
	require.NoError(t, err)
	require.NotNil(t, result)

	// Check that events were hydrated for all sources
	for i, sourceID := range sources {
		hydratedTransformerEvents := result.groupedEventsBySourceId[SourceIDT(sourceID)]
		require.Len(t, hydratedTransformerEvents, 2, "Should have 2 hydrated events for source %s", sourceID)

		// Verify first event was hydrated correctly
		require.Equal(t, "message-"+sourceID, hydratedTransformerEvents[0].Metadata.MessageID)
		require.Equal(t, "Test Product "+sourceID, hydratedTransformerEvents[0].Message["productName"])

		// Verify second event was hydrated correctly
		require.Equal(t, "message2-"+sourceID, hydratedTransformerEvents[1].Metadata.MessageID)
		require.Equal(t, "Test Product "+sourceID, hydratedTransformerEvents[1].Message["productName"])

		// Check that event schema jobs were created
		eventSchemaJobs := result.eventSchemaJobsBySourceId[SourceIDT(sourceID)]
		require.Len(t, eventSchemaJobs, 2, "Should have 2 event schema jobs for source %s", sourceID)
		require.Equal(t, "GW", eventSchemaJobs[0].CustomVal)
		require.Equal(t, "GW", eventSchemaJobs[1].CustomVal)
		require.Equal(t, "test-workspace", eventSchemaJobs[0].WorkspaceId)
		require.Equal(t, "test-workspace", eventSchemaJobs[1].WorkspaceId)
		require.Equal(t, "test-user", eventSchemaJobs[0].UserID)
		require.Equal(t, "test-user", eventSchemaJobs[1].UserID)
		require.Equal(t, jsonparser.GetStringOrEmpty(eventSchemaJobs[0].EventPayload, "productName"), "Test Product "+sourceID)
		require.Equal(t, jsonparser.GetStringOrEmpty(eventSchemaJobs[1].EventPayload, "productName"), "Test Product "+sourceID)
		require.Equal(t, jsonparser.GetFloatOrZero(eventSchemaJobs[0].EventPayload, "price"), float64(i*10)+29.99)
		require.Equal(t, jsonparser.GetFloatOrZero(eventSchemaJobs[1].EventPayload, "price"), float64(i*10)+39.99)

		// Check that eventsByMessageID was updated with hydrated event data
		updatedEvent1, exists1 := result.eventsByMessageID["message-"+sourceID]
		require.True(t, exists1)
		require.Equal(t, "Test Product "+sourceID, updatedEvent1.SingularEvent["productName"])

		updatedEvent2, exists2 := result.eventsByMessageID["message2-"+sourceID]
		require.True(t, exists2)
		require.Equal(t, "Test Product "+sourceID, updatedEvent2.SingularEvent["productName"])
	}
}
