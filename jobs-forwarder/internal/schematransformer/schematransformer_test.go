package schematransformer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/testdata"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	proto "github.com/rudderlabs/rudder-server/proto/event-schema"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/stretchr/testify/require"
)

var TestEventPayload = EventPayload{
	Event: testdata.TrackEvent,
}

func Test_SchemaTransformer_NoDataRetention(t *testing.T) {
	conf := config.New()
	g, ctx := errgroup.WithContext(context.Background())
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(gomock.NewController(t))
	schemaTransformer := SchemaTransformer{
		ctx:           ctx,
		g:             g,
		backendConfig: mockBackendConfig,
		config:        conf,
	}
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{testdata.SampleWorkspaceID: testdata.SampleBackendConfig}, Topic: string(topic)}
			close(ch)
			return ch
		})
	schemaTransformer.Start()
	defer schemaTransformer.Stop()
	t.Run("Test getEventType", func(t *testing.T) {
		require.Equal(t, schemaTransformer.getEventType(testdata.TrackEvent), "track")
		require.Equal(t, schemaTransformer.getEventType(testdata.IdentifyEvent), "identify")
		require.Equal(t, schemaTransformer.getEventIdentifier(testdata.TrackEvent, "track"), "event-name")
		require.Equal(t, schemaTransformer.getEventIdentifier(testdata.IdentifyEvent, "identify"), "")
	})

	t.Run("Test flattenEvent", func(t *testing.T) {
		flattenedEvent, err := schemaTransformer.flattenEvent(testdata.CompositeEvent)
		require.Nil(t, err)
		require.Equal(t, flattenedEvent, testdata.CompositeFlattenedEvent)

		flattenedIdentifyEvent, err := schemaTransformer.flattenEvent(testdata.IdentifyEvent)
		require.Nil(t, err)
		require.Equal(t, flattenedIdentifyEvent, testdata.IdentifyFlattenedEvent)
	})

	t.Run("Test getSchema", func(t *testing.T) {
		schema := schemaTransformer.getSchema(testdata.TrackEvent)
		require.Equal(t, schema, testdata.TrackSchema)

		compositeSchema := schemaTransformer.getSchema(testdata.CompositeFlattenedEvent)
		require.Equal(t, compositeSchema, testdata.CompositeSchema)
	})

	t.Run("Test getSampleEvent", func(t *testing.T) {
		require.Equal(t, schemaTransformer.getSampleEvent(testdata.IdentifyEvent, testdata.WriteKeyEnabled), []byte{})
	})

	t.Run("Test disablePIIReporting", func(t *testing.T) {
		require.True(t, schemaTransformer.disablePIIReporting(testdata.WriteKeyEnabled))
	})

	t.Run("Test getSchemaKeyFromJob", func(t *testing.T) {
		require.Equal(t, schemaTransformer.getSchemaKeyFromJob(TestEventPayload, testdata.WriteKeyEnabled), &testdata.TestEventSchemaKey)
	})

	t.Run("Test getWriteKeyFromParams", func(t *testing.T) {
		require.Equal(t, schemaTransformer.getWriteKeyFromParams(testdata.TestParams), testdata.WriteKeyEnabled)
	})

	t.Run("Test getSchemaMessage", func(t *testing.T) {
		schemaKey := schemaTransformer.getSchemaKeyFromJob(TestEventPayload, testdata.WriteKeyEnabled)
		timeNow := time.Now()
		schemaMessage, err := schemaTransformer.getSchemaMessage(schemaKey, testdata.TrackEvent, testdata.SampleWorkspaceID, timeNow)
		testEventSchemaMessage := generateTestEventSchemaMessage(timeNow)
		require.Nil(t, err)
		require.Equal(t, schemaMessage, testEventSchemaMessage)
	})

	t.Run("Test Transform", func(t *testing.T) {
		timeNow := time.Now()
		eventSchemaMessage, writeKey, err := schemaTransformer.Transform(generateTestJob(t, timeNow))
		require.Nil(t, err)
		require.Equal(t, writeKey, testdata.WriteKeyEnabled)
		testSchemaMessage := generateTestEventSchemaMessage(timeNow)
		require.Nil(t, err)
		require.Equal(t, eventSchemaMessage.Schema, testSchemaMessage.Schema)
		require.Equal(t, string(eventSchemaMessage.Sample), string(testSchemaMessage.Sample))
		require.Equal(t, eventSchemaMessage.WorkspaceID, testSchemaMessage.WorkspaceID)
		require.Equal(t, eventSchemaMessage.Key, testSchemaMessage.Key)
		require.Equal(t, eventSchemaMessage.ObservedAt.AsTime(), testSchemaMessage.ObservedAt.AsTime())
	})
}

func Test_SchemaTransformer_Interface(t *testing.T) {
	conf := config.New()
	g, ctx := errgroup.WithContext(context.Background())
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(gomock.NewController(t))
	closeChan := make(chan struct{})
	schemaTransformer := New(ctx, g, mockBackendConfig, conf)
	require.NotNil(t, schemaTransformer)
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{testdata.SampleWorkspaceID: testdata.SampleBackendConfig}, Topic: string(topic)}
			close(ch)
			defer close(closeChan)
			return ch
		})
	schemaTransformer.Start()
	defer schemaTransformer.Stop()
	<-closeChan
	t.Run("Test Transform", func(t *testing.T) {
		timeNow := time.Now()
		eventSchemaMessage, writeKey, err := schemaTransformer.Transform(generateTestJob(t, timeNow))
		require.Nil(t, err)
		require.Equal(t, writeKey, testdata.WriteKeyEnabled)
		testSchemaMessage := generateTestEventSchemaMessage(timeNow)
		require.Nil(t, err)
		require.Equal(t, eventSchemaMessage.Schema, testSchemaMessage.Schema)
		require.Equal(t, string(eventSchemaMessage.Sample), string(testSchemaMessage.Sample))
		require.Equal(t, eventSchemaMessage.WorkspaceID, testSchemaMessage.WorkspaceID)
		require.Equal(t, eventSchemaMessage.Key, testSchemaMessage.Key)
		require.Equal(t, eventSchemaMessage.ObservedAt.AsTime(), testSchemaMessage.ObservedAt.AsTime())
	})
}

func generateTestEventSchemaMessage(time time.Time) *proto.EventSchemaMessage {
	return &proto.EventSchemaMessage{
		WorkspaceID: testdata.SampleWorkspaceID,
		Key:         &testdata.TestEventSchemaKey,
		ObservedAt:  timestamppb.New(time),
		Schema:      testdata.TrackSchema,
		Sample:      []byte{},
	}
}

func generateTestJob(t *testing.T, time time.Time) *jobsdb.JobT {
	eventPayload, err := json.Marshal(TestEventPayload)
	require.Nil(t, err)
	jobUUID, err := uuid.NewUUID()
	require.Nil(t, err)
	return &jobsdb.JobT{
		EventPayload:  eventPayload,
		JobID:         1,
		UUID:          jobUUID,
		CreatedAt:     time,
		ExpireAt:      time,
		CustomVal:     "event-schema",
		EventCount:    1,
		PayloadSize:   100,
		LastJobStatus: jobsdb.JobStatusT{},
		WorkspaceId:   testdata.SampleWorkspaceID,
		Parameters:    []byte(`{"source_id": "enabled-source"}`),
	}
}
