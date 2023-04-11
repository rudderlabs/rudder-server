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
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	proto "github.com/rudderlabs/rudder-server/proto/event-schema"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/stretchr/testify/require"
)

func Test_SchemaTransformer_NoDataRetention(t *testing.T) {
	conf := config.New()
	g, ctx := errgroup.WithContext(context.Background())
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(gomock.NewController(t))
	closeChan := make(chan struct{})
	schemaTransformer := SchemaTransformer{
		ctx:              ctx,
		g:                g,
		backendConfig:    mockBackendConfig,
		transientSources: transientsource.NewEmptyService(),
		config:           conf,
	}
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{SampleWorkspaceID: SampleBackendConfig}, Topic: string(topic)}
			close(ch)
			defer close(closeChan)
			return ch
		})
	schemaTransformer.Setup()
	<-closeChan
	t.Run("Test getEventType", func(t *testing.T) {
		require.Equal(t, schemaTransformer.getEventType(TrackEvent), "track")
		require.Equal(t, schemaTransformer.getEventType(IdentifyEvent), "identify")
		require.Equal(t, schemaTransformer.getEventIdentifier(TrackEvent, "track"), "event-name")
		require.Equal(t, schemaTransformer.getEventIdentifier(IdentifyEvent, "identify"), "")
	})

	t.Run("Test flattenEvent", func(t *testing.T) {
		flattenedEvent, err := schemaTransformer.flattenEvent(CompositeEvent)
		require.Nil(t, err)
		require.Equal(t, flattenedEvent, CompositeFlattenedEvent)

		flattenedIdentifyEvent, err := schemaTransformer.flattenEvent(IdentifyEvent)
		require.Nil(t, err)
		require.Equal(t, flattenedIdentifyEvent, IdentifyFlattenedEvent)
	})

	t.Run("Test getSchema", func(t *testing.T) {
		schema := schemaTransformer.getSchema(TrackEvent)
		require.Equal(t, schema, TrackSchema)

		compositeSchema := schemaTransformer.getSchema(CompositeFlattenedEvent)
		require.Equal(t, compositeSchema, CompositeSchema)
	})

	t.Run("Test getSampleEvent", func(t *testing.T) {
		require.Equal(t, schemaTransformer.getSampleEvent(IdentifyEvent, WriteKeyEnabled), []byte{})
	})

	t.Run("Test disablePIIReporting", func(t *testing.T) {
		require.True(t, schemaTransformer.disablePIIReporting(WriteKeyEnabled))
	})

	t.Run("Test getSchemaKeyFromJob", func(t *testing.T) {
		require.Equal(t, schemaTransformer.getSchemaKeyFromJob(TestEventPayload), &TestEventSchemaKey)
	})

	t.Run("Test getSchemaMessage", func(t *testing.T) {
		schemaKey := schemaTransformer.getSchemaKeyFromJob(TestEventPayload)
		timeNow := time.Now()
		schemaMessage, err := schemaTransformer.getSchemaMessage(schemaKey, TrackEvent, SampleWorkspaceID, timeNow)
		testEventSchemaMessage := generateTestEventSchemaMessage(timeNow)
		require.Nil(t, err)
		require.Equal(t, schemaMessage, testEventSchemaMessage)
	})

	t.Run("Test Transform", func(t *testing.T) {
		timeNow := time.Now()
		eventSchemaMessage, err := schemaTransformer.Transform(generateTestJob(t, timeNow))
		require.Nil(t, err)
		testSchemaMessage := generateTestEventSchemaMessage(timeNow)
		require.Nil(t, err)
		result, err := proto.UnmarshalEventSchemaMessage(eventSchemaMessage)
		require.Nil(t, err)
		require.Equal(t, result.Schema, testSchemaMessage.Schema)
		require.Equal(t, string(result.Sample), string(testSchemaMessage.Sample))
		require.Equal(t, result.WorkspaceID, testSchemaMessage.WorkspaceID)
		require.Equal(t, result.Key, testSchemaMessage.Key)
		require.Equal(t, result.ObservedAt.AsTime(), testSchemaMessage.ObservedAt.AsTime())
	})
}

func Test_SchemaTransformer_Interface(t *testing.T) {
	conf := config.New()
	g, ctx := errgroup.WithContext(context.Background())
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(gomock.NewController(t))
	closeChan := make(chan struct{})
	schemaTransformer := New(ctx, g, mockBackendConfig, transientsource.NewEmptyService(), conf)
	require.NotNil(t, schemaTransformer)
	mockBackendConfig.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicProcessConfig).
		DoAndReturn(func(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel {
			ch := make(chan pubsub.DataEvent, 1)
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{SampleWorkspaceID: SampleBackendConfig}, Topic: string(topic)}
			close(ch)
			defer close(closeChan)
			return ch
		})
	schemaTransformer.Setup()
	<-closeChan
	t.Run("Test Transform", func(t *testing.T) {
		timeNow := time.Now()
		eventSchemaMessage, err := schemaTransformer.Transform(generateTestJob(t, timeNow))
		require.Nil(t, err)
		testSchemaMessage := generateTestEventSchemaMessage(timeNow)
		require.Nil(t, err)
		result, err := proto.UnmarshalEventSchemaMessage(eventSchemaMessage)
		require.Nil(t, err)
		require.Equal(t, result.Schema, testSchemaMessage.Schema)
		require.Equal(t, string(result.Sample), string(testSchemaMessage.Sample))
		require.Equal(t, result.WorkspaceID, testSchemaMessage.WorkspaceID)
		require.Equal(t, result.Key, testSchemaMessage.Key)
		require.Equal(t, result.ObservedAt.AsTime(), testSchemaMessage.ObservedAt.AsTime())
	})
}

func generateTestEventSchemaMessage(time time.Time) *proto.EventSchemaMessage {
	return &proto.EventSchemaMessage{
		WorkspaceID: SampleWorkspaceID,
		Key:         &TestEventSchemaKey,
		ObservedAt:  timestamppb.New(time),
		Schema:      TrackSchema,
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
		WorkspaceId:   SampleWorkspaceID,
		Parameters:    []byte{},
	}
}
