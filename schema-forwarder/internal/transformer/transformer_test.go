package transformer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	proto "github.com/rudderlabs/rudder-server/proto/event-schema"
	"github.com/rudderlabs/rudder-server/schema-forwarder/internal/testdata"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

func Test_SchemaTransformer_NoDataRetention(t *testing.T) {
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(gomock.NewController(t))
	schemaTransformer := transformer{
		backendConfig:   mockBackendConfig,
		identifierLimit: 10000,
		keysLimit:       10000,
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

	t.Run("Test disablePIIReporting", func(t *testing.T) {
		require.True(t, schemaTransformer.disablePIIReporting(testdata.WriteKeyEnabled))
	})

	t.Run("Test getSchemaKeyFromJob", func(t *testing.T) {
		expectedKey := &proto.EventSchemaKey{
			WriteKey:        testdata.WriteKeyEnabled,
			EventType:       "track",
			EventIdentifier: "event-name",
		}
		require.Equal(t, schemaTransformer.getSchemaKeyFromJob(testdata.TrackEvent, testdata.WriteKeyEnabled), expectedKey)
	})

	t.Run("Test getWriteKeyAndSourceIdFromParams", func(t *testing.T) {
		writeKey, sourceId := schemaTransformer.getWriteKeyAndSourceIdFromParams(testdata.TestParams)
		require.Equal(t, writeKey, testdata.WriteKeyEnabled)
		require.Equal(t, sourceId, testdata.SourceIDEnabled)

		writeKey, sourceId = schemaTransformer.getWriteKeyAndSourceIdFromParams([]byte(`{}`))
		require.Equal(t, writeKey, "")
		require.Equal(t, sourceId, "")

		writeKey, sourceId = schemaTransformer.getWriteKeyAndSourceIdFromParams([]byte(`{"source_id": ""}`))
		require.Equal(t, writeKey, "")
		require.Equal(t, sourceId, "")

		writeKey, sourceId = schemaTransformer.getWriteKeyAndSourceIdFromParams([]byte(`{"source_id": "non-existent-source"}`))
		require.Equal(t, writeKey, "")
		require.Equal(t, sourceId, "non-existent-source")
	})

	t.Run("Test getSchemaMessage", func(t *testing.T) {
		schemaKey := schemaTransformer.getSchemaKeyFromJob(testdata.TrackEvent, testdata.WriteKeyEnabled)
		timeNow := time.Now()
		schemaMessage, err := schemaTransformer.getSchemaMessage(schemaKey, testdata.TrackEvent, []byte{}, testdata.SampleWorkspaceID, testdata.SourceIDEnabled, timeNow)
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
		require.Equal(t, eventSchemaMessage.Schema, testSchemaMessage.Schema)
		require.Equal(t, string(eventSchemaMessage.Sample), string(testSchemaMessage.Sample))
		require.Equal(t, eventSchemaMessage.WorkspaceID, testSchemaMessage.WorkspaceID)
		require.Equal(t, eventSchemaMessage.Key, testSchemaMessage.Key)
		require.Equal(t, eventSchemaMessage.SourceId, testSchemaMessage.SourceId)
		require.Equal(t, eventSchemaMessage.ObservedAt.AsTime(), testSchemaMessage.ObservedAt.AsTime())
	})

	t.Run("Test Transform limits", func(t *testing.T) {
		event1 := generateTestJob(t, time.Now())
		event1.EventPayload = []byte(fmt.Sprintf(`{"type": "track", "event": %q}`, rand.String(schemaTransformer.identifierLimit+1)))

		e, err := schemaTransformer.Transform(event1)
		require.Nil(t, e)
		require.Error(t, err)
		require.ErrorContains(t, err, "event identifier size is greater than")

		event2 := generateTestJob(t, time.Now())
		payload := map[string]string{
			"type": "identify",
		}
		for i := 0; i < schemaTransformer.keysLimit; i++ {
			payload[fmt.Sprintf("key-%d", i)] = "value"
		}
		event2.EventPayload, err = jsonrs.Marshal(payload)
		require.NoError(t, err)

		e, err = schemaTransformer.Transform(event2)
		require.Nil(t, e)
		require.Error(t, err)
		require.ErrorContains(t, err, "event schema has more than")
	})

	t.Run("Test Transform missing sourceId", func(t *testing.T) {
		event := generateTestJob(t, time.Now())
		event.Parameters = []byte(`{}`)

		e, err := schemaTransformer.Transform(event)
		require.Nil(t, e)
		require.Error(t, err)
		require.ErrorContains(t, err, "writeKey or sourceId could not be found")
	})

	t.Run("Test Transform missing writeKey", func(t *testing.T) {
		event := generateTestJob(t, time.Now())
		event.Parameters = []byte(`{"source_id": "non-existent-source"}`)

		e, err := schemaTransformer.Transform(event)
		require.Nil(t, e)
		require.Error(t, err)
		require.ErrorContains(t, err, "writeKey or sourceId could not be found")
	})
}

func Test_SchemaTransformer_Interface(t *testing.T) {
	conf := config.New()
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(gomock.NewController(t))
	closeChan := make(chan struct{})
	schemaTransformer := New(mockBackendConfig, conf)
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
		eventSchemaMessage, err := schemaTransformer.Transform(generateTestJob(t, timeNow))
		require.Nil(t, err)
		testSchemaMessage := generateTestEventSchemaMessage(timeNow)
		require.Nil(t, err)
		require.Equal(t, eventSchemaMessage.Schema, testSchemaMessage.Schema)
		require.Equal(t, string(eventSchemaMessage.Sample), string(testSchemaMessage.Sample))
		require.Equal(t, eventSchemaMessage.WorkspaceID, testSchemaMessage.WorkspaceID)
		require.Equal(t, eventSchemaMessage.Key, testSchemaMessage.Key)
		require.Equal(t, eventSchemaMessage.SourceId, testSchemaMessage.SourceId)
		require.Equal(t, eventSchemaMessage.ObservedAt.AsTime(), testSchemaMessage.ObservedAt.AsTime())
	})
}

func generateTestEventSchemaMessage(time time.Time) *proto.EventSchemaMessage {
	return &proto.EventSchemaMessage{
		WorkspaceID: testdata.SampleWorkspaceID,
		Key: &proto.EventSchemaKey{
			WriteKey:        testdata.WriteKeyEnabled,
			EventType:       "track",
			EventIdentifier: "event-name",
		},
		ObservedAt: timestamppb.New(time),
		Schema:     testdata.TrackSchema,
		Hash:       proto.SchemaHash(testdata.TrackSchema),
		Sample:     []byte("{}"),
		SourceId:   testdata.SourceIDEnabled,
	}
}

func generateTestJob(t *testing.T, time time.Time) *jobsdb.JobT {
	eventPayload, err := jsonrs.Marshal(testdata.TrackEvent)
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
		LastJobStatus: jobsdb.JobStatusT{},
		WorkspaceId:   testdata.SampleWorkspaceID,
		Parameters:    []byte(`{"source_id": "enabled-source"}`),
	}
}
