package transformer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

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
		backendConfig:             mockBackendConfig,
		conf:                      config.New(),
		identifierLimit:           10000,
		keysLimit:                 10000,
		collapseArrayIndicesCache: make(map[string]*config.Reloadable[bool]),
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
		flattenedEvent, err := schemaTransformer.flattenEvent(testdata.SourceIDEnabled, testdata.CompositeEvent)
		require.Nil(t, err)
		require.Equal(t, flattenedEvent, testdata.CompositeFlattenedEvent)

		flattenedIdentifyEvent, err := schemaTransformer.flattenEvent(testdata.SourceIDEnabled, testdata.IdentifyEvent)
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
		require.Equal(t, schemaTransformer.getSchemaKeyFromJob(testdata.TrackEvent, testdata.WriteKeyEnabled), &testdata.TestEventSchemaKey)
	})

	t.Run("Test writeKeyForSourceID", func(t *testing.T) {
		require.Equal(t, schemaTransformer.writeKeyForSourceID(testdata.SourceIDEnabled), testdata.WriteKeyEnabled)
		require.Empty(t, schemaTransformer.writeKeyForSourceID(""))
	})

	t.Run("Test getSchemaMessage", func(t *testing.T) {
		schemaKey := schemaTransformer.getSchemaKeyFromJob(testdata.TrackEvent, testdata.WriteKeyEnabled)
		timeNow := time.Now()
		schemaMessage, err := schemaTransformer.getSchemaMessage(schemaKey, testdata.SourceIDEnabled, testdata.TrackEvent, []byte{}, testdata.SampleWorkspaceID, timeNow)
		testEventSchemaMessage := generateTestEventSchemaMessage(timeNow)
		require.Nil(t, err)
		require.Equal(t, schemaMessage, testEventSchemaMessage)
	})

	t.Run("Test Transform", func(t *testing.T) {
		timeNow := time.Now()
		eventSchemaMessage, sourceID, err := schemaTransformer.Transform(generateTestJob(t, timeNow))
		require.Nil(t, err)
		require.Equal(t, testdata.SourceIDEnabled, sourceID)
		testSchemaMessage := generateTestEventSchemaMessage(timeNow)
		require.Nil(t, err)
		require.Equal(t, eventSchemaMessage.Schema, testSchemaMessage.Schema)
		require.Equal(t, string(eventSchemaMessage.Sample), string(testSchemaMessage.Sample))
		require.Equal(t, eventSchemaMessage.WorkspaceID, testSchemaMessage.WorkspaceID)
		require.Equal(t, eventSchemaMessage.Key, testSchemaMessage.Key)
		require.Equal(t, eventSchemaMessage.ObservedAt.AsTime(), testSchemaMessage.ObservedAt.AsTime())
	})

	t.Run("Test Transform limits", func(t *testing.T) {
		event1 := generateTestJob(t, time.Now())
		event1.EventPayload = fmt.Appendf(nil, `{"type": "track", "event": %q}`, rand.String(schemaTransformer.identifierLimit+1))

		e, _, err := schemaTransformer.Transform(event1)
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

		e, _, err = schemaTransformer.Transform(event2)
		require.Nil(t, e)
		require.Error(t, err)
		require.ErrorContains(t, err, "event schema has more than")
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
		eventSchemaMessage, _, err := schemaTransformer.Transform(generateTestJob(t, timeNow))
		require.Nil(t, err)
		testSchemaMessage := generateTestEventSchemaMessage(timeNow)
		require.Nil(t, err)
		require.Equal(t, eventSchemaMessage.Schema, testSchemaMessage.Schema)
		require.Equal(t, string(eventSchemaMessage.Sample), string(testSchemaMessage.Sample))
		require.Equal(t, eventSchemaMessage.WorkspaceID, testSchemaMessage.WorkspaceID)
		require.Equal(t, eventSchemaMessage.Key, testSchemaMessage.Key)
		require.Equal(t, eventSchemaMessage.ObservedAt.AsTime(), testSchemaMessage.ObservedAt.AsTime())
	})
}

func Test_SchemaTransformer_CollapseArrayIndices(t *testing.T) {
	tests := []struct {
		name          string
		event         map[string]any
		captureNil    bool
		wantCollapsed map[string]string // schema with collapseArrayIndices on
		wantFlattened map[string]string // schema with collapseArrayIndices off
	}{
		{
			name: "array of objects",
			event: map[string]any{"products": []any{
				map[string]any{"name": "a", "price": 1.5},
				map[string]any{"name": "b", "price": 2.5},
			}},
			wantCollapsed: map[string]string{"products.0.name": "string", "products.0.price": "float64"},
			wantFlattened: map[string]string{
				"products.0.name": "string", "products.0.price": "float64",
				"products.1.name": "string", "products.1.price": "float64",
			},
		},
		{
			name:          "array of primitives",
			event:         map[string]any{"context": map[string]any{"allowedConsentIds": []any{"a", "b", "c", "d"}}},
			wantCollapsed: map[string]string{"context.allowedConsentIds.0": "string"},
			wantFlattened: map[string]string{
				"context.allowedConsentIds.0": "string", "context.allowedConsentIds.1": "string",
				"context.allowedConsentIds.2": "string", "context.allowedConsentIds.3": "string",
			},
		},
		{
			name: "nested arrays",
			event: map[string]any{
				"matrix": []any{[]any{}, []any{1, 2}},
				"orders": []any{
					map[string]any{"items": []any{map[string]any{"sku": "a"}}},
					map[string]any{"items": []any{map[string]any{"sku": "b"}, map[string]any{"sku": "c", "qty": 2}}},
				},
			},
			wantCollapsed: map[string]string{"matrix.0.0": "int", "orders.0.items.0.sku": "string", "orders.0.items.0.qty": "int"},
			wantFlattened: map[string]string{
				"matrix.1.0": "int", "matrix.1.1": "int",
				"orders.0.items.0.sku": "string",
				"orders.1.items.0.sku": "string", "orders.1.items.1.sku": "string", "orders.1.items.1.qty": "int",
			},
		},
		{
			name:          "mixed primitive types: the first element wins",
			event:         map[string]any{"values": []any{1, 2.5, "x", true}},
			wantCollapsed: map[string]string{"values.0": "int"},
			wantFlattened: map[string]string{"values.0": "int", "values.1": "float64", "values.2": "string", "values.3": "bool"},
		},
		{
			name: "mixed object types: the first element that has the key wins",
			event: map[string]any{"products": []any{
				map[string]any{"name": "a"},
				map[string]any{"name": 1, "id": 1.5},
				map[string]any{"name": true, "id": "x"},
			}},
			wantCollapsed: map[string]string{"products.0.name": "string", "products.0.id": "float64"},
			wantFlattened: map[string]string{
				"products.0.name": "string",
				"products.1.name": "int", "products.1.id": "float64",
				"products.2.name": "bool", "products.2.id": "string",
			},
		},
		{
			name: "numeric map keys are kept",
			event: map[string]any{
				"stats": map[string]any{"2024": map[string]any{"count": 1}, "2025": map[string]any{"count": 2}},
				"list":  []any{map[string]any{"7": "x"}, map[string]any{"8": "y"}},
			},
			wantCollapsed: map[string]string{"stats.2024.count": "int", "stats.2025.count": "int", "list.0.7": "string", "list.0.8": "string"},
			wantFlattened: map[string]string{"stats.2024.count": "int", "stats.2025.count": "int", "list.0.7": "string", "list.1.8": "string"},
		},
		{
			name:          "empty array, empty map and nil",
			event:         map[string]any{"emptyArray": []any{}, "emptyMap": map[string]any{}, "nil": nil, "nested": map[string]any{"emptyArray": []any{}, "nil": nil, "a": "x"}},
			wantCollapsed: map[string]string{"nested.a": "string"},
			wantFlattened: map[string]string{"nested.a": "string"},
		},
		{
			name:          "empty array, empty map and nil with captureNilAsUnknowns",
			event:         map[string]any{"emptyArray": []any{}, "emptyMap": map[string]any{}, "nil": nil, "nested": map[string]any{"emptyArray": []any{}, "nil": nil, "a": "x"}},
			captureNil:    true,
			wantCollapsed: map[string]string{"nil": "unknown", "nested.nil": "unknown", "nested.a": "string"},
			wantFlattened: map[string]string{"nil": "unknown", "nested.nil": "unknown", "nested.a": "string"},
		},
		{
			name:          "nil does not claim the key",
			event:         map[string]any{"products": []any{map[string]any{"id": nil}, map[string]any{"id": 1.5}, map[string]any{"id": nil}}},
			wantCollapsed: map[string]string{"products.0.id": "float64"},
			wantFlattened: map[string]string{"products.1.id": "float64"},
		},
		{
			name:          "nil does not claim the key with captureNilAsUnknowns",
			event:         map[string]any{"products": []any{map[string]any{"id": nil}, map[string]any{"id": 1.5}, map[string]any{"id": nil}}},
			captureNil:    true,
			wantCollapsed: map[string]string{"products.0.id": "float64"},
			wantFlattened: map[string]string{"products.0.id": "unknown", "products.1.id": "float64", "products.2.id": "unknown"},
		},
		{
			name:          "nil in every element",
			event:         map[string]any{"products": []any{map[string]any{"id": nil}, map[string]any{"id": nil}}},
			wantCollapsed: map[string]string{},
			wantFlattened: map[string]string{},
		},
		{
			name:          "nil in every element with captureNilAsUnknowns",
			event:         map[string]any{"products": []any{map[string]any{"id": nil}, map[string]any{"id": nil}}},
			captureNil:    true,
			wantCollapsed: map[string]string{"products.0.id": "unknown"},
			wantFlattened: map[string]string{"products.0.id": "unknown", "products.1.id": "unknown"},
		},
		{
			name:          "event without arrays",
			event:         testdata.CompositeEvent,
			wantCollapsed: testdata.CompositeSchema,
			wantFlattened: testdata.CompositeSchema,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, collapse := range []bool{true, false} {
				conf := config.New()
				conf.Set("EventSchemas.captureUnknowns", tc.captureNil)
				conf.Set("SchemaForwarder.collapseArrayIndices", collapse)
				st := New(nil, conf).(*transformer)
				sample := []byte(`{"sample":true}`)
				msg, err := st.getSchemaMessage(&testdata.TestEventSchemaKey, testdata.SourceIDEnabled, tc.event, sample, testdata.SampleWorkspaceID, time.Now())
				require.NoError(t, err)
				want := tc.wantFlattened
				if collapse {
					want = tc.wantCollapsed
				}
				require.Equal(t, want, msg.Schema, "collapseArrayIndices=%t", collapse)
				require.Equal(t, proto.SchemaHash(want), msg.Hash)
				require.Equal(t, sample, msg.Sample)
			}
		})
	}

	t.Run("keysLimit applies to the collapsed keys", func(t *testing.T) {
		sameKeys := map[string]any{"products": []any{map[string]any{"name": "a"}, map[string]any{"name": "b"}, map[string]any{"name": "c"}}}
		distinctKeys := map[string]any{"products": []any{map[string]any{"name": "a"}, map[string]any{"id": 1}, map[string]any{"sku": "x"}}}
		for _, tc := range []struct {
			event    map[string]any
			collapse bool
			wantErr  bool
		}{
			{event: sameKeys, collapse: false, wantErr: true},
			{event: sameKeys, collapse: true, wantErr: false},
			{event: distinctKeys, collapse: true, wantErr: true},
		} {
			conf := config.New()
			conf.Set("EventSchemas.keysLimit", 2)
			conf.Set("SchemaForwarder.collapseArrayIndices", tc.collapse)
			st := New(nil, conf).(*transformer)
			_, err := st.getSchemaMessage(&testdata.TestEventSchemaKey, testdata.SourceIDEnabled, tc.event, []byte("{}"), testdata.SampleWorkspaceID, time.Now())
			if tc.wantErr {
				require.ErrorContains(t, err, "event schema has more than 2 keys")
			} else {
				require.NoError(t, err)
			}
		}
	})

	t.Run("setting per source or global", func(t *testing.T) {
		event := map[string]any{"products": []any{map[string]any{"name": "a"}, map[string]any{"name": "b"}}}
		collapsed := map[string]any{"products.0.name": "a"}
		flattened := map[string]any{"products.0.name": "a", "products.1.name": "b"}
		// The steps share one transformer, so each step also checks that a changed setting applies after first use.
		conf := config.New()
		st := New(nil, conf).(*transformer)
		for _, step := range []struct {
			name string
			set  map[string]bool
			want map[string]map[string]any // sourceID -> flattened event
		}{
			{
				name: "no setting",
				want: map[string]map[string]any{"source-1": flattened, "source-2": flattened},
			},
			{
				name: "global on",
				set:  map[string]bool{"SchemaForwarder.collapseArrayIndices": true},
				want: map[string]map[string]any{"source-1": collapsed, "source-2": collapsed},
			},
			{
				name: "per-source off overrides global on",
				set:  map[string]bool{"SchemaForwarder.source-1.collapseArrayIndices": false},
				want: map[string]map[string]any{"source-1": flattened, "source-2": collapsed},
			},
			{
				name: "per-source on, global off",
				set:  map[string]bool{"SchemaForwarder.source-1.collapseArrayIndices": true, "SchemaForwarder.collapseArrayIndices": false},
				want: map[string]map[string]any{"source-1": collapsed, "source-2": flattened},
			},
		} {
			for k, v := range step.set {
				conf.Set(k, v)
			}
			for sourceID, want := range step.want {
				got, err := st.flattenEvent(sourceID, event)
				require.NoError(t, err)
				require.Equal(t, want, got, "%s: %s", step.name, sourceID)
			}
		}
	})

	t.Run("arrays differing only in length share a hash", func(t *testing.T) {
		job := func(products string) *jobsdb.JobT {
			return &jobsdb.JobT{
				EventPayload: fmt.Appendf(nil, `{"type":"track","event":"e","properties":{"products":%s}}`, products),
				Parameters:   testdata.TestParams,
			}
		}
		for _, collapse := range []bool{true, false} {
			conf := config.New()
			conf.Set("SchemaForwarder."+testdata.SourceIDEnabled+".collapseArrayIndices", collapse)
			st := New(nil, conf).(*transformer)
			st.sourceWriteKeyMap = map[string]string{testdata.SourceIDEnabled: testdata.WriteKeyEnabled}
			short, _, err := st.Transform(job(`[{"price":1}]`))
			require.NoError(t, err)
			long, _, err := st.Transform(job(`[{"price":1},{"price":2.5}]`))
			require.NoError(t, err)
			require.Equal(t, collapse, short.Hash == long.Hash, "collapseArrayIndices=%t", collapse)
		}
	})
}

func generateTestEventSchemaMessage(time time.Time) *proto.EventSchemaMessage {
	return &proto.EventSchemaMessage{
		WorkspaceID: testdata.SampleWorkspaceID,
		Key:         &testdata.TestEventSchemaKey,
		ObservedAt:  timestamppb.New(time),
		Schema:      testdata.TrackSchema,
		Hash:        proto.SchemaHash(testdata.TrackSchema),
		Sample:      []byte("{}"),
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
