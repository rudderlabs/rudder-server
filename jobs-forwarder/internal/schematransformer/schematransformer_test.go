package schematransformer

import (
	"context"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/golang/mock/gomock"
	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobs-forwarder/internal/schematransformer/testdata"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
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
			ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{testdata.SampleWorkspaceID: testdata.SampleBackendConfig}, Topic: string(topic)}
			close(ch)
			defer close(closeChan)
			return ch
		})
	schemaTransformer.Setup()
	<-closeChan
	require.Equal(t, schemaTransformer.getEventType(testdata.TrackEvent), "track")
	require.Equal(t, schemaTransformer.getEventType(testdata.IdentifyEvent), "identify")
	require.Equal(t, schemaTransformer.getEventIdentifier(testdata.TrackEvent, "track"), "event-name")
	require.Equal(t, schemaTransformer.getEventIdentifier(testdata.IdentifyEvent, "identify"), "")

	flattenedEvent, err := schemaTransformer.flattenEvent(testdata.CompositeEvent)
	require.Nil(t, err)
	require.Equal(t, flattenedEvent, testdata.CompositeFlattenedEvent)

	flattenedIdentifyEvent, err := schemaTransformer.flattenEvent(testdata.IdentifyEvent)
	require.Nil(t, err)
	require.Equal(t, flattenedIdentifyEvent, testdata.IdentifyFlattenedEvent)

	schema := schemaTransformer.getSchema(testdata.TrackEvent)
	require.Equal(t, schema, testdata.TrackSchema)

	compositeSchema := schemaTransformer.getSchema(testdata.CompositeFlattenedEvent)
	require.Equal(t, compositeSchema, testdata.CompositeSchema)

	require.True(t, schemaTransformer.disablePIIReporting(testdata.WriteKeyEnabled))
	require.Equal(t, schemaTransformer.getSampleEvent(testdata.IdentifyEvent, testdata.WriteKeyEnabled), []byte{})
}
