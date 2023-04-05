package schematransformer

import (
	"context"
	"encoding/json"
	"sync"

	"golang.org/x/sync/errgroup"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	proto "github.com/rudderlabs/rudder-server/proto/event-schema"
	"github.com/tidwall/gjson"
)

// EventPayload : Generic type for gateway event payload
type EventPayload struct {
	WriteKey string
	Event    json.RawMessage
}

type SchemaTransformer struct {
	ctx               context.Context
	g                 *errgroup.Group
	backendConfig     backendconfig.BackendConfig
	sourceWriteKeyMap map[string]string
	writeKeyMapLock   sync.RWMutex
}

type Transformer interface {
	Setup(ctx context.Context)
	Transform(job *jobsdb.JobT) ([]byte, error)
}

func New(ctx context.Context, g *errgroup.Group, backendConfig backendconfig.BackendConfig) Transformer {
	return &SchemaTransformer{
		ctx:           ctx,
		g:             g,
		backendConfig: backendConfig,
	}
}

func (st *SchemaTransformer) Setup(ctx context.Context) {
	st.g.Go(func() error {
		st.backendConfigSubscriber(ctx)
		return nil
	})
}

func (st *SchemaTransformer) Transform(job *jobsdb.JobT) ([]byte, error) {
	var eventPayload EventPayload
	err := json.Unmarshal(job.EventPayload, &eventPayload)
	if err != nil {
		return nil, err
	}
	_ = st.getSchemaKeyFromJob(eventPayload)
	return []byte{}, nil
}

func (st *SchemaTransformer) getSchemaKeyFromJob(eventPayload EventPayload) proto.EventSchemaKey {
	eventType := st.getEventType(eventPayload.Event)
	return proto.EventSchemaKey{
		WriteKey:        eventPayload.WriteKey,
		EventType:       eventType,
		EventIdentifier: st.getEventIdentifier(eventPayload.Event, eventType),
	}
}

func (st *SchemaTransformer) backendConfigSubscriber(ctx context.Context) {
	ch := st.backendConfig.Subscribe(ctx, backendconfig.TopicProcessConfig)
	for data := range ch {
		config := data.Data.(map[string]backendconfig.ConfigT)
		sourceWriteKeyMap := map[string]string{}
		for _, wConfig := range config {
			for i := range wConfig.Sources {
				source := &wConfig.Sources[i]
				sourceWriteKeyMap[source.ID] = source.WriteKey
			}
		}
		st.writeKeyMapLock.Lock()
		st.sourceWriteKeyMap = sourceWriteKeyMap
		st.writeKeyMapLock.Unlock()
	}
}

func (st *SchemaTransformer) getEventType(event json.RawMessage) string {
	return gjson.GetBytes(event, "type").Str
}

func (st *SchemaTransformer) getEventIdentifier(event json.RawMessage, eventType string) string {
	eventIdentifier := ""
	if eventType == "track" {
		eventIdentifier = gjson.GetBytes(event, "event").String()
	}
	return eventIdentifier
}
