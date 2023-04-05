package schematransformer

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jeremywohl/flatten"
	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	proto "github.com/rudderlabs/rudder-server/proto/event-schema"
	"github.com/rudderlabs/rudder-server/services/transientsource"
)

// EventPayload : Generic type for gateway event payload
type EventPayload struct {
	WriteKey string
	Event    map[string]interface{}
}

type SchemaTransformer struct {
	ctx                        context.Context
	g                          *errgroup.Group
	backendConfig              backendconfig.BackendConfig
	transientSources           transientsource.Service
	sourceWriteKeyMap          map[string]string
	newPIIReportingSettings    map[string]bool
	writeKeyMapLock            sync.RWMutex
	writeKeySourceIDMap        map[string]string
	config                     *config.Config
	shouldCaptureNilAsUnknowns bool
}

type Transformer interface {
	Setup(ctx context.Context)
	Transform(job *jobsdb.JobT) ([]byte, error)
}

func New(ctx context.Context, g *errgroup.Group, backendConfig backendconfig.BackendConfig, transientSources transientsource.Service, config *config.Config) Transformer {
	return &SchemaTransformer{
		ctx:                        ctx,
		g:                          g,
		backendConfig:              backendConfig,
		transientSources:           transientSources,
		config:                     config,
		shouldCaptureNilAsUnknowns: config.GetBool("EventSchemas.captureUnknowns", false),
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
		return []byte{}, err
	}
	schemaKey := st.getSchemaKeyFromJob(eventPayload)
	schemaMessage, err := st.getSchemaMessage(schemaKey, eventPayload.Event, job.WorkspaceId, job.CreatedAt)
	if err != nil {
		return []byte{}, err
	}
	result, err := schemaMessage.MustMarshal()
	if err != nil {
		return []byte{}, err
	}
	return result, nil
}

func (st *SchemaTransformer) getSchemaKeyFromJob(eventPayload EventPayload) *proto.EventSchemaKey {
	eventType := st.getEventType(eventPayload.Event)
	return &proto.EventSchemaKey{
		WriteKey:        eventPayload.WriteKey,
		EventType:       eventType,
		EventIdentifier: st.getEventIdentifier(eventPayload.Event, eventType),
	}
}

func (st *SchemaTransformer) backendConfigSubscriber(ctx context.Context) {
	ch := st.backendConfig.Subscribe(ctx, backendconfig.TopicProcessConfig)
	for data := range ch {
		configData := data.Data.(map[string]backendconfig.ConfigT)
		sourceWriteKeyMap := map[string]string{}
		writeKeySourceIDMap := map[string]string{}
		newPIIReportingSettings := map[string]bool{}
		for _, wConfig := range configData {
			for i := range wConfig.Sources {
				source := &wConfig.Sources[i]
				sourceWriteKeyMap[source.ID] = source.WriteKey
				writeKeySourceIDMap[source.WriteKey] = source.ID
				newPIIReportingSettings[source.WriteKey] = wConfig.Settings.DataRetention.DisableReportingPII
			}
		}
		st.writeKeyMapLock.Lock()
		st.sourceWriteKeyMap = sourceWriteKeyMap
		st.newPIIReportingSettings = newPIIReportingSettings
		st.writeKeySourceIDMap = writeKeySourceIDMap
		st.writeKeyMapLock.Unlock()
	}
}

func (st *SchemaTransformer) getEventType(event map[string]interface{}) string {
	eventType, ok := event["type"].(string)
	if !ok {
		return ""
	}
	return eventType
}

func (st *SchemaTransformer) getEventIdentifier(event map[string]interface{}, eventType string) string {
	eventIdentifier := ""
	if eventType == "track" {
		eventIdentifier, ok := event["event"].(string)
		if !ok {
			return ""
		}
		return eventIdentifier
	}
	return eventIdentifier
}

func (st *SchemaTransformer) getSchemaMessage(key *proto.EventSchemaKey, event map[string]interface{}, workspaceId string, observedAt time.Time) (*proto.EventSchemaMessage, error) {
	flattenedEvent, err := st.flattenEvent(event)
	if err != nil {
		return nil, err
	}
	schema := st.getSchema(flattenedEvent)
	sampleEvent := st.getSampleEvent(event, key.WriteKey)
	return &proto.EventSchemaMessage{
		WorkspaceID: workspaceId,
		Key:         key,
		ObservedAt:  timestamppb.New(observedAt),
		Schema:      schema,
		Sample:      sampleEvent,
	}, nil
}

func (st *SchemaTransformer) getSchema(flattenedEvent map[string]interface{}) map[string]string {
	schema := make(map[string]string)
	for k, v := range flattenedEvent {
		reflectType := reflect.TypeOf(v)
		if reflectType != nil {
			schema[k] = reflectType.String()
		} else if !(v == nil && !st.shouldCaptureNilAsUnknowns) {
			schema[k] = "unknown"
		}
	}
	return schema
}

func (st *SchemaTransformer) flattenEvent(event map[string]interface{}) (map[string]interface{}, error) {
	flattenedEvent, err := flatten.Flatten(event, "", flatten.DotStyle)
	if err != nil {
		return nil, err
	}
	return flattenedEvent, nil
}

func (st *SchemaTransformer) disablePIIReporting(writeKey string) bool {
	st.writeKeyMapLock.RLock()
	defer st.writeKeyMapLock.RUnlock()
	return st.newPIIReportingSettings[writeKey] && st.transientSources.Apply(st.writeKeySourceIDMap[writeKey])
}

func (st *SchemaTransformer) getSampleEvent(event map[string]interface{}, writeKey string) []byte {
	if st.disablePIIReporting(writeKey) {
		return []byte{}
	}
	sampleEvent, err := json.Marshal(event)
	if err != nil {
		return []byte{}
	}
	return sampleEvent
}
