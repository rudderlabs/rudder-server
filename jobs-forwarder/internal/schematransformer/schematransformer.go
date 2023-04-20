package schematransformer

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jeremywohl/flatten"
	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	proto "github.com/rudderlabs/rudder-server/proto/event-schema"
	"github.com/tidwall/gjson"
)

type Transformer interface {
	Start()
	Transform(job *jobsdb.JobT) (*proto.EventSchemaMessage, string, error)
	Stop()
}

func New(ctx context.Context, g *errgroup.Group, backendConfig backendconfig.BackendConfig, config *config.Config) Transformer {
	return &SchemaTransformer{
		ctx:                        ctx,
		g:                          g,
		backendConfig:              backendConfig,
		config:                     config,
		shouldCaptureNilAsUnknowns: config.GetBool("EventSchemas.captureUnknowns", false),
		initialised:                make(chan struct{}),
	}
}

func (st *SchemaTransformer) Start() {
	st.g.Go(func() error {
		st.backendConfigSubscriber(st.ctx)
		return nil
	})
	<-st.initialised
}

func (st *SchemaTransformer) Stop() {
}

func (st *SchemaTransformer) Transform(job *jobsdb.JobT) (*proto.EventSchemaMessage, string, error) {
	var eventPayload map[string]interface{}
	if err := json.Unmarshal(job.EventPayload, &eventPayload); err != nil {
		return &proto.EventSchemaMessage{}, "", err
	}
	writeKey := st.getWriteKeyFromParams(job.Parameters)
	schemaKey := st.getSchemaKeyFromJob(eventPayload, writeKey)
	schemaMessage, err := st.getSchemaMessage(schemaKey, eventPayload, job.EventPayload, job.WorkspaceId, job.CreatedAt)
	if err != nil {
		return &proto.EventSchemaMessage{}, "", err
	}
	return schemaMessage, writeKey, nil
}

func (st *SchemaTransformer) getSchemaKeyFromJob(eventPayload map[string]interface{}, writeKey string) *proto.EventSchemaKey {
	eventType := st.getEventType(eventPayload)
	return &proto.EventSchemaKey{
		WriteKey:        writeKey,
		EventType:       eventType,
		EventIdentifier: st.getEventIdentifier(eventPayload, eventType),
	}
}

func (st *SchemaTransformer) backendConfigSubscriber(ctx context.Context) {
	ch := st.backendConfig.Subscribe(ctx, backendconfig.TopicProcessConfig)
	for data := range ch {
		configData := data.Data.(map[string]backendconfig.ConfigT)
		sourceWriteKeyMap := map[string]string{}
		newPIIReportingSettings := map[string]bool{}
		for _, wConfig := range configData {
			for i := range wConfig.Sources {
				source := &wConfig.Sources[i]
				sourceWriteKeyMap[source.ID] = source.WriteKey
				newPIIReportingSettings[source.WriteKey] = wConfig.Settings.DataRetention.DisableReportingPII
			}
		}
		st.writeKeyMapLock.Lock()
		st.sourceWriteKeyMap = sourceWriteKeyMap
		st.newPIIReportingSettings = newPIIReportingSettings
		st.writeKeyMapLock.Unlock()
		st.initialisedOnce.Do(func() {
			close(st.initialised)
		})
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

func (st *SchemaTransformer) getSchemaMessage(key *proto.EventSchemaKey, event map[string]interface{}, marshalledEvent json.RawMessage, workspaceId string, observedAt time.Time) (*proto.EventSchemaMessage, error) {
	flattenedEvent, err := st.flattenEvent(event)
	if err != nil {
		return nil, err
	}
	schema := st.getSchema(flattenedEvent)
	if st.disablePIIReporting(key.WriteKey) {
		marshalledEvent = []byte("{}")
	}
	return &proto.EventSchemaMessage{
		WorkspaceID: workspaceId,
		Key:         key,
		ObservedAt:  timestamppb.New(observedAt),
		Schema:      schema,
		Sample:      marshalledEvent,
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
	return st.newPIIReportingSettings[writeKey]
}

func (st *SchemaTransformer) getWriteKeyFromParams(parameters json.RawMessage) string {
	sourceId := gjson.GetBytes(parameters, "source_id").Str
	if sourceId == "" {
		return sourceId
	}
	st.writeKeyMapLock.RLock()
	defer st.writeKeyMapLock.RUnlock()
	return st.sourceWriteKeyMap[sourceId]
}
