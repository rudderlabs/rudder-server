package transformer

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jeremywohl/flatten"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	proto "github.com/rudderlabs/rudder-server/proto/event-schema"
)

type Transformer interface {
	Start()
	Transform(job *jobsdb.JobT) (*proto.EventSchemaMessage, error)
	Stop()
}

// New returns a new instance of Schema Transformer
func New(backendConfig backendconfig.BackendConfig, config *config.Config) Transformer {
	return &transformer{
		backendConfig:        backendConfig,
		captureNilAsUnknowns: config.GetBool("EventSchemas.captureUnknowns", false),
		keysLimit:            config.GetInt("EventSchemas.keysLimit", 500),
		identifierLimit:      config.GetInt("EventSchemas.identifierLimit", 100),
	}
}

// Start starts the schema transformer
func (st *transformer) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	st.cancel = cancel
	st.g, ctx = errgroup.WithContext(ctx)

	var initialisedOnce sync.Once
	initialised := make(chan struct{})
	loopFn := func() {
		initialisedOnce.Do(func() {
			close(initialised)
		})
	}

	st.g.Go(func() error {
		st.backendConfigSubscriber(ctx, loopFn)
		return nil
	})

	<-initialised
}

// Stop stops the schema transformer
func (st *transformer) Stop() {
	st.cancel()
	_ = st.g.Wait()
}

// Transform transforms the job into a schema message and returns the schema message along with write key
func (st *transformer) Transform(job *jobsdb.JobT) (*proto.EventSchemaMessage, error) {
	var eventPayload map[string]interface{}
	if err := json.Unmarshal(job.EventPayload, &eventPayload); err != nil {
		return nil, err
	}
	writeKey := st.getWriteKeyFromParams(job.Parameters)
	if writeKey == "" {
		return nil, fmt.Errorf("writeKey could not be found")
	}
	schemaKey := st.getSchemaKeyFromJob(eventPayload, writeKey)
	if st.identifierLimit > 0 && len(schemaKey.EventIdentifier) > st.identifierLimit {
		return nil, fmt.Errorf("event identifier size is greater than %d", st.identifierLimit)
	}
	schemaMessage, err := st.getSchemaMessage(schemaKey, eventPayload, []byte("{}"), job.WorkspaceId, job.CreatedAt)
	if err != nil {
		return nil, err
	}

	return schemaMessage, nil
}

// getSchemaKeyFromJob returns the schema key from the job based on the event type and event identifier
func (st *transformer) getSchemaKeyFromJob(eventPayload map[string]interface{}, writeKey string) *proto.EventSchemaKey {
	eventType := st.getEventType(eventPayload)
	return &proto.EventSchemaKey{
		WriteKey:        writeKey,
		EventType:       eventType,
		EventIdentifier: st.getEventIdentifier(eventPayload, eventType),
	}
}

func (st *transformer) backendConfigSubscriber(ctx context.Context, loopFn func()) {
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
		st.mu.Lock()
		st.sourceWriteKeyMap = sourceWriteKeyMap
		st.newPIIReportingSettings = newPIIReportingSettings
		st.mu.Unlock()
		loopFn()
	}
}

// getEventType returns the event type from the event
func (st *transformer) getEventType(event map[string]interface{}) string {
	eventType, ok := event["type"].(string)
	if !ok {
		return ""
	}
	return eventType
}

// getEventIdentifier returns the event identifier from the event
func (st *transformer) getEventIdentifier(event map[string]interface{}, eventType string) string {
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

// getSchemaMessage returns the schema message from the event by flattening the event and getting the schema
func (st *transformer) getSchemaMessage(key *proto.EventSchemaKey, event map[string]interface{}, sample json.RawMessage, workspaceId string, observedAt time.Time) (*proto.EventSchemaMessage, error) {
	flattenedEvent, err := st.flattenEvent(event)
	if err != nil {
		return nil, err
	}
	if st.keysLimit > 0 && len(flattenedEvent) > st.keysLimit {
		return nil, fmt.Errorf("event schema has more than %d keys", st.keysLimit)
	}
	schema := st.getSchema(flattenedEvent)
	if st.disablePIIReporting(key.WriteKey) {
		sample = []byte("{}") // redact event
	}
	return &proto.EventSchemaMessage{
		WorkspaceID: workspaceId,
		Key:         key,
		Schema:      schema,
		Hash:        proto.SchemaHash(schema),
		ObservedAt:  timestamppb.New(observedAt),
		Sample:      sample,
	}, nil
}

// getSchema returns the schema from the flattened event
func (st *transformer) getSchema(flattenedEvent map[string]interface{}) map[string]string {
	schema := make(map[string]string)
	for k, v := range flattenedEvent {
		reflectType := reflect.TypeOf(v)
		if reflectType != nil {
			schema[k] = reflectType.String()
		} else if !(v == nil && !st.captureNilAsUnknowns) {
			schema[k] = "unknown"
		}
	}
	return schema
}

// flattenEvent flattens the event
func (st *transformer) flattenEvent(event map[string]interface{}) (map[string]interface{}, error) {
	flattenedEvent, err := flatten.Flatten(event, "", flatten.DotStyle)
	if err != nil {
		return nil, err
	}
	return flattenedEvent, nil
}

// disablePIIReporting returns whether PII reporting is disabled for the write key
func (st *transformer) disablePIIReporting(writeKey string) bool {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.newPIIReportingSettings[writeKey]
}

// getWriteKeyFromParams returns the write key from the job parameters
func (st *transformer) getWriteKeyFromParams(parameters json.RawMessage) string {
	sourceId := gjson.GetBytes(parameters, "source_id").Str
	if sourceId == "" {
		return sourceId
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.sourceWriteKeyMap[sourceId]
}
