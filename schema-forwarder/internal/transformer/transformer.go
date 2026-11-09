package transformer

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/jeremywohl/flatten"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	proto "github.com/rudderlabs/rudder-server/proto/event-schema"
)

type Transformer interface {
	Start()
	Transform(job *jobsdb.JobT) (msg *proto.EventSchemaMessage, sourceID string, err error)
	Stop()
}

// New returns a new instance of Schema Transformer
func New(backendConfig backendconfig.BackendConfig, conf *config.Config) Transformer {
	return &transformer{
		backendConfig:             backendConfig,
		conf:                      conf,
		captureNilAsUnknowns:      conf.GetBoolVar(false, "EventSchemas.captureUnknowns"),
		keysLimit:                 conf.GetIntVar(500, 1, "EventSchemas.keysLimit"),
		identifierLimit:           conf.GetIntVar(100, 1, "EventSchemas.identifierLimit"),
		collapseArrayIndicesCache: make(map[string]*config.Reloadable[bool]),
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
func (st *transformer) Transform(job *jobsdb.JobT) (*proto.EventSchemaMessage, string, error) {
	var eventPayload map[string]any
	if err := jsonrs.Unmarshal(job.EventPayload, &eventPayload); err != nil {
		return nil, "", err
	}
	sourceID := gjson.GetBytes(job.Parameters, "source_id").Str
	writeKey := st.writeKeyForSourceID(sourceID)
	if writeKey == "" {
		return nil, "", fmt.Errorf("writeKey could not be found")
	}
	schemaKey := st.getSchemaKeyFromJob(eventPayload, writeKey)
	if st.identifierLimit > 0 && len(schemaKey.EventIdentifier) > st.identifierLimit {
		return nil, "", fmt.Errorf("event identifier size is greater than %d", st.identifierLimit)
	}
	schemaMessage, err := st.getSchemaMessage(schemaKey, sourceID, eventPayload, []byte("{}"), job.WorkspaceId, job.CreatedAt)
	if err != nil {
		return nil, "", err
	}

	return schemaMessage, sourceID, nil
}

// getSchemaKeyFromJob returns the schema key from the job based on the event type and event identifier
func (st *transformer) getSchemaKeyFromJob(eventPayload map[string]any, writeKey string) *proto.EventSchemaKey {
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
func (st *transformer) getEventType(event map[string]any) string {
	eventType, ok := event["type"].(string)
	if !ok {
		return ""
	}
	return eventType
}

// getEventIdentifier returns the event identifier from the event
func (st *transformer) getEventIdentifier(event map[string]any, eventType string) string {
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
func (st *transformer) getSchemaMessage(key *proto.EventSchemaKey, sourceID string, event map[string]any, sample json.RawMessage, workspaceId string, observedAt time.Time) (*proto.EventSchemaMessage, error) {
	flattenedEvent, err := st.flattenEvent(sourceID, event)
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
func (st *transformer) getSchema(flattenedEvent map[string]any) map[string]string {
	schema := make(map[string]string)
	for k, v := range flattenedEvent {
		reflectType := reflect.TypeOf(v)
		if reflectType != nil {
			schema[k] = reflectType.String()
		} else if v != nil || st.captureNilAsUnknowns {
			schema[k] = "unknown"
		}
	}
	return schema
}

// flattenEvent flattens the event
func (st *transformer) flattenEvent(sourceID string, event map[string]any) (map[string]any, error) {
	if st.collapseArrayIndices(sourceID).Load() {
		return flattenCollapsingArrayIndices(event), nil
	}
	flattenedEvent, err := flatten.Flatten(event, "", flatten.DotStyle)
	if err != nil {
		return nil, err
	}
	return flattenedEvent, nil
}

// collapseArrayIndices returns the reloadable setting controlling whether array indices in schema keys are
// written as 0 for the given source. The setting is resolved hierarchically, the first key that is set wins, and
// the resulting reloadable is cached per source id:
//
//	SchemaForwarder.<source-id>.collapseArrayIndices
//	SchemaForwarder.collapseArrayIndices
func (st *transformer) collapseArrayIndices(sourceID string) *config.Reloadable[bool] {
	if setting, ok := st.collapseArrayIndicesCache[sourceID]; ok {
		return setting
	}
	setting := st.conf.GetReloadableBoolVar(false,
		"SchemaForwarder."+sourceID+".collapseArrayIndices",
		"SchemaForwarder.collapseArrayIndices",
	)
	st.collapseArrayIndicesCache[sourceID] = setting
	return setting
}

// flattenCollapsingArrayIndices flattens the event like flatten.Flatten with flatten.DotStyle, except that every
// array index in a key is written as 0. When several values map to the same key, the first non-nil one is kept:
// array elements in array order, and a dotted map key colliding with a collapsed array key in map order.
//
// Indices are collapsed while flattening because in a flattened key an array index and a numeric map key look
// the same, and only array indices may be collapsed.
func flattenCollapsingArrayIndices(event map[string]any) map[string]any {
	flattened := make(map[string]any)
	var add func(key string, v any)
	add = func(key string, v any) {
		switch v := v.(type) {
		case map[string]any:
			for k, e := range v {
				add(key+"."+k, e)
			}
		case []any:
			key += ".0"
			for _, e := range v {
				add(key, e)
			}
		default:
			if cur, ok := flattened[key]; !ok || cur == nil {
				flattened[key] = v
			}
		}
	}
	for k, v := range event {
		add(k, v)
	}
	return flattened
}

// disablePIIReporting returns whether PII reporting is disabled for the write key
func (st *transformer) disablePIIReporting(writeKey string) bool {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.newPIIReportingSettings[writeKey]
}

// writeKeyForSourceID returns the write key configured for the given source id
func (st *transformer) writeKeyForSourceID(sourceID string) string {
	if sourceID == "" {
		return ""
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.sourceWriteKeyMap[sourceID]
}
