package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-schemas/go/stream"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func NewStream2JobsDBTransformer(
	backendConfig backendconfig.BackendConfig,
	stats stats.Stats,
	logger logger.Logger,
) *Stream2JobsDBTransformer {
	return &Stream2JobsDBTransformer{
		BackendConfig:                backendConfig,
		StreamMsgValidator:           stream.NewMessageValidator(),
		EnableSuppressUserFeature:    false,
		SuppressUserHandler:          nil,
		Stats:                        stats,
		Logger:                       logger,
		configSubscriberLock:         sync.RWMutex{},
		writeKeysSourceMap:           make(map[string]backendconfig.SourceT),
		sourceIDSourceMap:            make(map[string]backendconfig.SourceT),
		backendConfigInitialised:     false,
		backendConfigInitialisedChan: make(chan struct{}),
	}
}

type Stream2JobsDBTransformer struct {
	BackendConfig             backendconfig.BackendConfig
	StreamMsgValidator        func(message *stream.Message) error
	EnableSuppressUserFeature bool
	SuppressUserHandler       types.UserSuppression
	Stats                     stats.Stats
	Logger                    logger.Logger

	// backendconfig state
	configSubscriberLock         sync.RWMutex
	writeKeysSourceMap           map[string]backendconfig.SourceT
	sourceIDSourceMap            map[string]backendconfig.SourceT
	backendConfigInitialised     bool
	backendConfigInitialisedChan chan struct{}
}

func (gw *Stream2JobsDBTransformer) Transform(reqType string, messages []pulsar.ProducerMessage) ([]*jobsdb.JobT, error) {
	stat := gwstats.SourceStat{ReqType: reqType}
	if len(messages) == 0 {
		stat.RequestFailed(response.NotRudderEvent)
		stat.Report(gw.Stats)
		gw.Logger.Errorn("no messages in request")
		return nil, errors.New(response.NotRudderEvent)
	}

	var (
		res              []jobWithStat
		isUserSuppressed = gw.memoizedIsUserSuppressed()
		streamMessages   = make([]stream.Message, 0, len(messages))
	)
	for _, msg := range messages {
		properties, err := stream.FromMapProperties(msg.Properties)
		if err != nil {
			stat.RequestEventsFailed(1, response.InvalidStreamMessage)
			stat.Report(gw.Stats)
			gw.Logger.Errorn("invalid properties in message", logger.NewStringField("key", msg.Key), obskit.Error(err))
			return nil, errors.New(response.InvalidStreamMessage)
		}
		streamMessages = append(streamMessages, stream.Message{
			Payload:    msg.Payload,
			Properties: properties,
		})
	}

	res = make([]jobWithStat, 0, len(streamMessages))

	for _, msg := range streamMessages {
		stat := gwstats.SourceStat{ReqType: reqType}
		err := gw.StreamMsgValidator(&msg)
		if err != nil {
			loggerFields := msg.Properties.LoggerFields()
			loggerFields = append(loggerFields, obskit.Error(err))
			gw.Logger.Errorn("invalid message in request",
				loggerFields...)
			stat.RequestEventsFailed(1, response.InvalidStreamMessage)
			stat.Report(gw.Stats)
			return nil, errors.New(response.InvalidStreamMessage)
		}
		// TODO: get rid of this check
		if msg.Properties.RequestType != "" {
			switch msg.Properties.RequestType {
			case "batch", "replay", "retl", "import":
			default:
				msg.Payload, err = sjson.SetBytes(msg.Payload, "type", msg.Properties.RequestType)
				if err != nil {
					stat.RequestEventsFailed(1, response.NotRudderEvent)
					stat.Report(gw.Stats)
					loggerFields := msg.Properties.LoggerFields()
					loggerFields = append(loggerFields, obskit.Error(err))
					gw.Logger.Errorn("failed to set type in message", loggerFields...)
					return nil, errors.New(response.NotRudderEvent)
				}
			}
		}

		anonIDFromReq := sanitizeAndTrim(gjson.GetBytes(msg.Payload, "anonymousId").String())
		userIDFromReq := sanitizeAndTrim(gjson.GetBytes(msg.Payload, "userId").String())
		messageID, changed := getMessageID(msg.Payload)
		if changed {
			msg.Payload, err = sjson.SetBytes(msg.Payload, "messageId", messageID)
			if err != nil {
				stat.RequestFailed(response.NotRudderEvent)
				stat.Report(gw.Stats)
				gw.Logger.Errorn("failed to set messageID in message",
					obskit.Error(err))
				return nil, errors.New(response.NotRudderEvent)
			}
		}
		rudderId, err := getRudderId(userIDFromReq, anonIDFromReq)
		if err != nil {
			stat.RequestFailed(response.NotRudderEvent)
			stat.Report(gw.Stats)
			gw.Logger.Errorn("failed to get rudderId",
				obskit.Error(err))
			return nil, errors.New(response.NotRudderEvent)
		}
		msg.Payload, err = sjson.SetBytes(msg.Payload, "rudderId", rudderId.String())
		if err != nil {
			stat.RequestFailed(response.NotRudderEvent)
			stat.Report(gw.Stats)
			loggerFields := msg.Properties.LoggerFields()
			loggerFields = append(loggerFields, obskit.Error(err))
			gw.Logger.Errorn("failed to set rudderId in message",
				loggerFields...)
			return nil, errors.New(response.NotRudderEvent)
		}
		writeKey, ok := gw.getWriteKeyFromSourceID(msg.Properties.SourceID)
		if !ok {
			// only live-events will not work if writeKey is not found
			gw.Logger.Errorn("unable to get writeKey for job",
				logger.NewStringField("messageId", messageID),
				obskit.SourceID(msg.Properties.SourceID))
		}
		stat.SourceID = msg.Properties.SourceID
		stat.WorkspaceID = msg.Properties.WorkspaceID
		stat.WriteKey = writeKey
		if isUserSuppressed(msg.Properties.WorkspaceID, msg.Properties.UserID, msg.Properties.SourceID) {
			sourceConfig := gw.getSourceConfigFromSourceID(msg.Properties.SourceID)
			gw.Logger.Infon("suppressed event",
				obskit.SourceID(msg.Properties.SourceID),
				obskit.WorkspaceID(msg.Properties.WorkspaceID),
				logger.NewStringField("userIDFromReq", msg.Properties.UserID),
			)
			gw.Stats.NewTaggedStat(
				"gateway.write_key_suppressed_events",
				stats.CountType,
				gw.newSourceStatTagsWithReason(&sourceConfig, reqType, errEventSuppressed.Error()),
			).Increment()
			continue
		}

		gw.Stats.NewTaggedStat("gateway.event_pickup_lag_seconds", stats.TimerType, stats.Tags{
			"workspaceId": msg.Properties.WorkspaceID,
		}).Since(msg.Properties.ReceivedAt)

		jobsDBParams := params{
			MessageID:       messageID,
			SourceID:        msg.Properties.SourceID,
			SourceJobRunID:  msg.Properties.SourceJobRunID,
			SourceTaskRunID: msg.Properties.SourceTaskRunID,
			UserID:          msg.Properties.UserID,
			TraceParent:     msg.Properties.TraceID,
			DestinationID:   msg.Properties.DestinationID,
			SourceCategory:  msg.Properties.SourceType,
		}

		marshalledParams, err := json.Marshal(jobsDBParams)
		if err != nil {
			gw.Logger.Errorn("[Gateway] Failed to marshal parameters map",
				logger.NewField("params", jobsDBParams),
				obskit.Error(err),
			)
			marshalledParams = []byte(
				`{"error": "rudder-server gateway failed to marshal params"}`,
			)
		}

		msg.Payload, err = fillReceivedAt(msg.Payload, msg.Properties.ReceivedAt)
		if err != nil {
			err = fmt.Errorf("filling receivedAt: %w", err)
			stat.RequestEventsFailed(1, err.Error())
			stat.Report(gw.Stats)
			gw.Logger.Errorn("failed to fill receivedAt in message",
				obskit.Error(err))
			return nil, fmt.Errorf("filling receivedAt: %w", err)
		}
		msg.Payload, err = fillRequestIP(msg.Payload, msg.Properties.RequestIP)
		if err != nil {
			err = fmt.Errorf("filling request_ip: %w", err)
			stat.RequestEventsFailed(1, err.Error())
			stat.Report(gw.Stats)
			gw.Logger.Errorn("failed to fill request_ip in message",
				obskit.Error(err))
			return nil, fmt.Errorf("filling request_ip: %w", err)
		}

		eventBatch := singularEventBatch{
			Batch:      []json.RawMessage{msg.Payload},
			ReceivedAt: msg.Properties.ReceivedAt.Format(misc.RFC3339Milli),
			RequestIP:  msg.Properties.RequestIP,
			WriteKey:   writeKey,
		}

		payload, err := json.Marshal(eventBatch)
		if err != nil {
			err = fmt.Errorf("marshalling event batch: %w", err)
			stat.RequestEventsFailed(1, err.Error())
			stat.Report(gw.Stats)
			loggerFields := msg.Properties.LoggerFields()
			loggerFields = append(loggerFields, obskit.Error(err))
			gw.Logger.Errorn("failed to marshal event batch",
				loggerFields...)
			return nil, fmt.Errorf("marshalling event batch: %w", err)
		}
		jobUUID := uuid.New()
		res = append(res, jobWithStat{
			stat: stat,
			job: &jobsdb.JobT{
				UUID:         jobUUID,
				UserID:       msg.Properties.RoutingKey,
				Parameters:   marshalledParams,
				CustomVal:    customVal,
				EventPayload: payload,
				EventCount:   len(eventBatch.Batch),
				WorkspaceId:  msg.Properties.WorkspaceID,
			},
		})
	}
	if len(res) == 0 { // events suppressed - but return success
		return nil, nil
	}

	jobs := lo.Map(res, func(jws jobWithStat, _ int) *jobsdb.JobT {
		return jws.job
	})

	return jobs, nil
}

// memoizedIsUserSuppressed is a memoized version of isUserSuppressed
func (gw *Stream2JobsDBTransformer) memoizedIsUserSuppressed() func(workspaceID, userID, sourceID string) bool {
	cache := map[string]bool{}
	return func(workspaceID, userID, sourceID string) bool {
		key := workspaceID + ":" + userID + ":" + sourceID
		if val, ok := cache[key]; ok {
			return val
		}
		val := gw.isUserSuppressed(workspaceID, userID, sourceID)
		cache[key] = val
		return val
	}
}

// isUserSuppressed checks if the user is suppressed or not
func (gw *Stream2JobsDBTransformer) isUserSuppressed(workspaceID, userID, sourceID string) bool {
	if !gw.EnableSuppressUserFeature || gw.SuppressUserHandler == nil {
		return false
	}
	if metadata := gw.SuppressUserHandler.GetSuppressedUser(workspaceID, userID, sourceID); metadata != nil {
		if !metadata.CreatedAt.IsZero() {
			gw.Stats.NewTaggedStat("gateway.user_suppression_age", stats.TimerType, stats.Tags{
				"workspaceId": workspaceID,
				"sourceID":    sourceID,
			}).Since(metadata.CreatedAt)
		}
		return true
	}
	return false
}

func (gw *Stream2JobsDBTransformer) newSourceStatTagsWithReason(s *backendconfig.SourceT, reqType, reason string) stats.Tags {
	tags := stats.Tags{
		"source":       misc.GetTagName(s.WriteKey, s.Name),
		"source_id":    s.ID,
		"write_key":    s.WriteKey,
		"req_type":     reqType,
		"workspace_id": s.WorkspaceID,
		"source_type":  s.SourceDefinition.Category,
	}
	if reason != "" {
		tags["reason"] = reason
	}
	return tags
}

func (gw *Stream2JobsDBTransformer) getSourceConfigFromSourceID(sourceID string) backendconfig.SourceT {
	gw.configSubscriberLock.RLock()
	defer gw.configSubscriberLock.RUnlock()
	if s, ok := gw.sourceIDSourceMap[sourceID]; ok {
		return s
	}
	return backendconfig.SourceT{}
}

func (gw *Stream2JobsDBTransformer) getWriteKeyFromSourceID(sourceID string) (string, bool) {
	gw.configSubscriberLock.RLock()
	defer gw.configSubscriberLock.RUnlock()
	if s, ok := gw.sourceIDSourceMap[sourceID]; ok {
		return s.WriteKey, true
	}
	return "", false
}

// backendConfigSubscriber gets the config from config backend and extracts source information from it.
func (gw *Stream2JobsDBTransformer) backendConfigSubscriber(ctx context.Context) {
	closeConfigChan := func(sources int) {
		if !gw.backendConfigInitialised {
			gw.Logger.Infow("BackendConfig initialised", "sources", sources)
			gw.backendConfigInitialised = true
			close(gw.backendConfigInitialisedChan)
		}
	}
	defer closeConfigChan(0)
	ch := gw.BackendConfig.Subscribe(ctx, backendconfig.TopicProcessConfig)
	for data := range ch {
		var (
			writeKeysSourceMap = map[string]backendconfig.SourceT{}
			sourceIDSourceMap  = map[string]backendconfig.SourceT{}
		)
		configData := data.Data.(map[string]backendconfig.ConfigT)
		for _, wsConfig := range configData {
			for _, source := range wsConfig.Sources {
				writeKeysSourceMap[source.WriteKey] = source
				sourceIDSourceMap[source.ID] = source
				if source.Enabled && source.SourceDefinition.Category == "webhook" {
					// gw.webhook.Register(source.SourceDefinition.Name) // TODO
				}
			}
		}
		gw.configSubscriberLock.Lock()
		gw.writeKeysSourceMap = writeKeysSourceMap
		gw.sourceIDSourceMap = sourceIDSourceMap
		gw.configSubscriberLock.Unlock()
		closeConfigChan(len(gw.writeKeysSourceMap))
	}
}

type params struct {
	MessageID       string `json:"message_id"`
	SourceID        string `json:"source_id"`
	SourceJobRunID  string `json:"source_job_run_id"`
	SourceTaskRunID string `json:"source_task_run_id"`
	UserID          string `json:"user_id"`
	TraceParent     string `json:"traceparent"`
	DestinationID   string `json:"destination_id,omitempty"`
	SourceCategory  string `json:"source_category"`
}

type singularEventBatch struct {
	Batch      []json.RawMessage `json:"batch"`
	ReceivedAt string            `json:"receivedAt"`
	RequestIP  string            `json:"requestIP"`
	WriteKey   string            `json:"writeKey"` // only needed for live-events
}
