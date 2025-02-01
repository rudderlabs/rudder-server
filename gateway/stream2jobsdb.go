package gateway

import (
	"encoding/json"
	"errors"
	"fmt"

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

// TODO remove duplicated code from GW?

type backendConfigReader interface {
	getWriteKeyFromSourceID(sourceID string) (string, bool)
	getSourceConfigFromSourceID(sourceID string) backendconfig.SourceT
}

type Stream2JobsDBTransformer struct {
	streamMsgValidator        func(message *stream.Message) error
	backendConfigReader       backendConfigReader
	enableSuppressUserFeature bool
	suppressUserHandler       types.UserSuppression
	requestSizeStat           stats.Histogram
	stats                     stats.Stats
	logger                    logger.Logger
}

func (gw *Stream2JobsDBTransformer) Transform(reqType string, body []byte) ([]*jobsdb.JobT, error) {
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

	var (
		messages         []stream.Message
		isUserSuppressed = gw.memoizedIsUserSuppressed()
		res              []jobWithStat
		stat             = gwstats.SourceStat{ReqType: reqType}
	)

	err := jsonfast.Unmarshal(body, &messages)
	if err != nil {
		stat.RequestFailed(response.InvalidJSON)
		stat.Report(gw.stats)
		gw.logger.Errorn("invalid json in request",
			obskit.Error(err))
		return nil, errors.New(response.InvalidJSON)
	}
	gw.requestSizeStat.Observe(float64(len(body)))

	if len(messages) == 0 {
		stat.RequestFailed(response.NotRudderEvent)
		stat.Report(gw.stats)
		gw.logger.Errorn("no messages in request")
		return nil, errors.New(response.NotRudderEvent)
	}

	res = make([]jobWithStat, 0, len(messages))

	for _, msg := range messages {
		stat := gwstats.SourceStat{ReqType: reqType}
		err := gw.streamMsgValidator(&msg)
		if err != nil {
			loggerFields := msg.Properties.LoggerFields()
			loggerFields = append(loggerFields, obskit.Error(err))
			gw.logger.Errorn("invalid message in request",
				loggerFields...)
			stat.RequestEventsFailed(1, response.InvalidStreamMessage)
			stat.Report(gw.stats)
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
					stat.Report(gw.stats)
					loggerFields := msg.Properties.LoggerFields()
					loggerFields = append(loggerFields, obskit.Error(err))
					gw.logger.Errorn("failed to set type in message", loggerFields...)
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
				stat.Report(gw.stats)
				gw.logger.Errorn("failed to set messageID in message",
					obskit.Error(err))
				return nil, errors.New(response.NotRudderEvent)
			}
		}
		rudderId, err := getRudderId(userIDFromReq, anonIDFromReq)
		if err != nil {
			stat.RequestFailed(response.NotRudderEvent)
			stat.Report(gw.stats)
			gw.logger.Errorn("failed to get rudderId",
				obskit.Error(err))
			return nil, errors.New(response.NotRudderEvent)
		}
		msg.Payload, err = sjson.SetBytes(msg.Payload, "rudderId", rudderId.String())
		if err != nil {
			stat.RequestFailed(response.NotRudderEvent)
			stat.Report(gw.stats)
			loggerFields := msg.Properties.LoggerFields()
			loggerFields = append(loggerFields, obskit.Error(err))
			gw.logger.Errorn("failed to set rudderId in message",
				loggerFields...)
			return nil, errors.New(response.NotRudderEvent)
		}
		writeKey, ok := gw.backendConfigReader.getWriteKeyFromSourceID(msg.Properties.SourceID)
		if !ok {
			// only live-events will not work if writeKey is not found
			gw.logger.Errorn("unable to get writeKey for job",
				logger.NewStringField("messageId", messageID),
				obskit.SourceID(msg.Properties.SourceID))
		}
		stat.SourceID = msg.Properties.SourceID
		stat.WorkspaceID = msg.Properties.WorkspaceID
		stat.WriteKey = writeKey
		if isUserSuppressed(msg.Properties.WorkspaceID, msg.Properties.UserID, msg.Properties.SourceID) {
			sourceConfig := gw.backendConfigReader.getSourceConfigFromSourceID(msg.Properties.SourceID)
			gw.logger.Infon("suppressed event",
				obskit.SourceID(msg.Properties.SourceID),
				obskit.WorkspaceID(msg.Properties.WorkspaceID),
				logger.NewStringField("userIDFromReq", msg.Properties.UserID),
			)
			gw.stats.NewTaggedStat(
				"gateway.write_key_suppressed_events",
				stats.CountType,
				gw.newSourceStatTagsWithReason(&sourceConfig, reqType, errEventSuppressed.Error()),
			).Increment()
			continue
		}

		gw.stats.NewTaggedStat("gateway.event_pickup_lag_seconds", stats.TimerType, stats.Tags{
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
			gw.logger.Errorn("[Gateway] Failed to marshal parameters map",
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
			stat.Report(gw.stats)
			gw.logger.Errorn("failed to fill receivedAt in message",
				obskit.Error(err))
			return nil, fmt.Errorf("filling receivedAt: %w", err)
		}
		msg.Payload, err = fillRequestIP(msg.Payload, msg.Properties.RequestIP)
		if err != nil {
			err = fmt.Errorf("filling request_ip: %w", err)
			stat.RequestEventsFailed(1, err.Error())
			stat.Report(gw.stats)
			gw.logger.Errorn("failed to fill request_ip in message",
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
			stat.Report(gw.stats)
			loggerFields := msg.Properties.LoggerFields()
			loggerFields = append(loggerFields, obskit.Error(err))
			gw.logger.Errorn("failed to marshal event batch",
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
	if !gw.enableSuppressUserFeature || gw.suppressUserHandler == nil {
		return false
	}
	if metadata := gw.suppressUserHandler.GetSuppressedUser(workspaceID, userID, sourceID); metadata != nil {
		if !metadata.CreatedAt.IsZero() {
			gw.stats.NewTaggedStat("gateway.user_suppression_age", stats.TimerType, stats.Tags{
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
