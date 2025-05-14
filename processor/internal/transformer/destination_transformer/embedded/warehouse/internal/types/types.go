package types

import (
	"time"

	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/utils"
	proctypes "github.com/rudderlabs/rudder-server/processor/types"
)

type Metadata struct {
	MessageID         any            `json:"messageId"`
	ReceivedAt        string         `json:"receivedAt"`
	SourceID          string         `json:"sourceId"`
	SourceType        string         `json:"sourceType"`
	DestinationID     string         `json:"destinationId"`
	DestinationType   string         `json:"destinationType"`
	SourceCategory    string         `json:"sourceCategory"`
	EventType         string         `json:"eventType,omitempty"`
	RecordID          interface{}    `json:"recordId,omitempty"`
	DestinationConfig map[string]any `json:"destinationConfig"`
}

type TransformerEvent struct {
	Message  proctypes.SingularEventT `json:"message"`
	Metadata Metadata                 `json:"metadata"`
}

var destConfigFields = []string{
	"skipTracksTable", "skipUsersTable", "underscoreDivideNumbers", "allowUsersContextTraits",
	"storeFullEvent", "jsonPaths",
}

func New(
	event *proctypes.TransformerEvent,
	uuidGenerator func() string,
	now func() time.Time,
) *TransformerEvent {
	wEvent := &TransformerEvent{}
	wEvent.Message = event.Message
	wEvent.Metadata.MessageID = utils.ExtractMessageID(event, uuidGenerator)
	wEvent.Metadata.ReceivedAt = utils.ExtractReceivedAt(event, now)
	wEvent.Metadata.SourceID = event.Metadata.SourceID
	wEvent.Metadata.SourceType = event.Metadata.SourceType
	wEvent.Metadata.DestinationID = event.Metadata.DestinationID
	wEvent.Metadata.DestinationType = event.Metadata.DestinationType
	wEvent.Metadata.SourceCategory = event.Metadata.SourceCategory
	wEvent.Metadata.EventType = utils.ToString(event.Message["type"])
	wEvent.Metadata.RecordID = event.Metadata.RecordID
	wEvent.Metadata.DestinationConfig = map[string]any{}
	for _, key := range destConfigFields {
		configValue, ok := event.Destination.Config[key]
		if !ok {
			continue
		}
		wEvent.Metadata.DestinationConfig[key] = configValue
	}
	return wEvent
}
