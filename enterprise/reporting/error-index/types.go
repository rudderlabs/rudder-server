package error_index

import (
	"context"
	"os"
	"time"

	ptypes "github.com/xitongsys/parquet-go/types"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
)

type configFetcher interface {
	WorkspaceIDFromSource(sourceID string) string
	IsPIIReportingDisabled(workspaceID string) bool
}

type uploader interface {
	Upload(context.Context, *os.File, ...string) (filemanager.UploadedFile, error)
}

type payload struct {
	MessageID        string    `json:"messageId"`
	SourceID         string    `json:"sourceId"`
	DestinationID    string    `json:"destinationId"`
	TransformationID string    `json:"transformationId"`
	TrackingPlanID   string    `json:"trackingPlanId"`
	FailedStage      string    `json:"failedStage"`
	EventType        string    `json:"eventType"`
	EventName        string    `json:"eventName"`
	ReceivedAt       time.Time `json:"receivedAt"`
	FailedAt         time.Time `json:"failedAt"`
}

type payloadParquet struct {
	MessageID        string `parquet:"name=message_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	SourceID         string `parquet:"name=source_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	DestinationID    string `parquet:"name=destination_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	TransformationID string `parquet:"name=transformation_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	TrackingPlanID   string `parquet:"name=tracking_plan_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	FailedStage      string `parquet:"name=failed_stage, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	EventType        string `parquet:"name=event_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	EventName        string `parquet:"name=event_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	ReceivedAt       int64  `parquet:"name=received_at, type=INT64, convertedtype=TIMESTAMP_MICROS, encoding=DELTA_BINARY_PACKED"`
	FailedAt         int64  `parquet:"name=failed_at, type=INT64, convertedtype=TIMESTAMP_MICROS, encoding=DELTA_BINARY_PACKED"`
}

func (p payload) toParquet() payloadParquet {
	return payloadParquet{
		MessageID:        p.MessageID,
		SourceID:         p.SourceID,
		DestinationID:    p.DestinationID,
		TransformationID: p.TransformationID,
		TrackingPlanID:   p.TrackingPlanID,
		FailedStage:      p.FailedStage,
		EventType:        p.EventType,
		EventName:        p.EventName,
		ReceivedAt:       ptypes.TimeToTIMESTAMP_MICROS(p.ReceivedAt, true),
		FailedAt:         ptypes.TimeToTIMESTAMP_MICROS(p.FailedAt, true),
	}
}
