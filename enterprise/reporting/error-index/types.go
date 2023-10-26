package error_index

import (
	"context"
	"os"
	"time"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
)

type configFetcher interface {
	WorkspaceIDFromSource(sourceID string) string
}

type uploader interface {
	Upload(context.Context, *os.File, ...string) (filemanager.UploadedFile, error)
}

type payload struct {
	MessageID        string `json:"messageId" parquet:"name=message_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	SourceID         string `json:"sourceId" parquet:"name=source_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	DestinationID    string `json:"destinationId" parquet:"name=destination_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	TransformationID string `json:"transformationId" parquet:"name=transformation_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	TrackingPlanID   string `json:"trackingPlanId" parquet:"name=tracking_plan_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	FailedStage      string `json:"failedStage" parquet:"name=failed_stage, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	EventType        string `json:"eventType" parquet:"name=event_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	EventName        string `json:"eventName" parquet:"name=event_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	ReceivedAt       int64  `json:"receivedAt" parquet:"name=received_at, type=INT64, convertedtype=TIMESTAMP_MICROS, encoding=DELTA_BINARY_PACKED"` // In Microseconds
	FailedAt         int64  `json:"failedAt" parquet:"name=failed_at, type=INT64, convertedtype=TIMESTAMP_MICROS, encoding=DELTA_BINARY_PACKED"`     // In Microseconds
}

func (p *payload) SetReceivedAt(t time.Time) {
	p.ReceivedAt = t.UnixMicro()
}

func (p *payload) SetFailedAt(t time.Time) {
	p.FailedAt = t.UnixMicro()
}

func (p *payload) FailedTime() time.Time {
	return time.UnixMicro(p.FailedAt).UTC()
}

func (p *payload) AggregateKey() string {
	return p.FailedTime().Format("2006-01-02/15")
}
