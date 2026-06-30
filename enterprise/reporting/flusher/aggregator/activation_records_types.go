package aggregator

import (
	"encoding/hex"
	"time"

	"github.com/segmentio/go-hll"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
)

type ActivationRecordsReport struct {
	ReportedAt        time.Time `json:"reportedAt"`
	WorkspaceID       string    `json:"workspaceId"`
	SourceID          string    `json:"sourceId"`
	DestinationID     string    `json:"destinationId"`
	InstanceID        string    `json:"instanceId"`
	FingerprintHLL    *hll.Hll  `json:"-"`
	FingerprintHLLHex string    `json:"fingerprintHLL"`
}

func (t *ActivationRecordsReport) MarshalJSON() ([]byte, error) {
	t.FingerprintHLLHex = hex.EncodeToString(t.FingerprintHLL.ToBytes())

	type Alias ActivationRecordsReport
	return jsonrs.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(t),
	})
}
