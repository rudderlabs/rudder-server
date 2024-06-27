package handler

import (
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/segmentio/go-hll"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/report"
)

type TrackedUsersReport struct {
	ReportedAt                  time.Time `json:"reportedAt"`
	WorkspaceID                 string    `json:"workspaceId"`
	SourceID                    string    `json:"sourceId"`
	InstanceID                  string    `json:"instanceId"`
	UserIDHLL                   hll.Hll   `json:"-"`
	AnonymousIDHLL              hll.Hll   `json:"-"`
	IdentifiedAnonymousIDHLL    hll.Hll   `json:"-"`
	UserIDHLLHex                string    `json:"userIdHLL"`
	AnonymousIDHLLHex           string    `json:"anonymousIdHLL"`
	IdentifiedAnonymousIDHLLHex string    `json:"identifiedAnonymousIdHLL"`
}

func (t *TrackedUsersReport) MarshalJSON() ([]byte, error) {
	t.UserIDHLLHex = hex.EncodeToString(t.UserIDHLL.ToBytes())
	t.AnonymousIDHLLHex = hex.EncodeToString(t.AnonymousIDHLL.ToBytes())
	t.IdentifiedAnonymousIDHLLHex = hex.EncodeToString(t.IdentifiedAnonymousIDHLL.ToBytes())

	type Alias TrackedUsersReport
	return json.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(t),
	})
}

type TrackedUsersHandler struct {
	table  string
	labels []string
}

func NewTrackedUsersHandler(table string, labels []string) *TrackedUsersHandler {
	return &TrackedUsersHandler{
		table:  table,
		labels: labels,
	}
}

func (t *TrackedUsersHandler) Aggregate(aggReport report.DecodedReport, report report.DecodedReport) error {
	tuReport := report.(*TrackedUsersReport)
	tuAggReport := aggReport.(*TrackedUsersReport)

	tuAggReport.UserIDHLL.Union(tuReport.UserIDHLL)
	tuAggReport.AnonymousIDHLL.Union(tuReport.AnonymousIDHLL)
	tuAggReport.IdentifiedAnonymousIDHLL.Union(tuReport.IdentifiedAnonymousIDHLL)

	return nil
}

func (t *TrackedUsersHandler) decodeHLL(encoded string) (*hll.Hll, error) {
	data, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	hll, err := hll.FromBytes(data)
	if err != nil {
		return nil, err
	}
	return &hll, nil
}

func (t *TrackedUsersHandler) Decode(r report.RawReport) (report.DecodedReport, error) {
	tuReport := &TrackedUsersReport{
		ReportedAt:  r["reported_at"].(time.Time),
		WorkspaceID: r["workspace_id"].(string),
		SourceID:    r["source_id"].(string),
		InstanceID:  r["instance_id"].(string),
	}

	userIDHLL, err := t.decodeHLL(r["userid_hll"].(string))
	if err != nil {
		return nil, err
	}
	tuReport.UserIDHLL = *userIDHLL

	anonymousIDHLL, err := t.decodeHLL(r["anonymousid_hll"].(string))
	if err != nil {
		return nil, err
	}
	tuReport.AnonymousIDHLL = *anonymousIDHLL

	identifiedAnonymousIDHLL, err := t.decodeHLL(r["identified_anonymousid_hll"].(string))
	if err != nil {
		return nil, err
	}
	tuReport.IdentifiedAnonymousIDHLL = *identifiedAnonymousIDHLL

	return tuReport, nil
}
