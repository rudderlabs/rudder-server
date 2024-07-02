package aggregator

import (
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/segmentio/go-hll"
)

type TrackedUsersReport struct {
	ReportedAt                  time.Time `json:"reportedAt"`
	WorkspaceID                 string    `json:"workspaceId"`
	SourceID                    string    `json:"sourceId"`
	InstanceID                  string    `json:"instanceId"`
	UserIDHLL                   *hll.Hll  `json:"-"`
	AnonymousIDHLL              *hll.Hll  `json:"-"`
	IdentifiedAnonymousIDHLL    *hll.Hll  `json:"-"`
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
