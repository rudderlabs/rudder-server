package gateway

import (
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// NewSourceStat creates a new source stat for a gateway request
func (gw *Handle) NewSourceStat(arctx *gwtypes.AuthRequestContext, reqType string) *gwstats.SourceStat {
	return &gwstats.SourceStat{
		Source:      arctx.SourceTag(),
		SourceID:    arctx.SourceID,
		WriteKey:    arctx.WriteKey,
		ReqType:     reqType,
		WorkspaceID: arctx.WorkspaceID,
		SourceType:  arctx.SourceCategory,
	}
}

func (gw *Handle) newSourceStatTagsWithReason(s *backendconfig.SourceT, reqType, reason string) stats.Tags {
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
