package gateway

import (
	"github.com/rudderlabs/rudder-go-kit/stats"
	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
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

func (gw *Handle) newSourceStatTagsWithReason(arctx *gwtypes.AuthRequestContext, reqType, reason string) stats.Tags {
	tags := stats.Tags{
		"source":       arctx.SourceTag(),
		"source_id":    arctx.SourceID,
		"write_key":    arctx.WriteKey,
		"req_type":     reqType,
		"workspace_id": arctx.WorkspaceID,
		"source_type":  arctx.SourceCategory,
	}
	if reason != "" {
		tags["reason"] = reason
	}
	return tags
}

func (gw *Handle) newReqTypeStatsTagsWithReason(reqType, reason string) stats.Tags {
	tags := stats.Tags{
		"req_type": reqType,
	}
	if reason != "" {
		tags["reason"] = reason
	}
	return tags
}
