package gateway

import (
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-schemas/go/stream"
	gwstats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// NewSourceStat creates a new source stat for a gateway request
func (gw *Handle) NewSourceStat(arctx *gwtypes.AuthRequestContext, reqType string) *gwstats.SourceStat {
	return &gwstats.SourceStat{
		Source:        arctx.SourceTag(),
		SourceID:      arctx.SourceID,
		WriteKey:      arctx.WriteKey,
		ReqType:       reqType,
		WorkspaceID:   arctx.WorkspaceID,
		SourceType:    arctx.SourceCategory,
		SourceDefName: arctx.SourceDefName,
	}
}

func (gw *Handle) newSourceStatTagsWithReason(properties stream.MessageProperties, reqType, reason, writeKey, name string) stats.Tags {
	tags := stats.Tags{
		"source":       misc.GetTagName(writeKey, name),
		"source_id":    properties.SourceID,
		"write_key":    writeKey,
		"req_type":     reqType,
		"workspace_id": properties.WorkspaceID,
		"source_type":  properties.SourceType,
	}
	if reason != "" {
		tags["reason"] = reason
	}
	return tags
}
