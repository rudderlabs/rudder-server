package gateway

import (
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
