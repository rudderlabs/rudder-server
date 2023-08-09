package gateway

import (
	"context"
	"sync/atomic"
	"time"

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

// IncrementRecvCount increments the received count for gateway requests
func (gw *Handle) IncrementRecvCount(count uint64) {
	atomic.AddUint64(&gw.recvCount, count)
}

// IncrementAckCount increments the acknowledged count for gateway requests
func (gw *Handle) IncrementAckCount(count uint64) {
	atomic.AddUint64(&gw.ackCount, count)
}

// printStats prints a debug log containing received and acked events every 10 seconds
func (gw *Handle) printStats(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}

		recvCount := atomic.LoadUint64(&gw.recvCount)
		ackCount := atomic.LoadUint64(&gw.ackCount)
		gw.logger.Debug("Gateway Recv/Ack ", recvCount, ackCount)
	}
}
