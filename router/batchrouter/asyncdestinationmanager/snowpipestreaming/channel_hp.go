package snowpipestreaming

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/restapi"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// writesTo reports whether the channel writes to the given database and schema.
func (ch *hpChannel) writesTo(database, schema string) bool {
	return ch.ref.Database == database && ch.ref.Schema == schema
}

// channel returns the open channel for the table, opening it if needed.
// A new column is accepted on an open channel, so unlike the classic path, adding columns never reopens it.
func (m *ManagerV2) channel(ctx context.Context, session warehouseSession, destConf *destConfig, table string, eventSchema whutils.ModelTableSchema) (*hpChannel, error) {
	if ch, ok := m.state.channels[table]; ok && !ch.isBroken && ch.writesTo(destConf.Database, destConf.Namespace) {
		return ch, nil
	}
	ref := restapi.ChannelRef{PipeRef: restapi.DefaultPipe(destConf.Database, destConf.Namespace, table), Channel: m.channelName}
	ch, err := m.openChannel(ctx, ref)
	if isTableGone(err) {
		// The table was dropped after its schema was cached: recreate it and try once more.
		m.logger.Infon("Table missing on channel open, recreating it", logger.NewStringField("table", table), obskit.Error(err))
		delete(m.schemas.cache, schemaKey(destConf.Namespace, table))
		if _, err := m.ensureSchema(ctx, session, destConf.Namespace, table, eventSchema); err != nil {
			return nil, err
		}
		ch, err = m.openChannel(ctx, ref)
	}
	if err != nil {
		delete(m.state.channels, table)
		if isAuthError(err) {
			return nil, fmt.Errorf("%w: %w", errAbort, err)
		}
		return nil, err
	}
	m.state.channels[table] = ch
	return ch, nil
}

// openChannel opens (or reopens) a channel. Reopening discards the channel's uncommitted rows.
func (m *ManagerV2) openChannel(ctx context.Context, ref restapi.ChannelRef) (*hpChannel, error) {
	defer m.stats.apiTimer("open_channel")()
	res, err := m.api.OpenChannel(ctx, ref)
	if err != nil {
		return nil, err
	}
	if !res.ChannelStatus.Healthy() {
		return nil, fmt.Errorf("channel %s opened with status %s", ref.Channel, res.ChannelStatus.ChannelStatusCode)
	}
	m.logger.Infon("Opened channel",
		logger.NewStringField("pipe", ref.Pipe),
		logger.NewStringField("channel", ref.Channel),
		logger.NewStringField("committedOffset", res.ChannelStatus.CommittedOffset()),
	)
	return &hpChannel{
		ref:               ref,
		continuationToken: res.NextContinuationToken,
		committedOffset:   res.ChannelStatus.CommittedOffset(),
		rowsErrorBaseline: res.ChannelStatus.RowsErrorCount,
	}, nil
}

// appendChunk appends one chunk and moves the channel to the continuation token Snowflake returned.
func (m *ManagerV2) appendChunk(ctx context.Context, ch *hpChannel, body []byte, start, end int64) error {
	defer m.stats.apiTimer("append_rows")()
	m.stats.requestBodySize.Observe(float64(len(body)))
	endOffset := strconv.FormatInt(end, 10)
	res, err := m.api.AppendRows(ctx, ch.ref, restapi.AppendRowsRequest{
		ContinuationToken: ch.continuationToken,
		StartOffsetToken:  strconv.FormatInt(start, 10),
		EndOffsetToken:    endOffset,
		Body:              body,
	})
	if err != nil {
		return err
	}
	ch.continuationToken = res.NextContinuationToken
	ch.appendedOffset = endOffset
	return nil
}

// lastOffset returns the highest offset known on the channel: appended in this process, or committed. MinInt64 if none.
func lastOffset(ch *hpChannel) int64 {
	last := int64(math.MinInt64)
	for _, o := range []string{ch.appendedOffset, ch.committedOffset} {
		if v, err := strconv.ParseInt(o, 10, 64); err == nil && v > last {
			last = v
		}
	}
	return last
}

// isAuthError reports whether Snowflake refused the credentials, the role or the request itself. Retrying can't help,
// so, as on the classic path, the jobs are aborted.
func isAuthError(err error) bool {
	apiErr, ok := errors.AsType[*restapi.APIError](err)
	if !ok {
		return false
	}
	switch apiErr.StatusCode {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
		return true
	}
	return false
}
