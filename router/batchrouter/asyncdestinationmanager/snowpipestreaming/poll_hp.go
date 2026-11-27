package snowpipestreaming

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/restapi"
)

// importState is the state of an import, derived from its channel's offsets.
type importState int

const (
	importInProgress importState = iota
	importCommitted
	importFailed
)

// resolveImport decides the state of an import from the channel's committed offset, the offset this process last
// appended on the channel (empty if unknown, e.g. after a restart or a reopen) and the import's expected offset.
//
// An empty committed offset is not an error: a channel reports none until its first commit, and none again after the
// table is replaced. It is handled before parsing, never as a number. For a failed import, failed is the range of
// failed job IDs, or nil when all of the import's jobs failed.
func resolveImport(committed, appended, expected string) (state importState, failed *failedJobIds, err error) {
	expectedID, err := strconv.ParseInt(expected, 10, 64)
	if err != nil {
		return importFailed, nil, fmt.Errorf("parsing expected offset %q: %w", expected, err)
	}
	appendedReached := false
	if appended != "" {
		appendedID, err := strconv.ParseInt(appended, 10, 64)
		if err != nil {
			return importFailed, nil, fmt.Errorf("parsing appended offset %q: %w", appended, err)
		}
		appendedReached = appendedID >= expectedID
	}

	if committed == "" {
		if appendedReached {
			return importInProgress, nil, nil // nothing committed yet on a fresh channel
		}
		return importFailed, nil, fmt.Errorf("nothing committed on the channel and rows up to offset %d are no longer pending", expectedID)
	}
	committedID, err := strconv.ParseInt(committed, 10, 64)
	if err != nil {
		return importFailed, nil, fmt.Errorf("parsing committed offset %q: %w", committed, err)
	}
	if committedID >= expectedID {
		return importCommitted, nil, nil
	}
	if !appendedReached {
		return importFailed, &failedJobIds{Start: committedID + 1, End: expectedID},
			fmt.Errorf("rows lost: committed offset %d, expected offset %d, no longer pending", committedID, expectedID)
	}
	return importInProgress, nil, nil
}

// Poll checks the imports against their channels' status. Imports that reached a terminal state are remembered until
// every import of the batch has, so later polls only check the rest. Channels of failed imports are reopened right
// away, which discards their uncommitted rows, so that the jobs failed here can't also be committed later.
func (m *ManagerV2) Poll(_ context.Context, pollInput common.AsyncPoll) common.PollStatusResponse {
	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	ctx := context.Background() // see Upload

	var infos []*importInfo
	if err := jsonrs.Unmarshal([]byte(pollInput.ImportId), &infos); err != nil {
		m.logger.Errorn("Failed to unmarshal import id", obskit.Error(err))
		return common.PollStatusResponse{
			StatusCode: http.StatusBadRequest,
			Complete:   true,
			HasFailed:  true,
			Error:      fmt.Errorf("unmarshalling import id: %w", err).Error(),
		}
	}

	// After a restart, Poll can run before any Upload: the client is built from the destination's config.
	var destConf destConfig
	if err := destConf.Decode(m.destination.Config); err != nil {
		return m.failAll(infos, fmt.Errorf("decoding destination config: %w", err))
	}
	if _, err := m.apiFor(&destConf); err != nil {
		return m.failAll(infos, err)
	}

	pending := lo.Filter(infos, func(info *importInfo, _ int) bool {
		_, done := m.polled[info.Table]
		return !done
	})
	inProgress := m.checkImports(ctx, destConf.Database, pending)
	if len(inProgress) > 0 {
		m.stats.pollingInProgress.Increment()
		if m.pollingStartTime.IsZero() {
			m.pollingStartTime = m.now()
			return common.PollStatusResponse{InProgress: true}
		}
		duration, threshold := m.now().Sub(m.pollingStartTime), m.config.stuckPipelineTime.Load()
		if duration <= threshold {
			return common.PollStatusResponse{InProgress: true}
		}
		m.logger.Warnn("Stuck snowpipe pipeline detected",
			logger.NewDurationField("duration", duration),
			logger.NewDurationField("threshold", threshold),
			logger.NewIntField("imports", int64(len(inProgress))),
		)
		for _, info := range inProgress {
			m.markFailed(info, nil, fmt.Sprintf("events still in progress after threshold: %s > %s", duration, threshold))
		}
	}

	results := lo.Map(infos, func(info *importInfo, _ int) *importInfo { return m.polled[info.Table] })
	isFailed := func(info *importInfo, _ int) bool { return info.Failed }
	m.recoverFailedImports(ctx, destConf.Database, lo.Filter(results, isFailed))
	m.updateJobStatistics(results)
	m.resetPolling()
	// Filtered again: recovering a failed import can find it committed after all.
	return m.base.buildPollStatusResponse(results, lo.Filter(results, isFailed))
}

// statusResult is the status of an import's channel: nil if the channel (or its table) doesn't exist.
type statusResult struct {
	status *restapi.ChannelStatus
	err    error
}

// checkImports reads the status of each import's channel and records the imports that reached a terminal state.
// It returns the imports still in progress, including those whose status couldn't be read.
func (m *ManagerV2) checkImports(ctx context.Context, database string, infos []*importInfo) []*importInfo {
	statuses := m.fetchStatuses(ctx, database, infos)
	var inProgress []*importInfo
	for _, info := range infos {
		if m.resolve(info, statuses[info]) == importInProgress {
			inProgress = append(inProgress, info)
		}
	}
	return inProgress
}

// fetchStatuses reads the status of every import's channel, at most pollConcurrency at a time.
func (m *ManagerV2) fetchStatuses(ctx context.Context, database string, infos []*importInfo) map[*importInfo]statusResult {
	var (
		mu       sync.Mutex
		statuses = make(map[*importInfo]statusResult, len(infos))
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(m.config.pollConcurrency.Load())
	for _, info := range infos {
		g.Go(func() error {
			status, found, err := m.bulkChannelStatus(gctx, pipeOf(info, database), info.ChannelID)
			if !found {
				status = nil
			}
			mu.Lock()
			statuses[info] = statusResult{status: status, err: err}
			mu.Unlock()
			return nil // a failed read leaves this import in progress, without stopping the others
		})
	}
	_ = g.Wait()
	return statuses
}

// resolve decides the state of one import from its channel's status, recording it when it is terminal.
func (m *ManagerV2) resolve(info *importInfo, res statusResult) importState {
	log := m.logger.Withn(
		logger.NewStringField("table", info.Table),
		logger.NewStringField("channel", info.ChannelID),
		logger.NewStringField("expectedOffset", info.Offset),
	)
	clearFailure(info)

	switch {
	case res.err != nil:
		log.Warnn("Failed to get channel status, polling again", obskit.Error(res.err))
		return importInProgress
	case res.status == nil:
		m.stats.nullOffsetRecovery("channel_not_found").Increment()
		m.markFailed(info, nil, "channel not found")
		return importFailed
	case !res.status.Healthy():
		m.markFailed(info, nil, fmt.Sprintf("channel status is %s", res.status.ChannelStatusCode))
		return importFailed
	}

	committed, appended := res.status.CommittedOffset(), ""
	if ch := m.channelFor(info); ch != nil {
		m.recordRowErrors(log, ch, res.status)
		appended = ch.appendedOffset
		ch.committedOffset = committed
	}

	state, failedRange, err := resolveImport(committed, appended, info.Offset)
	switch state {
	case importCommitted:
		m.polled[info.Table] = info
	case importFailed:
		branch := "lost"
		if committed == "" {
			branch = "empty_offset"
		}
		m.stats.nullOffsetRecovery(branch).Increment()
		log.Warnn("Import failed",
			logger.NewStringField("committedOffset", committed),
			logger.NewStringField("appendedOffset", appended),
			obskit.Error(err),
		)
		m.markFailed(info, failedRange, err.Error())
	}
	return state
}

func (m *ManagerV2) bulkChannelStatus(ctx context.Context, pipe restapi.PipeRef, channel string) (status *restapi.ChannelStatus, found bool, err error) {
	defer m.stats.apiTimer("bulk_channel_status")()
	statuses, err := m.api.BulkChannelStatus(ctx, pipe, []string{channel})
	if isTableGone(err) {
		// The table was dropped or replaced: rows not committed by then are gone, and waiting can't bring them back.
		// Snowflake may even have accepted appends for a moment after the drop without ever committing them (verified).
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	st, ok := statuses[channel]
	if !ok {
		return nil, false, nil
	}
	return &st, true, nil
}

// isTableGone reports whether Snowflake says the table, its default pipe or its schema no longer exists.
func isTableGone(err error) bool {
	apiErr, ok := errors.AsType[*restapi.APIError](err)
	if !ok {
		return false
	}
	switch apiErr.Code {
	case restapi.ErrCodeTableDoesNotExist, restapi.ErrCodePipeDoesNotExist, restapi.ErrCodeSchemaDoesNotExist:
		return true
	}
	return false
}

// pipeOf returns the pipe an import was written to. Imports from before the database was recorded use the current one.
func pipeOf(info *importInfo, database string) restapi.PipeRef {
	return restapi.DefaultPipe(lo.CoalesceOrEmpty(info.Database, database), info.Schema, info.Table)
}

// channelFor returns the cached channel an import was written to, if this process still has it open.
func (m *ManagerV2) channelFor(info *importInfo) *hpChannel {
	ch, ok := m.state.channels[info.Table]
	if !ok || ch.ref.Channel != info.ChannelID || !ch.writesTo(lo.CoalesceOrEmpty(info.Database, ch.ref.Database), info.Schema) {
		return nil
	}
	return ch
}

// recordRowErrors reports the rows Snowflake skipped since the last poll. Skipped rows don't fail the import:
// Snowflake commits the other rows, so retrying would duplicate them. rows_error_count is cumulative per channel,
// so the delta since the channel was opened or last polled is reported, never a negative one.
func (m *ManagerV2) recordRowErrors(log logger.Logger, ch *hpChannel, status *restapi.ChannelStatus) {
	delta := status.RowsErrorCount - ch.rowsErrorBaseline
	ch.rowsErrorBaseline = status.RowsErrorCount
	if delta <= 0 {
		return
	}
	m.stats.rowErrors.Count(int(delta))
	// The message is redacted by Snowflake; payloads are never logged.
	log.Warnn("Snowflake skipped rows", logger.NewIntField("rowErrors", delta), logger.NewStringField("lastError", lo.FromPtr(status.LastErrorMessage)))
}

func (m *ManagerV2) markFailed(info *importInfo, failedRange *failedJobIds, reason string) {
	info.Failed, info.FailedJobIds, info.Reason = true, failedRange, reason
	m.polled[info.Table] = info
}

// clearFailure resets an import's failure, before it is resolved again.
func clearFailure(info *importInfo) {
	info.Failed, info.Reason, info.FailedJobIds = false, "", nil
}

// resetPolling forgets the polling state of the batch, once all its imports reached a terminal state.
func (m *ManagerV2) resetPolling() {
	m.pollingStartTime = time.Time{}
	m.polled = make(map[string]*importInfo)
}

// recoverFailedImports reopens the channels of failed imports. Reopening discards their uncommitted rows, so the failed
// jobs can't be committed after being retried. The committed offset at reopen is final: jobs at or below it are
// committed, and only the jobs above it fail.
func (m *ManagerV2) recoverFailedImports(ctx context.Context, database string, failed []*importInfo) {
	for _, info := range failed {
		log := m.logger.Withn(logger.NewStringField("table", info.Table), logger.NewStringField("channel", info.ChannelID))
		ref := restapi.ChannelRef{PipeRef: pipeOf(info, database), Channel: info.ChannelID}
		ch, err := m.openChannel(ctx, m.api, ref)
		if err != nil {
			// The next upload reopens it. Until then nothing else is appended, so the outcome stands.
			log.Warnn("Failed to reopen channel of failed import", obskit.Error(err))
			delete(m.state.channels, info.Table)
			continue
		}
		m.state.channels[info.Table] = ch

		state, failedRange, _ := resolveImport(ch.committedOffset, "", info.Offset)
		switch state {
		case importCommitted:
			log.Infon("Import committed before its channel was reopened", logger.NewStringField("committedOffset", ch.committedOffset))
			clearFailure(info)
		case importFailed:
			info.FailedJobIds = failedRange
		}
	}
}

// failAll fails every import of the batch, when their channels can't be checked at all.
func (m *ManagerV2) failAll(infos []*importInfo, err error) common.PollStatusResponse {
	m.logger.Warnn("Failing all imports", obskit.Error(err))
	for _, info := range infos {
		info.Failed, info.FailedJobIds, info.Reason = true, nil, err.Error()
	}
	m.updateJobStatistics(infos)
	m.resetPolling()
	return m.base.buildPollStatusResponse(infos, infos)
}

func (m *ManagerV2) updateJobStatistics(infos []*importInfo) {
	var succeeded, failed int
	for _, info := range infos {
		if info.Failed {
			failed += info.Count
		} else {
			succeeded += info.Count
		}
	}
	m.stats.jobs.succeeded.Count(succeeded)
	m.stats.jobs.failed.Count(failed)
}

// GetUploadStats maps the polled imports back to jobs; it is the same as on the classic path.
func (m *ManagerV2) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	return m.base.GetUploadStats(input)
}
