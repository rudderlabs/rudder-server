package snowpipestreaming

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/restapi"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// versionHP is the [importInfo.Version] of imports created by [ManagerV2].
const versionHP = 2

type (
	// hpAPI is the part of the Snowpipe Streaming HP REST API that [ManagerV2] uses. See [restapi.Client].
	hpAPI interface {
		OpenChannel(ctx context.Context, ref restapi.ChannelRef) (*restapi.OpenChannelResponse, error)
		AppendRows(ctx context.Context, ref restapi.ChannelRef, req restapi.AppendRowsRequest) (*restapi.AppendRowsResponse, error)
		BulkChannelStatus(ctx context.Context, pipe restapi.PipeRef, channelNames []string) (map[string]restapi.ChannelStatus, error)
	}

	// hpChannel is an open channel, one per table. There is at most one append in flight per channel.
	hpChannel struct {
		ref restapi.ChannelRef
		// continuationToken is the latest token Snowflake returned for the channel. Appending with an older one
		// makes Snowflake drop the rows silently, so it is replaced after every successful call.
		continuationToken string
		// appendedOffset is the end offset of the last chunk Snowflake accepted since the channel was opened.
		// It is empty after an open, since opening a channel discards its uncommitted rows.
		appendedOffset string
		// committedOffset is the committed offset Snowflake last reported for the channel.
		committedOffset string
		// rowsErrorBaseline is the channel's cumulative rows_error_count when it was opened or last polled.
		rowsErrorBaseline int64
		// broken is set when an append failed: the channel is reopened before its next append.
		broken bool
	}

	// hpCredentials identifies the Snowflake user an API client was built for.
	hpCredentials struct {
		account, user, role, privateKey, passphrase string
	}

	// ManagerV2 uploads to Snowpipe Streaming through Snowflake's high-performance (HP) REST API, without snowpipe-clients.
	//
	// Compared to the classic [Manager], it owns the table schema (HP doesn't return it), validates and creates columns
	// before sending (HP silently drops unknown fields), sends JSON values natively, and treats an empty committed offset
	// as "nothing committed yet". Offsets are job IDs: rows are sent in job ID order, split in chunks of at most 4 MiB
	// compressed, each chunk's end offset being its last job ID.
	ManagerV2 struct {
		base         *Manager // shared helpers: reading the batch file, discards, abort and fail outputs
		logger       logger.Logger
		statsFactory stats.Stats
		destination  *backendconfig.DestinationT
		now          func() time.Time
		channelName  string // <destinationID>-<instanceID>: each pod writes only its own channels

		// newAPI builds the REST client for the destination's credentials.
		newAPI func(creds hpCredentials) (hpAPI, error)
		// newSession opens a session for running SQL in the given namespace.
		newSession func(destConf *destConfig) warehouseSession

		// state holds the destination's open channels. It outlives the manager, which batchrouter rebuilds whenever
		// the destination's config changes, so that imports in flight keep their channel's state across a rebuild.
		// Its mutex serialises Upload and Poll, including across an old and a new manager of the same destination.
		state            *hpState
		api              hpAPI
		apiCreds         hpCredentials
		schemas          hpSchemas
		polled           map[string]*importInfo // imports that reached a terminal state, by table
		pollingStartTime time.Time

		config struct {
			schemaCacheTTL    config.ValueLoader[time.Duration] // cached table schemas are read again after this long
			maxChunkRawBytes  config.ValueLoader[int]           // uncompressed bytes compressed per chunk attempt
			pollConcurrency   config.ValueLoader[int]           // concurrent bulk channel status calls per poll
			maxRetries        int                               // retries of a retryable REST call
			retryWaitMin      time.Duration                     // minimum backoff between retries
			retryWaitMax      time.Duration                     // maximum backoff between retries
			timeout           time.Duration                     // timeout of a single REST call
			maxConnsPerHost   int                               // HTTP connections per Snowflake host
			stuckPipelineTime config.ValueLoader[time.Duration] // polling longer than this fails the in-progress imports
		}

		stats hpStats
	}
)

// NewV2 creates a [ManagerV2] for the destination. base provides the helpers shared with the classic path.
func NewV2(conf *config.Config, statsFactory stats.Stats, destination *backendconfig.DestinationT, base *Manager) *ManagerV2 {
	m := &ManagerV2{
		base:         base,
		logger:       base.logger.Withn(logger.NewIntField("version", versionHP)),
		statsFactory: statsFactory,
		destination:  destination,
		now:          timeutil.Now,
		channelName:  destination.ID + "-" + base.config.instanceID,
		state:        processHPStates.get(destination.ID),
		schemas:      hpSchemas{cache: make(map[string]cachedSchema)},
		polled:       make(map[string]*importInfo),
	}
	m.config.schemaCacheTTL = conf.GetReloadableDurationVar(30, time.Minute, "SnowpipeStreaming.HP.schemaCacheTTL")
	m.config.maxChunkRawBytes = conf.GetReloadableIntVar(16, int(bytesize.MB), "SnowpipeStreaming.HP.maxChunkRawBytesInMB")
	m.config.pollConcurrency = conf.GetReloadableIntVar(16, 1, "SnowpipeStreaming.HP.pollConcurrency")
	m.config.maxRetries = conf.GetIntVar(5, 1, "SnowpipeStreaming.HP.retryMax")
	m.config.retryWaitMin = conf.GetDurationVar(100, time.Millisecond, "SnowpipeStreaming.HP.retryWaitMin")
	m.config.retryWaitMax = conf.GetDurationVar(10, time.Second, "SnowpipeStreaming.HP.retryWaitMax")
	m.config.timeout = conf.GetDurationVar(60, time.Second, "SnowpipeStreaming.HP.timeout")
	m.config.maxConnsPerHost = conf.GetIntVar(10, 1, "SnowpipeStreaming.HP.maxHTTPConnections")
	m.config.stuckPipelineTime = base.config.stuckPipelineThreshold
	m.stats = newHPStats(statsFactory, destination)

	m.newAPI = m.newRESTClient
	m.newSession = func(destConf *destConfig) warehouseSession {
		return &sqlWarehouseSession{
			database:  destConf.Database,
			namespace: destConf.Namespace,
			connect: func(ctx context.Context) (*snowflake.Snowflake, error) {
				return m.connectSnowflake(ctx, destConf.Namespace)
			},
		}
	}
	return m
}

func (m *ManagerV2) newRESTClient(creds hpCredentials) (hpAPI, error) {
	key, err := restapi.ParsePrivateKey(whutils.FormatPemContent(creds.privateKey), creds.passphrase)
	if err != nil {
		return nil, err
	}
	return restapi.New(
		restapi.Config{Account: creds.account, User: creds.user, Role: creds.role, PrivateKey: key},
		restapi.WithHTTPClient(&http.Client{
			Transport: &http.Transport{
				Proxy:               http.ProxyFromEnvironment,
				MaxConnsPerHost:     m.config.maxConnsPerHost,
				MaxIdleConnsPerHost: m.config.maxConnsPerHost,
				IdleConnTimeout:     90 * time.Second,
			},
			Timeout: m.config.timeout,
		}),
		restapi.WithRetries(m.config.maxRetries, m.config.retryWaitMin, m.config.retryWaitMax),
	)
}

// apiFor returns the REST client for the destination's credentials, building a new one when they change.
// Channels and schemas belong to the account, so they are forgotten along with the old client.
func (m *ManagerV2) apiFor(destConf *destConfig) (hpAPI, error) {
	creds := hpCredentials{
		account:    destConf.Account,
		user:       destConf.User,
		role:       destConf.Role,
		privateKey: destConf.PrivateKey,
		passphrase: destConf.PrivateKeyPassphrase,
	}
	if m.api != nil && m.apiCreds == creds {
		return m.api, nil
	}
	api, err := m.newAPI(creds)
	if err != nil {
		return nil, fmt.Errorf("creating snowflake client: %w", err)
	}
	m.api, m.apiCreds = api, creds
	// Channels belong to the account: a new key, user or role keeps writing to the same channels, but another
	// account has none of them.
	if m.state.account != creds.account {
		clear(m.state.channels)
		m.state.account = creds.account
	}
	return api, nil
}

func (m *ManagerV2) Transform(job *jobsdb.JobT) (string, error) {
	return m.base.Transform(job)
}

// tableResult is the outcome of sending one table's events.
type tableResult struct {
	imported      *importInfo
	discards      *importInfo
	importingJobs []int64
	failedJobs    []int64
	failedReason  string
	abortedJobs   []int64
	abortReason   string
}

// Upload sends the batch to Snowflake, one channel per table, and returns the jobs to poll.
func (m *ManagerV2) Upload(_ context.Context, asyncDest *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	m.state.mu.Lock()
	defer m.state.mu.Unlock()
	m.logger.Infon("Uploading data to snowpipe streaming destination")

	// Not the manager's context: cancelling requests in flight leaves their outcome unknown, causing duplicates on retry.
	// Request timeouts still bound every call.
	ctx := context.Background()

	var destConf destConfig
	if err := destConf.Decode(asyncDest.Destination.Config); err != nil {
		return m.abortJobs(asyncDest, fmt.Errorf("decoding destination config: %w", err).Error())
	}
	api, err := m.apiFor(&destConf)
	if err != nil {
		m.logger.Warnn("Failed to create snowflake client", obskit.Error(err))
		return m.abortJobs(asyncDest, err.Error())
	}
	events, err := m.base.eventsFromFile(asyncDest.FileName, asyncDest.Count)
	if err != nil {
		return m.abortJobs(asyncDest, fmt.Errorf("reading events from file: %w", err).Error())
	}

	session := m.newSession(&destConf)
	defer session.Close(ctx)

	maxInsertRequestSizeBytes := m.base.config.maxInsertRequestSizeBytes.Load()
	// Events are sorted by job ID before splitting, so that the jobs left for a retry come after the ones sent.
	grouped := lo.GroupBy(events, func(e *event) string { return e.Message.Metadata.Table })
	for _, tableEvents := range grouped {
		slices.SortStableFunc(tableEvents, func(a, b *event) int { return cmp.Compare(a.Metadata.JobID, b.Metadata.JobID) })
	}
	included, overflowed, oversized := splitEventsExceedingMaxInsertRequestSize(grouped, maxInsertRequestSizeBytes)
	infos := lo.MapToSlice(included, func(table string, tableEvents []*event) *uploadInfo {
		return &uploadInfo{
			tableName:    table,
			events:       tableEvents,
			jobIDs:       lo.Map(tableEvents, func(e *event, _ int) int64 { return e.Metadata.JobID }),
			eventsSchema: schemaFromEvents(tableEvents),
			latestJobID:  tableEvents[len(tableEvents)-1].Metadata.JobID,
		}
	})
	// Ascending latest job ID, so that offsets on the shared discards channel only increase.
	slices.SortFunc(infos, func(a, b *uploadInfo) int { return cmp.Compare(a.latestJobID, b.latestJobID) })

	var (
		out           common.AsyncUploadOutput
		importInfos   []*importInfo
		discardsInfo  *importInfo
		importingJobs []int64
	)
	for _, info := range infos {
		res := m.sendTable(ctx, api, session, &destConf, info)
		importingJobs = append(importingJobs, res.importingJobs...)
		if res.imported != nil {
			importInfos = append(importInfos, res.imported)
		}
		if res.discards != nil {
			if discardsInfo == nil {
				discardsInfo = res.discards
			} else {
				discardsInfo.Count += res.discards.Count
				discardsInfo.Offset = res.discards.Offset
			}
		}
		out.FailedJobIDs = append(out.FailedJobIDs, res.failedJobs...)
		if out.FailedReason == "" {
			out.FailedReason = res.failedReason
		}
		out.AbortJobIDs = append(out.AbortJobIDs, res.abortedJobs...)
		if out.AbortReason == "" {
			out.AbortReason = res.abortReason
		}
	}
	if discardsInfo != nil {
		importInfos = append(importInfos, discardsInfo)
	}
	if len(overflowed) > 0 {
		out.FailedJobIDs = append(out.FailedJobIDs, jobIDsOf(overflowed)...)
		if out.FailedReason == "" {
			out.FailedReason = fmt.Sprintf("some events were not uploaded because they exceeded the max insert request size: %d bytes", maxInsertRequestSizeBytes)
		}
	}
	if len(oversized) > 0 {
		out.AbortJobIDs = append(out.AbortJobIDs, jobIDsOf(oversized)...)
		if out.AbortReason == "" {
			out.AbortReason = fmt.Sprintf("some events were aborted because they exceeded the max insert request size: %d bytes", maxInsertRequestSizeBytes)
		}
	}

	if len(importInfos) > 0 {
		params, err := jsonrs.Marshal(common.ImportParameters{ImportId: importInfos, ImportCount: len(importingJobs)})
		if err != nil {
			return m.abortJobs(asyncDest, fmt.Errorf("marshalling import id: %w", err).Error())
		}
		out.ImportingParameters = json.RawMessage(params)
	}
	out.ImportingJobIDs = importingJobs
	out.ImportingCount = len(importingJobs)
	out.FailedCount = len(out.FailedJobIDs)
	out.AbortCount = len(out.AbortJobIDs)
	out.DestinationID = asyncDest.Destination.ID

	m.stats.jobs.importing.Count(out.ImportingCount)
	m.stats.jobs.failed.Count(out.FailedCount)
	m.stats.jobs.aborted.Count(out.AbortCount)
	m.logger.Infon("Uploaded data to snowpipe streaming destination",
		logger.NewIntField("importing", int64(out.ImportingCount)),
		logger.NewIntField("failed", int64(out.FailedCount)),
		logger.NewIntField("aborted", int64(out.AbortCount)),
	)
	return out
}

// sendTable sends one table's events, after making sure the table has all their columns.
// Chunks are appended in order; if one fails, its jobs and every later one are failed, while the chunks Snowflake
// already accepted stay importing and are confirmed by poll.
func (m *ManagerV2) sendTable(ctx context.Context, api hpAPI, session warehouseSession, destConf *destConfig, info *uploadInfo) (res tableResult) {
	log := m.logger.Withn(
		append([]logger.Field{
			logger.NewStringField("table", info.tableName),
			logger.NewIntField("events", int64(len(info.events))),
		}, jobIDRangeFields(info.jobIDs)...)...,
	)
	failAll := func(err error) tableResult {
		log.Warnn("Failed to send events", obskit.Error(err))
		if errors.Is(err, errAbort) {
			return tableResult{abortedJobs: info.jobIDs, abortReason: err.Error()}
		}
		return tableResult{failedJobs: info.jobIDs, failedReason: err.Error()}
	}

	schema, err := m.ensureSchema(ctx, session, destConf.Namespace, info.tableName, info.eventsSchema)
	if err != nil {
		return failAll(fmt.Errorf("preparing table %s: %w", info.tableName, err))
	}

	formattedTS := m.now().Format(misc.RFC3339Milli)
	var discards []discardInfo
	for _, e := range info.events {
		discards = append(discards, discardRecordsFromEvent(m.logger, e, schema, info.tableName, formattedTS, false)...)
		prepareValuesHP(e, schema)
	}
	// Discards are sent before the table's rows, as on the classic path: if the rows then fail, the retry
	// duplicates discards rather than rows.
	if len(discards) > 0 {
		if res.discards, err = m.sendDiscards(ctx, api, session, destConf, discards, info.latestJobID); err != nil {
			return failAll(fmt.Errorf("sending discards for %s: %w", info.tableName, err))
		}
	}

	ch, err := m.channel(ctx, api, session, destConf, info.tableName, info.eventsSchema)
	if err != nil {
		res2 := failAll(fmt.Errorf("opening channel for %s: %w", info.tableName, err))
		res2.discards = res.discards
		return res2
	}
	m.countDuplicates(log, ch, info)

	rows := make([][]byte, 0, len(info.events))
	rowJobIDs := make([]int64, 0, len(info.events))
	for _, e := range info.events {
		row, err := jsonrs.Marshal(e.Message.Data)
		if err != nil {
			res.abortedJobs = append(res.abortedJobs, e.Metadata.JobID)
			res.abortReason = fmt.Sprintf("marshalling event: %v", err)
			continue
		}
		rows = append(rows, row)
		rowJobIDs = append(rowJobIDs, e.Metadata.JobID)
	}

	chunks, oversizedRows := restapi.NewChunker(m.config.maxChunkRawBytes.Load()).Chunk(rows)
	if len(oversizedRows) > 0 {
		res.abortedJobs = append(res.abortedJobs, lo.Map(oversizedRows, func(i, _ int) int64 { return rowJobIDs[i] })...)
		res.abortReason = "some events were aborted because they exceed the 4 MiB request limit even when compressed"
	}

	var (
		accepted     int
		importOffset string
	)
	for i, chunk := range chunks {
		start, end := rowJobIDs[chunk.First], rowJobIDs[chunk.Last]
		err := m.appendChunk(ctx, api, ch, chunk.Body, start, end)
		if err == nil {
			res.importingJobs = append(res.importingJobs, rowJobIDs[chunk.First:chunk.Last+1]...)
			accepted += chunk.Last - chunk.First + 1
			importOffset = ch.appendedOffset
			continue
		}

		// The failed append may still have reached Snowflake (e.g. its response was lost after the retries), and the
		// chunks accepted before it aren't committed yet. Reopening the channel discards whatever isn't committed, so
		// its committed offset then tells exactly which chunks landed: those stay importing, every other one fails.
		res.failedReason = fmt.Sprintf("appending rows to %s: %v", info.tableName, err)
		log.Warnn("Failed to append rows, reopening the channel",
			logger.NewIntField("chunk", int64(i)),
			logger.NewIntField("chunks", int64(len(chunks))),
			obskit.Error(err),
		)
		reopened, reopenErr := m.openChannel(ctx, api, ch.ref)
		if reopenErr != nil {
			// The outcome of the failed chunk is unknown: fail it and the rest, and keep the accepted ones for poll.
			log.Warnn("Failed to reopen channel after a failed append", obskit.Error(reopenErr))
			ch.broken = true
			for _, rest := range chunks[i:] {
				res.failedJobs = append(res.failedJobs, rowJobIDs[rest.First:rest.Last+1]...)
			}
			break
		}
		m.state.channels[info.tableName] = reopened
		committed := lastOffset(reopened)
		res.importingJobs, accepted, importOffset = nil, 0, ""
		for _, c := range chunks {
			jobs := rowJobIDs[c.First : c.Last+1]
			if rowJobIDs[c.Last] <= committed {
				res.importingJobs = append(res.importingJobs, jobs...)
				accepted += len(jobs)
				importOffset = strconv.FormatInt(rowJobIDs[c.Last], 10)
				continue
			}
			res.failedJobs = append(res.failedJobs, jobs...)
		}
		break
	}
	if accepted > 0 {
		res.imported = &importInfo{
			ChannelID: ch.ref.Channel,
			Offset:    importOffset,
			Table:     info.tableName,
			Count:     accepted,
			Version:   versionHP,
			Schema:    destConf.Namespace,
			Database:  destConf.Database,
		}
	}
	log.Infon("Sent events", logger.NewIntField("chunks", int64(len(chunks))), logger.NewIntField("accepted", int64(accepted)))
	return res
}

// sendDiscards sends the discarded values of one table to the discards table, with that table's latest job ID as offset.
func (m *ManagerV2) sendDiscards(ctx context.Context, api hpAPI, session warehouseSession, destConf *destConfig, discards []discardInfo, offset int64) (*importInfo, error) {
	table := discardsTable()
	if _, err := m.ensureSchema(ctx, session, destConf.Namespace, table, discardsSchema()); err != nil {
		return nil, fmt.Errorf("preparing discards table: %w", err)
	}
	ch, err := m.channel(ctx, api, session, destConf, table, discardsSchema())
	if err != nil {
		return nil, fmt.Errorf("opening discards channel: %w", err)
	}
	// Every table's discards share this channel, and a retried batch can carry job IDs below ones already sent.
	// The offset must still grow, or poll would see an earlier, higher offset as this import's commit.
	offset = max(offset, lastOffset(ch)+1)

	rows := make([][]byte, 0, len(discards))
	for _, row := range discardRowsHP(discards) {
		b, err := jsonrs.Marshal(row)
		if err != nil {
			return nil, fmt.Errorf("marshalling discard: %w", err)
		}
		rows = append(rows, b)
	}
	chunks, oversizedRows := restapi.NewChunker(m.config.maxChunkRawBytes.Load()).Chunk(rows)
	if len(oversizedRows) > 0 {
		m.logger.Warnn("Dropping discards too large to send", logger.NewIntField("count", int64(len(oversizedRows))))
	}
	for _, chunk := range chunks {
		if err := m.appendChunk(ctx, api, ch, chunk.Body, offset, offset); err != nil {
			ch.broken = true
			return nil, err
		}
	}
	sent := len(discards) - len(oversizedRows)
	m.stats.discards.Count(sent)
	return &importInfo{
		ChannelID: ch.ref.Channel,
		Offset:    strconv.FormatInt(offset, 10),
		Table:     table,
		Count:     sent,
		Version:   versionHP,
		Schema:    destConf.Namespace,
		Database:  destConf.Database,
	}, nil
}

// channel returns the open channel for the table, opening it if needed.
// A new column is accepted on an open channel, so unlike the classic path, adding columns never reopens it.
func (m *ManagerV2) channel(ctx context.Context, api hpAPI, session warehouseSession, destConf *destConfig, table string, eventSchema whutils.ModelTableSchema) (*hpChannel, error) {
	if ch, ok := m.state.channels[table]; ok && !ch.broken && ch.ref.Database == destConf.Database && ch.ref.Schema == destConf.Namespace {
		return ch, nil
	}
	ref := restapi.ChannelRef{PipeRef: restapi.DefaultPipe(destConf.Database, destConf.Namespace, table), Channel: m.channelName}
	ch, err := m.openChannel(ctx, api, ref)
	if isTableGone(err) {
		// The table was dropped after its schema was cached: recreate it and try once more.
		m.logger.Infon("Table missing on channel open, recreating it", logger.NewStringField("table", table), obskit.Error(err))
		delete(m.schemas.cache, schemaKey(destConf.Namespace, table))
		if _, err := m.ensureSchema(ctx, session, destConf.Namespace, table, eventSchema); err != nil {
			return nil, err
		}
		ch, err = m.openChannel(ctx, api, ref)
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
func (m *ManagerV2) openChannel(ctx context.Context, api hpAPI, ref restapi.ChannelRef) (*hpChannel, error) {
	defer m.stats.apiTimer("open_channel")()
	res, err := api.OpenChannel(ctx, ref)
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
func (m *ManagerV2) appendChunk(ctx context.Context, api hpAPI, ch *hpChannel, body []byte, start, end int64) error {
	defer m.stats.apiTimer("append_rows")()
	m.stats.requestBodySize.Observe(float64(len(body)))
	endOffset := strconv.FormatInt(end, 10)
	res, err := api.AppendRows(ctx, ch.ref, restapi.AppendRowsRequest{
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

// countDuplicates reports events that are likely duplicates, without dropping them: job IDs repeated in the batch,
// and job IDs at or below the channel's committed offset. Job IDs are not unique across nodes after a partition
// migration, so events are never skipped on this basis.
func (m *ManagerV2) countDuplicates(log logger.Logger, ch *hpChannel, info *uploadInfo) {
	if info.tableName == whutils.ToProviderCase(whutils.SnowpipeStreaming, whutils.UsersTable) {
		return
	}
	if n := checkForDuplicateIDsInBatch(info.events); n > 0 {
		log.Warnn("Duplicate ids found in the events", logger.NewIntField("duplicateEvents", int64(n)), logger.NewStringField("reason", "batch"))
		m.stats.duplicateEventsInBatch.Count(n)
	}
	if ch.committedOffset == "" {
		return
	}
	committed, err := strconv.ParseInt(ch.committedOffset, 10, 64)
	if err != nil {
		return
	}
	n := lo.CountBy(info.events, func(e *event) bool { return e.Metadata.JobID >= 0 && e.Metadata.JobID <= committed })
	if n > 0 {
		log.Warnn("Duplicate ids found in the events", logger.NewIntField("duplicateEvents", int64(n)), logger.NewStringField("reason", "offset"))
		m.stats.duplicateEventsDueToOffset.Count(n)
	}
}

func (m *ManagerV2) abortJobs(asyncDest *common.AsyncDestinationStruct, reason string) common.AsyncUploadOutput {
	m.stats.jobs.aborted.Count(len(asyncDest.ImportingJobIDs))
	return common.AsyncUploadOutput{
		AbortJobIDs:   asyncDest.ImportingJobIDs,
		AbortCount:    len(asyncDest.ImportingJobIDs),
		AbortReason:   reason,
		DestinationID: asyncDest.Destination.ID,
	}
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

func jobIDsOf(events []*event) []int64 {
	return lo.Map(events, func(e *event, _ int) int64 { return e.Metadata.JobID })
}

// connectSnowflake connects to Snowflake in the given namespace. It builds the Snowflake integration directly rather
// than through the warehouse manager factory, whose stats decorator hides the connection that SHOW COLUMNS needs.
func (m *ManagerV2) connectSnowflake(ctx context.Context, namespace string) (*snowflake.Snowflake, error) {
	sf := snowflake.New(m.base.appConfig, m.base.logger, m.statsFactory)
	sf.SetConnectionTimeout(whutils.GetConnectionTimeout(m.destination.DestinationDefinition.Name, m.destination.ID))
	modelWarehouse := whutils.ModelWarehouse{
		WorkspaceID: m.destination.WorkspaceID,
		Destination: *m.destination,
		Namespace:   namespace,
		Type:        m.destination.DestinationDefinition.Name,
		Identifier:  m.destination.WorkspaceID + ":" + m.destination.ID,
	}
	if err := sf.Setup(ctx, modelWarehouse, whutils.NewNoOpUploader()); err != nil {
		return nil, fmt.Errorf("setting up snowflake: %w", err)
	}
	return sf, nil
}
