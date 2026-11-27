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

// v2Opt configures a [ManagerV2].
type v2Opt func(*ManagerV2)

// withAPIFactory sets how the REST client is built for the destination's credentials.
func withAPIFactory(newAPI func(creds hpCredentials) (hpAPI, error)) v2Opt {
	return func(m *ManagerV2) {
		m.newAPI = newAPI
	}
}

// withWarehouseSession sets how a session for running SQL is opened.
func withWarehouseSession(newSession func(destConf *destConfig) warehouseSession) v2Opt {
	return func(m *ManagerV2) {
		m.newSession = newSession
	}
}

// withState sets the destination's channel state, which defaults to this process's state for the destination.
func withState(state *hpState) v2Opt {
	return func(m *ManagerV2) {
		m.state = state
	}
}

// withClock sets the clock.
func withClock(now func() time.Time) v2Opt {
	return func(m *ManagerV2) {
		m.now = now
	}
}

// withMaxChunkRawBytes sets the uncompressed size compressed per chunk attempt, normally set in MB by config.
func withMaxChunkRawBytes(n int) v2Opt {
	return func(m *ManagerV2) {
		m.config.maxChunkRawBytes = config.SingleValueLoader(n)
	}
}

// NewV2 creates a [ManagerV2] for the destination. base provides the helpers shared with the classic path.
func NewV2(conf *config.Config, statsFactory stats.Stats, destination *backendconfig.DestinationT, base *Manager, opts ...v2Opt) *ManagerV2 {
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
	m.newSession = m.newSQLSession
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *ManagerV2) newSQLSession(destConf *destConfig) warehouseSession {
	return &sqlWarehouseSession{
		database:  destConf.Database,
		namespace: destConf.Namespace,
		connect: func(ctx context.Context) (*snowflake.Snowflake, error) {
			return m.connectSnowflake(ctx, destConf.Namespace)
		},
	}
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

// abort adds aborted jobs, keeping the first reason.
func (r *tableResult) abort(jobIDs []int64, reason string) {
	r.abortedJobs = append(r.abortedJobs, jobIDs...)
	if r.abortReason == "" {
		r.abortReason = reason
	}
}

// uploadResult accumulates the outcome of an upload, table by table.
type uploadResult struct {
	out         common.AsyncUploadOutput
	importInfos []*importInfo
	discards    *importInfo // a single import for the discards of every table
}

func (u *uploadResult) add(res tableResult) {
	u.out.ImportingJobIDs = append(u.out.ImportingJobIDs, res.importingJobs...)
	if res.imported != nil {
		u.importInfos = append(u.importInfos, res.imported)
	}
	if res.discards != nil {
		if u.discards == nil {
			u.discards = res.discards
		} else {
			u.discards.Count += res.discards.Count
			u.discards.Offset = res.discards.Offset
		}
	}
	u.fail(res.failedJobs, res.failedReason)
	u.abort(res.abortedJobs, res.abortReason)
}

// fail adds failed jobs, keeping the first reason.
func (u *uploadResult) fail(jobIDs []int64, reason string) {
	u.out.FailedJobIDs = append(u.out.FailedJobIDs, jobIDs...)
	if u.out.FailedReason == "" && len(jobIDs) > 0 {
		u.out.FailedReason = reason
	}
}

// abort adds aborted jobs, keeping the first reason.
func (u *uploadResult) abort(jobIDs []int64, reason string) {
	u.out.AbortJobIDs = append(u.out.AbortJobIDs, jobIDs...)
	if u.out.AbortReason == "" && len(jobIDs) > 0 {
		u.out.AbortReason = reason
	}
}

// output returns the upload's output, with the imports to poll as its importing parameters.
func (u *uploadResult) output(destinationID string) (common.AsyncUploadOutput, error) {
	importInfos := u.importInfos
	if u.discards != nil {
		importInfos = append(importInfos, u.discards)
	}
	if len(importInfos) > 0 {
		params, err := jsonrs.Marshal(common.ImportParameters{ImportId: importInfos, ImportCount: len(u.out.ImportingJobIDs)})
		if err != nil {
			return common.AsyncUploadOutput{}, fmt.Errorf("marshalling import id: %w", err)
		}
		u.out.ImportingParameters = json.RawMessage(params)
	}
	u.out.ImportingCount = len(u.out.ImportingJobIDs)
	u.out.FailedCount = len(u.out.FailedJobIDs)
	u.out.AbortCount = len(u.out.AbortJobIDs)
	u.out.DestinationID = destinationID
	return u.out, nil
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
	infos, overflowed, oversized := tablesToSend(events, maxInsertRequestSizeBytes)

	var result uploadResult
	for _, info := range infos {
		result.add(m.sendTable(ctx, api, session, &destConf, info))
	}
	result.fail(jobIDsOf(overflowed), fmt.Sprintf("some events were not uploaded because they exceeded the max insert request size: %d bytes", maxInsertRequestSizeBytes))
	result.abort(jobIDsOf(oversized), fmt.Sprintf("some events were aborted because they exceeded the max insert request size: %d bytes", maxInsertRequestSizeBytes))

	out, err := result.output(asyncDest.Destination.ID)
	if err != nil {
		return m.abortJobs(asyncDest, err.Error())
	}
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

// tablesToSend groups the events by table, in job ID order, and splits off those exceeding the max insert request size
// (see [splitEventsExceedingMaxInsertRequestSize]). Events are sorted before splitting, so that the jobs left for a
// retry come after the ones sent. Tables come in ascending latest job ID, so that offsets on the shared discards
// channel only grow.
func tablesToSend(events []*event, maxInsertRequestSizeBytes int64) (infos []*uploadInfo, overflowed, oversized []*event) {
	grouped := lo.GroupBy(events, func(e *event) string { return e.Message.Metadata.Table })
	for _, tableEvents := range grouped {
		slices.SortStableFunc(tableEvents, func(a, b *event) int { return cmp.Compare(a.Metadata.JobID, b.Metadata.JobID) })
	}
	included, overflowed, oversized := splitEventsExceedingMaxInsertRequestSize(grouped, maxInsertRequestSizeBytes)
	infos = lo.MapToSlice(included, func(table string, tableEvents []*event) *uploadInfo {
		return &uploadInfo{
			tableName:    table,
			events:       tableEvents,
			jobIDs:       jobIDsOf(tableEvents),
			eventsSchema: schemaFromEvents(tableEvents),
			latestJobID:  tableEvents[len(tableEvents)-1].Metadata.JobID,
		}
	})
	slices.SortFunc(infos, func(a, b *uploadInfo) int { return cmp.Compare(a.latestJobID, b.latestJobID) })
	return infos, overflowed, oversized
}

// sendTable sends one table's events: it makes sure the table has all their columns, prepares their values, sends
// their discards, then appends their rows to the table's channel.
func (m *ManagerV2) sendTable(ctx context.Context, api hpAPI, session warehouseSession, destConf *destConfig, info *uploadInfo) tableResult {
	log := m.logger.Withn(
		append([]logger.Field{
			logger.NewStringField("table", info.tableName),
			logger.NewIntField("events", int64(len(info.events))),
		}, jobIDRangeFields(info.jobIDs)...)...,
	)

	schema, err := m.ensureSchema(ctx, session, destConf.Namespace, info.tableName, info.eventsSchema)
	if err != nil {
		return failedTable(log, info, fmt.Errorf("preparing table %s: %w", info.tableName, err))
	}

	// Discards are sent before the table's rows, as on the classic path: if the rows then fail, the retry
	// duplicates discards rather than rows.
	var discardsImport *importInfo
	if discards := m.prepareEvents(info, schema); len(discards) > 0 {
		if discardsImport, err = m.sendDiscards(ctx, api, session, destConf, discards, info.latestJobID); err != nil {
			return failedTable(log, info, fmt.Errorf("sending discards for %s: %w", info.tableName, err))
		}
	}

	ch, err := m.channel(ctx, api, session, destConf, info.tableName, info.eventsSchema)
	if err != nil {
		res := failedTable(log, info, err) // the error names the table's pipe
		res.discards = discardsImport
		return res
	}
	m.countDuplicates(log, ch, info)

	rows, rowJobIDs, unmarshalable, marshalErr := marshalRows(info.events)
	res := m.appendRows(ctx, api, ch, info.tableName, rows, rowJobIDs, log)
	res.discards = discardsImport
	if len(unmarshalable) > 0 {
		res.abort(unmarshalable, fmt.Sprintf("marshalling event: %v", marshalErr))
	}
	log.Infon("Sent events", logger.NewIntField("importing", int64(len(res.importingJobs))))
	return res
}

// failedTable fails all of a table's jobs, or aborts them if retrying can't help.
func failedTable(log logger.Logger, info *uploadInfo, err error) tableResult {
	log.Warnn("Failed to send events", obskit.Error(err))
	if errors.Is(err, errAbort) {
		return tableResult{abortedJobs: info.jobIDs, abortReason: err.Error()}
	}
	return tableResult{failedJobs: info.jobIDs, failedReason: err.Error()}
}

// prepareEvents detects the values that don't match the table's columns, which it returns as discards, and prepares
// the other values for the high-performance architecture.
func (m *ManagerV2) prepareEvents(info *uploadInfo, schema whutils.ModelTableSchema) []discardInfo {
	formattedTS := m.now().Format(misc.RFC3339Milli)
	var discards []discardInfo
	for _, e := range info.events {
		discards = append(discards, discardRecordsFromEvent(m.logger, e, schema, info.tableName, formattedTS, false)...)
		prepareValuesHP(e, schema)
	}
	return discards
}

// marshalRows encodes each event's data as one JSON row. Events that can't be encoded are returned apart, with the last error.
func marshalRows(events []*event) (rows [][]byte, jobIDs, unmarshalable []int64, lastErr error) {
	rows = make([][]byte, 0, len(events))
	jobIDs = make([]int64, 0, len(events))
	for _, e := range events {
		row, err := jsonrs.Marshal(e.Message.Data)
		if err != nil {
			unmarshalable, lastErr = append(unmarshalable, e.Metadata.JobID), err
			continue
		}
		rows = append(rows, row)
		jobIDs = append(jobIDs, e.Metadata.JobID)
	}
	return rows, jobIDs, unmarshalable, lastErr
}

// appendRows chunks the rows and appends the chunks in order, each chunk's end offset being its last job ID.
// Rows too large for any request are aborted. If an append fails, [ManagerV2.settleFailedAppend] decides which
// chunks stay importing.
func (m *ManagerV2) appendRows(ctx context.Context, api hpAPI, ch *hpChannel, table string, rows [][]byte, jobIDs []int64, log logger.Logger) tableResult {
	var res tableResult
	chunks, oversized := restapi.NewChunker(m.config.maxChunkRawBytes.Load()).Chunk(rows)
	if len(oversized) > 0 {
		res.abort(lo.Map(oversized, func(i, _ int) int64 { return jobIDs[i] }),
			"some events were aborted because they exceed the 4 MiB request limit even when compressed")
	}
	for i, chunk := range chunks {
		if err := m.appendChunk(ctx, api, ch, chunk.Body, jobIDs[chunk.First], jobIDs[chunk.Last]); err != nil {
			log.Warnn("Failed to append rows, reopening the channel",
				logger.NewIntField("chunk", int64(i)),
				logger.NewIntField("chunks", int64(len(chunks))),
				obskit.Error(err),
			)
			res.importingJobs, res.failedJobs = m.settleFailedAppend(ctx, api, ch, table, chunks, i, jobIDs, log)
			res.failedReason = err.Error()
			break
		}
		res.importingJobs = append(res.importingJobs, jobIDs[chunk.First:chunk.Last+1]...)
	}
	if n := len(res.importingJobs); n > 0 {
		res.imported = &importInfo{
			ChannelID: ch.ref.Channel,
			Offset:    strconv.FormatInt(res.importingJobs[n-1], 10),
			Table:     table,
			Count:     n,
			Version:   versionHP,
			Schema:    ch.ref.Schema,
			Database:  ch.ref.Database,
		}
	}
	return res
}

// settleFailedAppend decides the outcome of every chunk once the chunk at index failed did.
//
// The failed append may still have reached Snowflake (e.g. its response was lost after the retries), and the chunks
// accepted before it aren't committed yet. Reopening the channel discards whatever isn't committed, so its committed
// offset then tells exactly which chunks landed: those stay importing, every other one fails. If the channel can't
// be reopened, the chunks accepted so far stay importing for poll to confirm, and the rest fail.
func (m *ManagerV2) settleFailedAppend(ctx context.Context, api hpAPI, ch *hpChannel, table string, chunks []restapi.Chunk, failed int, jobIDs []int64, log logger.Logger) (importing, failedJobs []int64) {
	reopened, err := m.openChannel(ctx, api, ch.ref)
	if err != nil {
		log.Warnn("Failed to reopen channel after a failed append", obskit.Error(err))
		ch.broken = true
		return chunkJobIDs(chunks[:failed], jobIDs), chunkJobIDs(chunks[failed:], jobIDs)
	}
	m.state.channels[table] = reopened
	committed := lastOffset(reopened)
	for _, c := range chunks {
		if jobIDs[c.Last] <= committed {
			importing = append(importing, jobIDs[c.First:c.Last+1]...)
		} else {
			failedJobs = append(failedJobs, jobIDs[c.First:c.Last+1]...)
		}
	}
	return importing, failedJobs
}

// chunkJobIDs returns the job IDs of the rows in the chunks.
func chunkJobIDs(chunks []restapi.Chunk, jobIDs []int64) []int64 {
	var ids []int64
	for _, c := range chunks {
		ids = append(ids, jobIDs[c.First:c.Last+1]...)
	}
	return ids
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
