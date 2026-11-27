package snowpipestreaming

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/snowflakedb/gosnowflake/v2"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/restapi"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// fakeHP is an in-memory Snowpipe Streaming HP account. Like the real one, an append with a stale continuation token
// is answered with success but its rows are dropped, and reopening a channel discards its uncommitted rows.
type fakeHP struct {
	mu        sync.Mutex
	channels  map[string]*fakeHPChannel // by pipe + "/" + channel
	opens     int
	openErr   func(ref restapi.ChannelRef) error
	appendErr func(ref restapi.ChannelRef, n int) error // n counts appends, from 1
	appends   int
	statusErr error
	dropped   int // appends made with a stale token
}

type fakeHPChannel struct {
	seq       int
	rowSeq    int
	pending   []fakeAppend // accepted, not committed
	committed []fakeAppend
	offset    *string
	rowErrors int64
}

type fakeAppend struct {
	start, end string
	rows       []map[string]any
}

func newFakeHP() *fakeHP { return &fakeHP{channels: map[string]*fakeHPChannel{}} }

func (f *fakeHP) key(ref restapi.ChannelRef) string { return ref.Pipe + "/" + ref.Channel }

func (f *fakeHP) token(ch *fakeHPChannel) string { return fmt.Sprintf("%d_%d", ch.seq, ch.rowSeq) }

func (f *fakeHP) status(ref restapi.ChannelRef, ch *fakeHPChannel) restapi.ChannelStatus {
	return restapi.ChannelStatus{
		ChannelName:              ref.Channel,
		ChannelStatusCode:        restapi.ChannelStatusSuccess,
		PipeName:                 ref.Pipe,
		LastCommittedOffsetToken: ch.offset,
		RowsErrorCount:           ch.rowErrors,
	}
}

func (f *fakeHP) OpenChannel(_ context.Context, ref restapi.ChannelRef) (*restapi.OpenChannelResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opens++
	if f.openErr != nil {
		if err := f.openErr(ref); err != nil {
			return nil, err
		}
	}
	ch, ok := f.channels[f.key(ref)]
	if !ok {
		ch = &fakeHPChannel{}
		f.channels[f.key(ref)] = ch
	} else {
		ch.seq++
		ch.pending = nil // reopening discards uncommitted rows
	}
	ch.rowSeq = 1
	return &restapi.OpenChannelResponse{NextContinuationToken: f.token(ch), ChannelStatus: f.status(ref, ch)}, nil
}

func (f *fakeHP) AppendRows(_ context.Context, ref restapi.ChannelRef, req restapi.AppendRowsRequest) (*restapi.AppendRowsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.appends++
	if f.appendErr != nil {
		if err := f.appendErr(ref, f.appends); err != nil {
			return nil, err
		}
	}
	ch := f.channels[f.key(ref)]
	if ch == nil {
		return nil, &restapi.APIError{StatusCode: http.StatusNotFound, Code: "ERR_CHANNEL_DOES_NOT_EXIST"}
	}
	if req.ContinuationToken != f.token(ch) {
		f.dropped++ // silently dropped, as Snowflake does
		return &restapi.AppendRowsResponse{NextContinuationToken: f.token(ch)}, nil
	}
	ch.pending = append(ch.pending, fakeAppend{start: req.StartOffsetToken, end: req.EndOffsetToken, rows: decodeNDJSON(req.Body)})
	ch.rowSeq++
	return &restapi.AppendRowsResponse{NextContinuationToken: f.token(ch)}, nil
}

func (f *fakeHP) BulkChannelStatus(_ context.Context, pipe restapi.PipeRef, names []string) (map[string]restapi.ChannelStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.statusErr != nil {
		return nil, f.statusErr
	}
	out := map[string]restapi.ChannelStatus{}
	for _, name := range names {
		ref := restapi.ChannelRef{PipeRef: pipe, Channel: name}
		if ch, ok := f.channels[f.key(ref)]; ok {
			out[name] = f.status(ref, ch)
		}
	}
	return out, nil
}

// commit commits every pending append of every channel.
func (f *fakeHP) commit() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ch := range f.channels {
		f.commitN(ch, len(ch.pending))
	}
}

// commitTable commits the first n pending appends of a table's channel.
func (f *fakeHP) commitTable(table string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for k, ch := range f.channels {
		if strings.HasPrefix(k, table+"-STREAMING/") {
			f.commitN(ch, n)
		}
	}
}

func (f *fakeHP) commitN(ch *fakeHPChannel, n int) {
	for _, a := range ch.pending[:n] {
		end := a.end
		ch.offset = &end
	}
	ch.committed = append(ch.committed, ch.pending[:n]...)
	ch.pending = ch.pending[n:]
}

// nolint: unparam
func (f *fakeHP) committedRows(table string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var rows []map[string]any
	for k, ch := range f.channels {
		if strings.HasPrefix(k, table+"-STREAMING/") {
			for _, a := range ch.committed {
				rows = append(rows, a.rows...)
			}
		}
	}
	return rows
}

func (f *fakeHP) appendsOf(table string) []fakeAppend {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeAppend
	for k, ch := range f.channels {
		if strings.HasPrefix(k, table+"-STREAMING/") {
			out = append(append(out, ch.committed...), ch.pending...)
		}
	}
	return out
}

func decodeNDJSON(body []byte) []map[string]any {
	dec, err := zstd.NewReader(nil)
	if err != nil {
		panic(err)
	}
	defer dec.Close()
	raw, err := dec.DecodeAll(body, nil)
	if err != nil {
		panic(err)
	}
	var rows []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
		var row map[string]any
		if err := jsonrs.Unmarshal([]byte(line), &row); err != nil {
			panic(err)
		}
		rows = append(rows, row)
	}
	return rows
}

// fakeWarehouse is an in-memory warehouseSession.
type fakeWarehouse struct {
	tables        map[string]whutils.ModelTableSchema
	columnReads   int
	addedColumns  []string
	loseColumns   bool // AddColumns succeeds without adding anything
	addColumnsErr error
}

func newFakeWarehouse() *fakeWarehouse {
	return &fakeWarehouse{tables: map[string]whutils.ModelTableSchema{}}
}

func (w *fakeWarehouse) Columns(_ context.Context, table string) (whutils.ModelTableSchema, error) {
	w.columnReads++
	schema, ok := w.tables[table]
	if !ok {
		return nil, errTableNotFound
	}
	return maps.Clone(schema), nil
}

func (w *fakeWarehouse) CreateSchema(context.Context) error { return nil }

func (w *fakeWarehouse) CreateTable(_ context.Context, table string, schema whutils.ModelTableSchema) error {
	w.tables[table] = maps.Clone(schema)
	return nil
}

func (w *fakeWarehouse) AddColumns(_ context.Context, table string, columns []whutils.ColumnInfo) error {
	if w.addColumnsErr != nil {
		return w.addColumnsErr
	}
	for _, c := range columns {
		w.addedColumns = append(w.addedColumns, c.Name)
		if !w.loseColumns {
			w.tables[table][c.Name] = c.Type
		}
	}
	return nil
}

func (w *fakeWarehouse) Close(context.Context) {}

var testHPDestination = &backendconfig.DestinationT{
	ID:          "dest-1",
	WorkspaceID: "ws-1",
	DestinationDefinition: backendconfig.DestinationDefinitionT{
		Name: "SNOWPIPE_STREAMING",
	},
	Config: map[string]any{
		"account":    "xy12345",
		"user":       "loader",
		"role":       "LOADER",
		"database":   "DB",
		"namespace":  "rudder",
		"privateKey": "unused by the fake",
	},
}

// newTestManagerV2 creates a manager on the fakes, as its own process (with its own channel state).
// A nil api keeps the real REST client. opts come last, so they override these defaults.
func newTestManagerV2(t *testing.T, api *fakeHP, wh *fakeWarehouse, statsFactory stats.Stats, conf *config.Config, opts ...hpOpt) *ManagerV2 {
	t.Helper()
	if conf == nil {
		conf = config.New()
	}
	conf.Set("INSTANCE_ID", "rudder-0")
	base := New(conf, logger.NOP, statsFactory, testHPDestination)
	defaults := []hpOpt{
		withWarehouseSession(func(*destConfig) warehouseSession { return wh }),
		withState(newHPStates().get(testHPDestination.ID)),
	}
	if api != nil {
		defaults = append(defaults, withAPIFactory(func(hpCredentials) (hpAPI, error) { return api, nil }))
	}
	return NewV2(conf, statsFactory, testHPDestination, base, append(defaults, opts...)...)
}

type testEvent struct {
	table   string
	jobID   int64
	columns map[string]string
	data    map[string]any
}

func writeBatch(t *testing.T, events ...testEvent) (string, []int64) {
	t.Helper()
	var sb strings.Builder
	jobIDs := make([]int64, 0, len(events))
	for _, e := range events {
		line, err := jsonrs.Marshal(map[string]any{
			"message":  map[string]any{"metadata": map[string]any{"table": e.table, "columns": e.columns}, "data": e.data},
			"metadata": map[string]any{"job_id": e.jobID},
		})
		require.NoError(t, err)
		sb.Write(line)
		sb.WriteByte('\n')
		jobIDs = append(jobIDs, e.jobID)
	}
	path := filepath.Join(t.TempDir(), "batch.txt")
	require.NoError(t, os.WriteFile(path, []byte(sb.String()), 0o600))
	return path, jobIDs
}

var trackColumns = map[string]string{"ID": "string", "EVENT": "string", "CONTEXT": "json", "TAGS": "string", "RECEIVED_AT": "datetime"}

func track(jobID int64) testEvent {
	return testEvent{
		table:   "TRACKS",
		jobID:   jobID,
		columns: trackColumns,
		data: map[string]any{
			"ID":          "id-" + strconv.FormatInt(jobID, 10),
			"EVENT":       "clicked",
			"CONTEXT":     map[string]any{"ip": "1.2.3.4", "list": []any{1, 2}},
			"TAGS":        []any{"a", "b"},
			"RECEIVED_AT": "2026-11-24T10:00:00.000Z",
		},
	}
}

func upload(m *ManagerV2, file string, jobIDs []int64) common.AsyncUploadOutput {
	return m.Upload(context.Background(), &common.AsyncDestinationStruct{
		ImportingJobIDs: jobIDs,
		FileName:        file,
		Count:           len(jobIDs),
		Destination:     testHPDestination,
	})
}

func importIDOf(t *testing.T, out common.AsyncUploadOutput) (string, []*importInfo) {
	t.Helper()
	var params common.ImportParameters
	require.NoError(t, jsonrs.Unmarshal(out.ImportingParameters, &params))
	raw, err := jsonrs.Marshal(params.ImportId)
	require.NoError(t, err)
	var infos []*importInfo
	require.NoError(t, jsonrs.Unmarshal(raw, &infos))
	return string(raw), infos
}

func poll(m *ManagerV2, importID string) common.PollStatusResponse {
	return m.Poll(context.Background(), common.AsyncPoll{ImportId: importID})
}

func uploadStats(t *testing.T, m asyncManager, file string, jobIDs []int64, failedParams string) common.GetUploadStatsResponse {
	t.Helper()
	raw, err := os.ReadFile(file)
	require.NoError(t, err)
	var jobs []*jobsdb.JobT
	for i, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var e struct {
			Message any `json:"message"`
		}
		require.NoError(t, jsonrs.Unmarshal([]byte(line), &e))
		payload, err := jsonrs.Marshal(e.Message)
		require.NoError(t, err)
		jobs = append(jobs, &jobsdb.JobT{JobID: jobIDs[i], EventPayload: payload})
	}
	return m.GetUploadStats(common.GetUploadStatsInput{FailedJobParameters: failedParams, ImportingList: jobs})
}

func TestManagerV2(t *testing.T) {
	t.Run("upload creates the table, sends rows in job order with native JSON, and poll completes once committed", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		statsStore, err := memstats.New()
		require.NoError(t, err)
		m := newTestManagerV2(t, api, wh, statsStore, nil)

		file, jobIDs := writeBatch(t, track(1003), track(1001), track(1002))
		out := upload(m, file, jobIDs)
		require.Empty(t, out.FailedJobIDs)
		require.Empty(t, out.AbortJobIDs)
		require.ElementsMatch(t, jobIDs, out.ImportingJobIDs)
		require.Contains(t, wh.tables, "TRACKS")

		importID, infos := importIDOf(t, out)
		require.Len(t, infos, 1)
		require.Equal(t, importInfo{ChannelID: "dest-1-rudder-0", Offset: "1003", Table: "TRACKS", Count: 3, Version: versionHP, Schema: "RUDDER", Database: "DB"}, *infos[0])

		appends := api.appendsOf("TRACKS")
		require.Len(t, appends, 1)
		require.Equal(t, "1001", appends[0].start)
		require.Equal(t, "1003", appends[0].end)
		require.Equal(t, []any{"id-1001", "id-1002", "id-1003"}, []any{appends[0].rows[0]["ID"], appends[0].rows[1]["ID"], appends[0].rows[2]["ID"]})
		require.Equal(t, map[string]any{"ip": "1.2.3.4", "list": []any{float64(1), float64(2)}}, appends[0].rows[0]["CONTEXT"], "json columns are sent natively")
		require.Equal(t, `["a","b"]`, appends[0].rows[0]["TAGS"], "arrays in string columns are sent as JSON text")

		// Nothing committed yet on a fresh channel: in progress, not a failure.
		require.True(t, poll(m, importID).InProgress)
		require.True(t, poll(m, importID).InProgress)

		api.commit()
		res := poll(m, importID)
		require.False(t, res.InProgress)
		require.True(t, res.Complete)
		require.False(t, res.HasFailed)
		require.EqualValues(t, 3, statsStore.Get("snowpipe_streaming_jobs", stats.Tags{
			"module": "batch_router", "workspaceId": "ws-1", "destType": "SNOWPIPE_STREAMING", "destinationId": "dest-1", "version": "2", "status": "succeeded",
		}).LastValue())
		require.Len(t, api.committedRows("TRACKS"), 3)
	})

	t.Run("the next upload uses the latest continuation token and keeps the channel open", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil)

		for _, batch := range [][]testEvent{{track(1), track(2)}, {track(3)}, {track(4), track(5)}} {
			file, jobIDs := writeBatch(t, batch...)
			out := upload(m, file, jobIDs)
			require.Len(t, out.ImportingJobIDs, len(batch))
			importID, _ := importIDOf(t, out)
			api.commit()
			require.False(t, poll(m, importID).HasFailed)
		}
		require.Equal(t, 1, api.opens)
		require.Zero(t, api.dropped, "no append may use a stale continuation token")
		require.Len(t, api.committedRows("TRACKS"), 5)
		require.Equal(t, 2, wh.columnReads, "the schema is read before and after creating the table, then cached")
	})

	t.Run("a new column is added before sending, without reopening the channel", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil)

		file, jobIDs := writeBatch(t, track(1))
		upload(m, file, jobIDs)

		e := track(2)
		e.columns = maps.Clone(trackColumns)
		e.columns["PLAN"] = "string"
		e.data["PLAN"] = "pro"
		file, jobIDs = writeBatch(t, e)
		out := upload(m, file, jobIDs)
		require.Equal(t, []int64{2}, out.ImportingJobIDs)
		require.Equal(t, []string{"PLAN"}, wh.addedColumns)
		require.Equal(t, 1, api.opens)
		appends := api.appendsOf("TRACKS")
		require.Equal(t, "pro", appends[len(appends)-1].rows[0]["PLAN"])
	})

	t.Run("a column still missing after adding it stops the batch: nothing is sent", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		statsStore, err := memstats.New()
		require.NoError(t, err)
		m := newTestManagerV2(t, api, wh, statsStore, nil)
		wh.tables["TRACKS"] = whutils.ModelTableSchema{"ID": "string"}
		wh.loseColumns = true

		file, jobIDs := writeBatch(t, track(1), track(2))
		out := upload(m, file, jobIDs)
		require.ElementsMatch(t, jobIDs, out.FailedJobIDs)
		require.Empty(t, out.ImportingJobIDs)
		require.Zero(t, api.appends)
		require.Positive(t, statsStore.Get("snowpipe_streaming_schema_cache_miss_on_send", stats.Tags{
			"module": "batch_router", "workspaceId": "ws-1", "destType": "SNOWPIPE_STREAMING", "destinationId": "dest-1", "version": "2",
		}).LastValue())
	})

	t.Run("failing to add columns aborts the batch", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil)
		wh.tables["TRACKS"] = whutils.ModelTableSchema{"ID": "string"}
		wh.addColumnsErr = errors.New("insufficient privileges")

		file, jobIDs := writeBatch(t, track(1))
		out := upload(m, file, jobIDs)
		require.Equal(t, jobIDs, out.AbortJobIDs)
		require.Zero(t, api.appends)
	})

	t.Run("values that don't match the column type go to the discards table", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil)
		wh.tables["TRACKS"] = whutils.ModelTableSchema{"ID": "string", "EVENT": "string", "CONTEXT": "json", "TAGS": "string", "RECEIVED_AT": "datetime", "COUNT": "int"}

		e := track(7)
		e.columns = maps.Clone(trackColumns)
		e.columns["COUNT"] = "string"
		e.data["COUNT"] = "not a number"
		file, jobIDs := writeBatch(t, e)
		out := upload(m, file, jobIDs)
		require.Equal(t, []int64{7}, out.ImportingJobIDs)

		_, infos := importIDOf(t, out)
		require.Len(t, infos, 2)
		require.Equal(t, "RUDDER_DISCARDS", infos[1].Table)
		require.Equal(t, "7", infos[1].Offset)
		discards := api.appendsOf("RUDDER_DISCARDS")
		require.Len(t, discards, 1)
		row := discards[0].rows[0]
		require.Equal(t, "COUNT", row["COLUMN_NAME"], "discard fields are provider-cased")
		require.Equal(t, "not a number", row["COLUMN_VALUE"])
		require.Nil(t, api.appendsOf("TRACKS")[0].rows[0]["COUNT"])
	})

	t.Run("a failed append reopens the channel: chunks not committed by then fail, so none can land twice", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil, withMaxChunkRawBytes(1)) // one row per chunk
		api.appendErr = func(_ restapi.ChannelRef, n int) error {
			if n == 2 {
				return &restapi.APIError{StatusCode: http.StatusServiceUnavailable, Message: "gave up"}
			}
			return nil
		}

		file, jobIDs := writeBatch(t, track(1), track(2), track(3))
		out := upload(m, file, jobIDs)
		require.Empty(t, out.ImportingJobIDs, "chunk 1 wasn't committed, so the reopen discarded it")
		require.Equal(t, []int64{1, 2, 3}, out.FailedJobIDs)
		require.Empty(t, out.ImportingParameters)
		require.Equal(t, 2, api.opens)

		api.appendErr = nil
		file, jobIDs = writeBatch(t, track(1), track(2), track(3))
		out = upload(m, file, jobIDs)
		require.Equal(t, []int64{1, 2, 3}, out.ImportingJobIDs)
		require.Equal(t, 2, api.opens, "the reopened channel is reused")
		require.Zero(t, api.dropped)
		api.commit()
		require.Len(t, api.committedRows("TRACKS"), 3)
	})

	t.Run("a failed append keeps the chunks committed before it importing", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil, withMaxChunkRawBytes(1))
		api.appendErr = func(_ restapi.ChannelRef, n int) error {
			if n == 2 {
				for _, ch := range api.channels {
					api.commitN(ch, len(ch.pending)) // chunk 1 commits while chunk 2 fails
				}
				return &restapi.APIError{StatusCode: http.StatusServiceUnavailable, Message: "gave up"}
			}
			return nil
		}

		file, jobIDs := writeBatch(t, track(1), track(2), track(3))
		out := upload(m, file, jobIDs)
		require.Equal(t, []int64{1}, out.ImportingJobIDs)
		require.Equal(t, []int64{2, 3}, out.FailedJobIDs)
		importID, infos := importIDOf(t, out)
		require.Equal(t, "1", infos[0].Offset)
		require.False(t, poll(m, importID).HasFailed, "already committed")
	})

	t.Run("a failed append whose channel can't be reopened keeps the accepted chunks importing for poll", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil, withMaxChunkRawBytes(1)) // one row per chunk
		api.appendErr = func(_ restapi.ChannelRef, n int) error {
			if n == 2 {
				api.openErr = func(restapi.ChannelRef) error { // the reopen that follows fails too
					return &restapi.APIError{StatusCode: http.StatusServiceUnavailable}
				}
				return &restapi.APIError{StatusCode: http.StatusServiceUnavailable, Message: "gave up"}
			}
			return nil
		}

		file, jobIDs := writeBatch(t, track(1), track(2), track(3))
		out := upload(m, file, jobIDs)
		require.Equal(t, []int64{1}, out.ImportingJobIDs, "chunk 1 was accepted: poll confirms it")
		require.Equal(t, []int64{2, 3}, out.FailedJobIDs)
		_, infos := importIDOf(t, out)
		require.Equal(t, "1", infos[0].Offset)
		require.True(t, m.state.channels["TRACKS"].isBroken, "the channel is reopened before its next append")
	})

	t.Run("an append whose response was lost but which landed is not failed", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil)
		api.appendErr = func(ref restapi.ChannelRef, n int) error {
			ch := api.channels[api.key(ref)]
			end := "2"
			ch.offset = &end // the rows land and commit, but the client never hears back
			ch.rowSeq++
			api.appendErr = nil
			return &restapi.APIError{StatusCode: http.StatusGatewayTimeout, Message: "timeout"}
		}
		file, jobIDs := writeBatch(t, track(1), track(2))
		out := upload(m, file, jobIDs)
		require.Equal(t, []int64{1, 2}, out.ImportingJobIDs)
		require.Empty(t, out.FailedJobIDs)
	})

	t.Run("a row too large for a request is aborted alone", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		conf := config.New()
		conf.Set("SnowpipeStreaming.maxBufferCapacity", 32*1024*1024)
		m := newTestManagerV2(t, api, wh, stats.NOP, conf)
		big := track(2)
		big.data["EVENT"] = rand.String(7 * 1024 * 1024) // ~5.3 MiB compressed
		file, jobIDs := writeBatch(t, track(1), big, track(3))
		out := upload(m, file, jobIDs)
		require.Equal(t, []int64{2}, out.AbortJobIDs)
		require.Equal(t, []int64{1, 3}, out.ImportingJobIDs)
		_, infos := importIDOf(t, out)
		require.Equal(t, "3", infos[0].Offset)
	})

	t.Run("rows skipped by Snowflake are counted but don't fail the import", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		statsStore, err := memstats.New()
		require.NoError(t, err)
		m := newTestManagerV2(t, api, wh, statsStore, nil)

		file, jobIDs := writeBatch(t, track(1), track(2))
		importID, _ := importIDOf(t, upload(m, file, jobIDs))
		api.commit()
		for _, ch := range api.channels {
			ch.rowErrors = 2
		}
		require.False(t, poll(m, importID).HasFailed)
		tags := stats.Tags{"module": "batch_router", "workspaceId": "ws-1", "destType": "SNOWPIPE_STREAMING", "destinationId": "dest-1", "version": "2"}
		require.EqualValues(t, 2, statsStore.Get("snowpipe_streaming_row_errors", tags).LastValue())

		// The count is cumulative: only the delta is reported on the next poll.
		file, jobIDs = writeBatch(t, track(3))
		importID, _ = importIDOf(t, upload(m, file, jobIDs))
		api.commit()
		for _, ch := range api.channels {
			ch.rowErrors = 3
		}
		require.False(t, poll(m, importID).HasFailed)
		require.EqualValues(t, 3, statsStore.Get("snowpipe_streaming_row_errors", tags).LastValue())
	})

	t.Run("after a restart, uncommitted rows are lost: the import fails, and only the jobs above the committed offset", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		conf := config.New()
		m := newTestManagerV2(t, api, wh, stats.NOP, conf, withMaxChunkRawBytes(1)) // one row per chunk

		file, jobIDs := writeBatch(t, track(1), track(2), track(3))
		importID, _ := importIDOf(t, upload(m, file, jobIDs))
		api.commitTable("TRACKS", 1) // job 1 committed, 2 and 3 pending

		restarted := newTestManagerV2(t, api, wh, stats.NOP, conf)
		res := poll(restarted, importID)
		require.True(t, res.HasFailed)
		var infos []*importInfo
		require.NoError(t, jsonrs.Unmarshal([]byte(res.FailedJobParameters), &infos))
		require.Equal(t, &failedJobIds{Start: 2, End: 3}, infos[0].FailedJobIds)

		st := uploadStats(t, restarted, file, jobIDs, res.FailedJobParameters)
		require.Equal(t, []int64{1}, st.Metadata.SucceededKeys)
		require.ElementsMatch(t, []int64{2, 3}, st.Metadata.FailedKeys)

		// The failed channel was reopened at once, so the failed rows can never commit after being retried.
		api.commit()
		require.Len(t, api.committedRows("TRACKS"), 1)
	})

	t.Run("after a restart with nothing committed, all of the import's jobs fail", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		conf := config.New()
		m := newTestManagerV2(t, api, wh, stats.NOP, conf)
		file, jobIDs := writeBatch(t, track(1), track(2))
		importID, _ := importIDOf(t, upload(m, file, jobIDs))

		res := poll(newTestManagerV2(t, api, wh, stats.NOP, conf), importID)
		require.True(t, res.HasFailed)
		var infos []*importInfo
		require.NoError(t, jsonrs.Unmarshal([]byte(res.FailedJobParameters), &infos))
		require.Nil(t, infos[0].FailedJobIds, "never a range derived from an empty offset")
	})

	t.Run("an import committed just before its channel is reopened succeeds", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		conf := config.New()
		m := newTestManagerV2(t, api, wh, stats.NOP, conf)
		file, jobIDs := writeBatch(t, track(1), track(2))
		importID, _ := importIDOf(t, upload(m, file, jobIDs))

		restarted := newTestManagerV2(t, api, wh, stats.NOP, conf)
		// The status says nothing is committed, then the commit lands before the reopen.
		api.statusErr = nil
		var once sync.Once
		api.openErr = func(restapi.ChannelRef) error {
			once.Do(func() {
				for _, ch := range api.channels {
					api.commitN(ch, len(ch.pending))
				}
			})
			return nil
		}
		res := poll(restarted, importID)
		require.False(t, res.HasFailed)
	})

	t.Run("a missing channel fails the import", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil)
		file, jobIDs := writeBatch(t, track(1))
		importID, _ := importIDOf(t, upload(m, file, jobIDs))
		clear(api.channels)
		require.True(t, poll(m, importID).HasFailed)
	})

	t.Run("status errors keep polling until the stuck threshold", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		now := time.Now()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil, withClock(func() time.Time { return now }))
		file, jobIDs := writeBatch(t, track(1))
		importID, _ := importIDOf(t, upload(m, file, jobIDs))

		api.statusErr = errors.New("connection reset")
		require.True(t, poll(m, importID).InProgress)
		require.True(t, poll(m, importID).InProgress)
		now = now.Add(16 * time.Minute)
		res := poll(m, importID)
		require.False(t, res.InProgress)
		require.True(t, res.HasFailed)
	})

	t.Run("a missing table on channel open is recreated", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil)
		file, jobIDs := writeBatch(t, track(1))
		upload(m, file, jobIDs)

		// The table is dropped externally while its schema is cached and its channel broken.
		delete(wh.tables, "TRACKS")
		m.state.channels["TRACKS"].isBroken = true
		var dropped bool
		api.openErr = func(restapi.ChannelRef) error {
			if !dropped {
				dropped = true
				return &restapi.APIError{StatusCode: http.StatusNotFound, Code: restapi.CodeTableDoesNotExist}
			}
			return nil
		}
		file, jobIDs = writeBatch(t, track(2))
		out := upload(m, file, jobIDs)
		require.Equal(t, []int64{2}, out.ImportingJobIDs)
		require.Contains(t, wh.tables, "TRACKS")
	})

	t.Run("a manager rebuilt mid-import keeps polling the import instead of failing it", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		conf := config.New()
		m := newTestManagerV2(t, api, wh, stats.NOP, conf)
		file, jobIDs := writeBatch(t, track(1), track(2))
		importID, _ := importIDOf(t, upload(m, file, jobIDs))

		// batchrouter rebuilds the manager when the destination's config changes; the process keeps running.
		rebuilt := newTestManagerV2(t, api, wh, stats.NOP, conf, withState(m.state))
		require.True(t, poll(rebuilt, importID).InProgress)
		require.Equal(t, 1, api.opens, "nothing reopened")
		api.commit()
		require.False(t, poll(rebuilt, importID).HasFailed)
		require.Len(t, api.committedRows("TRACKS"), 2)
	})

	t.Run("cached schemas expire, so a column dropped outside RudderStack is added back", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		now := time.Now()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil, withClock(func() time.Time { return now }))
		file, jobIDs := writeBatch(t, track(1))
		upload(m, file, jobIDs)

		delete(wh.tables["TRACKS"], "EVENT") // dropped by the customer: HP would silently drop the field
		file, jobIDs = writeBatch(t, track(2))
		upload(m, file, jobIDs)
		require.Empty(t, wh.addedColumns, "within the TTL the cache is trusted")

		now = now.Add(31 * time.Minute)
		file, jobIDs = writeBatch(t, track(3))
		out := upload(m, file, jobIDs)
		require.Equal(t, []int64{3}, out.ImportingJobIDs)
		require.Equal(t, []string{"EVENT"}, wh.addedColumns)
	})

	t.Run("columns added by another pod at the same time don't fail the batch", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		wh.tables["TRACKS"] = maps.Clone(trackColumns)
		delete(wh.tables["TRACKS"], "TAGS")
		wh.addColumnsErr = errors.New("column already exists")
		session := &racingWarehouse{fakeWarehouse: wh, onAdd: func() { wh.tables["TRACKS"]["TAGS"] = "string" }}
		m := newTestManagerV2(t, api, wh, stats.NOP, nil, withWarehouseSession(func(*destConfig) warehouseSession { return session }))

		file, jobIDs := writeBatch(t, track(1))
		out := upload(m, file, jobIDs)
		require.Equal(t, []int64{1}, out.ImportingJobIDs, out.AbortReason)
	})

	t.Run("a column of a type we can't map is kept, and its values go to discards", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil)
		wh.tables["TRACKS"] = maps.Clone(trackColumns)
		wh.tables["TRACKS"]["LOCATION"] = unsupportedDataTypePrefix + `{"type":"GEOGRAPHY"}`
		e := track(1)
		e.columns = maps.Clone(trackColumns)
		e.columns["LOCATION"] = "string"
		e.data["LOCATION"] = "POINT(1 2)"
		file, jobIDs := writeBatch(t, e)
		out := upload(m, file, jobIDs)
		require.Equal(t, []int64{1}, out.ImportingJobIDs)
		require.Empty(t, wh.addedColumns, "an existing column is never added again")
		require.Equal(t, "LOCATION", api.appendsOf("RUDDER_DISCARDS")[0].rows[0]["COLUMN_NAME"])
	})

	t.Run("offsets on the shared discards channel only grow, even for a retried batch", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil)
		wh.tables["TRACKS"] = maps.Clone(trackColumns)
		wh.tables["TRACKS"]["COUNT"] = "int"
		bad := func(jobID int64) testEvent {
			e := track(jobID)
			e.columns = maps.Clone(trackColumns)
			e.columns["COUNT"] = "string"
			e.data["COUNT"] = "many"
			return e
		}
		file, jobIDs := writeBatch(t, bad(100))
		upload(m, file, jobIDs)
		file, jobIDs = writeBatch(t, bad(50)) // e.g. a retry of jobs that failed earlier
		_, infos := importIDOf(t, upload(m, file, jobIDs))
		require.Equal(t, "RUDDER_DISCARDS", infos[1].Table)
		require.Equal(t, "101", infos[1].Offset)
		require.Equal(t, "50", infos[0].Offset, "table channels keep job IDs as offsets")
	})

	t.Run("credentials or role refused on channel open abort the batch, as on the classic path", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil)
		api.openErr = func(restapi.ChannelRef) error {
			return &restapi.APIError{StatusCode: http.StatusForbidden, Code: "ERR_ROLE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED"}
		}
		file, jobIDs := writeBatch(t, track(1))
		out := upload(m, file, jobIDs)
		require.Equal(t, jobIDs, out.AbortJobIDs)
	})

	t.Run("transient errors on channel open fail the batch for a retry", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil)
		api.openErr = func(restapi.ChannelRef) error {
			return &restapi.APIError{StatusCode: http.StatusServiceUnavailable}
		}
		file, jobIDs := writeBatch(t, track(1))
		out := upload(m, file, jobIDs)
		require.Equal(t, jobIDs, out.FailedJobIDs)
	})

	t.Run("a table dropped or replaced mid-import fails the import at once instead of waiting for the stuck threshold", func(t *testing.T) {
		api, wh := newFakeHP(), newFakeWarehouse()
		m := newTestManagerV2(t, api, wh, stats.NOP, nil)
		file, jobIDs := writeBatch(t, track(1))
		importID, _ := importIDOf(t, upload(m, file, jobIDs))

		api.statusErr = &restapi.APIError{StatusCode: http.StatusNotFound, Code: restapi.CodePipeDoesNotExist}
		res := poll(m, importID)
		require.False(t, res.InProgress)
		require.True(t, res.HasFailed)
	})

	t.Run("an invalid private key aborts the batch", func(t *testing.T) {
		m := newTestManagerV2(t, nil, newFakeWarehouse(), stats.NOP, nil) // the real REST client, given an invalid key
		file, jobIDs := writeBatch(t, track(1))
		out := upload(m, file, jobIDs)
		require.Equal(t, jobIDs, out.AbortJobIDs)
		require.Contains(t, out.AbortReason, "creating snowflake client")
	})
}

func TestResolveImport(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		committed, appended, expected string
		state                         importState
		failed                        *failedJobIds
	}{
		{name: "fresh channel, rows pending", committed: "", appended: "10", expected: "10", state: importInProgress},
		{name: "empty offset, rows not pending", committed: "", appended: "", expected: "10", state: importFailed},
		{name: "empty offset, older rows pending", committed: "", appended: "5", expected: "10", state: importFailed},
		{name: "committed equals expected", committed: "10", appended: "", expected: "10", state: importCommitted},
		{name: "committed beyond expected", committed: "12", appended: "12", expected: "10", state: importCommitted},
		{name: "rows lost", committed: "4", appended: "", expected: "10", state: importFailed, failed: &failedJobIds{Start: 5, End: 10}},
		{name: "flushing", committed: "4", appended: "10", expected: "10", state: importInProgress},
		{name: "negative job ids", committed: "-5", appended: "", expected: "-2", state: importFailed, failed: &failedJobIds{Start: -4, End: -2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, failed, _ := resolveImport(tc.committed, tc.appended, tc.expected)
			require.Equal(t, tc.state, state)
			require.Equal(t, tc.failed, failed)
		})
	}
	_, _, err := resolveImport("", "", "not a number")
	require.Error(t, err)
}

func TestNormaliseShowColumnsType(t *testing.T) {
	for in, want := range map[string]string{
		`{"type":"FIXED","precision":38,"scale":0,"nullable":true}`: "int",
		`{"type":"FIXED","precision":38,"scale":2,"nullable":true}`: "float",
		`{"type":"TEXT","length":16777216,"nullable":true}`:         "string",
		`{"type":"REAL","nullable":true}`:                           "float",
		`{"type":"BOOLEAN","nullable":true}`:                        "boolean",
		`{"type":"VARIANT","nullable":true}`:                        "json",
		`{"type":"OBJECT","nullable":true}`:                         "json",
		`{"type":"ARRAY","nullable":true}`:                          "json",
		`{"type":"TIMESTAMP_TZ","precision":0,"scale":9}`:           "datetime",
		`{"type":"TIMESTAMP_NTZ","precision":0,"scale":9}`:          "datetime",
		`{"type":"TIMESTAMP_LTZ","precision":0,"scale":9}`:          "datetime",
		`{"type":"DATE","nullable":true}`:                           "datetime",
	} {
		got, ok := normaliseShowColumnsType(in)
		require.True(t, ok, in)
		require.Equal(t, want, got, in)
	}
	_, ok := normaliseShowColumnsType(`{"type":"GEOGRAPHY"}`)
	require.False(t, ok)
	_, ok = normaliseShowColumnsType(`not json`)
	require.False(t, ok)
}

func TestPrepareValuesHP(t *testing.T) {
	schema := whutils.ModelTableSchema{"J": "json", "S": "string", "N": "int"}
	var e event
	e.Message.Data = map[string]any{
		"J":  `{"a":[1,2]}`,
		"S":  []any{"x"},
		"N":  1,
		"J2": "plain",
	}
	schema["J2"] = "json"
	prepareValuesHP(&e, schema)
	require.Equal(t, map[string]any{"a": []any{float64(1), float64(2)}}, e.Message.Data["J"], "JSON strings in json columns are parsed")
	require.Equal(t, `["x"]`, e.Message.Data["S"])
	require.Equal(t, 1, e.Message.Data["N"])
	require.Equal(t, "plain", e.Message.Data["J2"], "scalar strings stay strings")

	// The classic path is unchanged: slices are still marshalled by discard detection.
	var classic event
	classic.Message.Metadata.Columns = map[string]string{"J": "json"}
	classic.Message.Data = map[string]any{"J": []any{1}}
	getDiscardedRecordsFromEvent(logger.NOP, &classic, whutils.ModelTableSchema{"J": "json"}, "T", "")
	require.Equal(t, "[1]", classic.Message.Data["J"])
}

type recordingManager struct {
	name  string
	calls *[]string
}

func (r recordingManager) Transform(*jobsdb.JobT) (string, error) { return "", nil }
func (r recordingManager) Upload(context.Context, *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	*r.calls = append(*r.calls, r.name+".Upload")
	return common.AsyncUploadOutput{}
}

func (r recordingManager) Poll(context.Context, common.AsyncPoll) common.PollStatusResponse {
	*r.calls = append(*r.calls, r.name+".Poll")
	return common.PollStatusResponse{}
}

func (r recordingManager) GetUploadStats(common.GetUploadStatsInput) common.GetUploadStatsResponse {
	*r.calls = append(*r.calls, r.name+".GetUploadStats")
	return common.GetUploadStatsResponse{}
}

func TestDispatcher(t *testing.T) {
	var calls []string
	conf := config.New()
	d, ok := NewManager(conf, logger.NOP, stats.NOP, testHPDestination).(*Dispatcher)
	require.True(t, ok)
	d.v1 = recordingManager{name: "v1", calls: &calls}
	d.v2 = recordingManager{name: "v2", calls: &calls}

	ctx := context.Background()
	d.Upload(ctx, &common.AsyncDestinationStruct{})
	conf.Set("SnowpipeStreaming.dest-1.highPerformance.enabled", true)
	d.Upload(ctx, &common.AsyncDestinationStruct{})
	conf.Set("SnowpipeStreaming.dest-1.highPerformance.enabled", false)
	conf.Set("SnowpipeStreaming.highPerformance.enabled", true)
	d.Upload(ctx, &common.AsyncDestinationStruct{})
	require.Equal(t, []string{"v1.Upload", "v2.Upload", "v1.Upload"}, calls, "the destination setting wins over the global one")

	// Polls follow the imports' version, not the flag.
	calls = nil
	d.Poll(ctx, common.AsyncPoll{ImportId: `[{"channelId":"c","offset":"1","table":"T"}]`})
	d.Poll(ctx, common.AsyncPoll{ImportId: `[{"channelId":"c","offset":"1","table":"T","version":2}]`})
	d.GetUploadStats(common.GetUploadStatsInput{FailedJobParameters: `[{"table":"T","version":2}]`})
	d.GetUploadStats(common.GetUploadStatsInput{FailedJobParameters: `[{"table":"T"}]`})
	require.Equal(t, []string{"v1.Poll", "v2.Poll", "v2.GetUploadStats", "v1.GetUploadStats"}, calls)

	calls = nil
	res := d.Poll(ctx, common.AsyncPoll{ImportId: `[{"table":"T"},{"table":"U","version":2}]`})
	require.Empty(t, calls)
	require.True(t, res.HasFailed)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
}

// racingWarehouse simulates another pod adding the same columns: AddColumns fails, but the columns exist afterwards.
type racingWarehouse struct {
	*fakeWarehouse
	onAdd func()
}

func (w *racingWarehouse) AddColumns(context.Context, string, []whutils.ColumnInfo) error {
	w.onAdd()
	return w.addColumnsErr
}

func TestIsObjectNotFound(t *testing.T) {
	notFound := &gosnowflake.SnowflakeError{Number: 2003, Message: "Table does not exist or not authorized."}
	require.True(t, isObjectNotFound(fmt.Errorf("showing columns: %w", notFound)))
	require.False(t, isObjectNotFound(&gosnowflake.SnowflakeError{Number: gosnowflake.ErrObjectNotExistOrAuthorized}), "390201 is an authentication error")
	require.False(t, isObjectNotFound(errors.New("002003 does not exist or not authorized")), "only the typed error counts")
}
