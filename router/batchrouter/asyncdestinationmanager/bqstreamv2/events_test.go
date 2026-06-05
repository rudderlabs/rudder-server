package bqstreamv2

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/utils/misc"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestEventsFromFile(t *testing.T) {
	now := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	m := &Manager{now: func() time.Time { return now }}
	m.config.maxBufferCapacity = config.SingleValueLoader(int64(512 * 1024))

	line1 := `{"message":{"metadata":{"table":"pages","columns":{"name":"string","` + uuidTSColumnName + `":"datetime"}},"data":{"name":"bob"}},"metadata":{"job_id":1}}`
	line2 := `{"message":{"metadata":{"table":"tracks","columns":{"event":"string"}},"data":{"event":"click"}},"metadata":{"job_id":2}}`

	fileName := filepath.Join(t.TempDir(), "events.txt")
	require.NoError(t, os.WriteFile(fileName, []byte(line1+"\n"+line2+"\n"), 0o600))

	events, err := m.eventsFromFile(fileName, 2)
	require.NoError(t, err)
	require.Len(t, events, 2)

	formattedTS := now.Format(misc.RFC3339Milli)
	require.Equal(t, "pages", events[0].Message.Metadata.Table)
	require.EqualValues(t, 1, events[0].Metadata.JobID)
	require.Equal(t, formattedTS, events[0].Message.Data[uuidTSColumnName])
	require.Equal(t, int64(len(line1)+len(formattedTS)), events[0].MessageDataByteSize)

	require.Equal(t, "tracks", events[1].Message.Metadata.Table)
	require.EqualValues(t, 2, events[1].Metadata.JobID)
	require.NotContains(t, events[1].Message.Data, uuidTSColumnName)
	require.Equal(t, int64(len(line2)), events[1].MessageDataByteSize)

	t.Run("missing file", func(t *testing.T) {
		_, err := m.eventsFromFile(filepath.Join(t.TempDir(), "missing.txt"), 0)
		require.Error(t, err)
	})

	t.Run("invalid json line", func(t *testing.T) {
		fileName := filepath.Join(t.TempDir(), "bad.txt")
		require.NoError(t, os.WriteFile(fileName, []byte("not-json\n"), 0o600))
		_, err := m.eventsFromFile(fileName, 1)
		require.Error(t, err)
	})
}

func TestSetTimestamps(t *testing.T) {
	m := &Manager{}

	e := &event{}
	require.False(t, m.setUUIDTimestamp(e, "ts"))
	require.False(t, m.setLoadedAtTimestamp(e, "ts"))

	e.Message.Metadata.Columns = map[string]string{uuidTSColumnName: "datetime", loadedAtColumnName: "datetime"}
	e.Message.Data = map[string]any{}
	require.True(t, m.setUUIDTimestamp(e, "ts1"))
	require.True(t, m.setLoadedAtTimestamp(e, "ts2"))
	require.Equal(t, "ts1", e.Message.Data[uuidTSColumnName])
	require.Equal(t, "ts2", e.Message.Data[loadedAtColumnName])
}

func testEvent(table string, jobID, size int64, columns map[string]string) *event {
	e := &event{}
	e.Message.Metadata.Table = table
	e.Message.Metadata.Columns = columns
	e.Metadata.JobID = jobID
	e.MessageDataByteSize = size
	return e
}

func TestGroupAndChunkEvents(t *testing.T) {
	m := &Manager{}
	m.config.maxChunkBytes = config.SingleValueLoader(int64(100))

	events := []*event{
		testEvent("pages", 1, 60, map[string]string{"a": "string"}),
		testEvent("pages", 2, 60, map[string]string{"b": "int"}),
		testEvent("tracks", 3, 10, nil),
	}

	grouped := m.groupAndChunkEvents(events)
	require.Len(t, grouped, 2)

	pages := grouped["pages"]
	require.Len(t, pages, 2) // 60+60 exceeds the 100-byte chunk limit
	require.Equal(t, whutils.ToProviderCase(whutils.BQ, "pages"), pages[0].tableName)
	require.Equal(t, []int64{1}, pages[0].jobIDs)
	require.Equal(t, []int64{2}, pages[1].jobIDs)
	// chunks of the same table share the merged events schema
	require.Equal(t, whutils.ModelTableSchema{"a": "string", "b": "int"}, pages[0].eventsSchema)
	require.Equal(t, pages[0].eventsSchema, pages[1].eventsSchema)

	tracks := grouped["tracks"]
	require.Len(t, tracks, 1)
	require.Equal(t, []int64{3}, tracks[0].jobIDs)
}

func TestSchemaFromEvents(t *testing.T) {
	events := []*event{
		testEvent("pages", 1, 0, map[string]string{"a": "string"}),
		testEvent("pages", 2, 0, map[string]string{"a": "int", "b": "float"}),
	}
	// first seen type wins
	require.Equal(t, whutils.ModelTableSchema{"a": "string", "b": "float"}, schemaFromEvents(events))
}

func TestShouldFetchSchema(t *testing.T) {
	m := &Manager{now: time.Now, schemaCache: NewTableSchemaCache(time.Minute)}

	grouped := map[string][]tableEvents{"pages": {{tableName: "pages"}}}
	require.True(t, m.shouldFetchSchema(grouped)) // empty cache

	m.schemaCache.Set("pages", whutils.ModelTableSchema{"a": "string"}, time.Now())
	require.False(t, m.shouldFetchSchema(grouped))

	grouped["tracks"] = []tableEvents{{tableName: "tracks"}}
	require.True(t, m.shouldFetchSchema(grouped)) // one table missing from the cache
}

func TestJobIDsFromTableEvents(t *testing.T) {
	require.Equal(t, []int64{1, 2, 3}, jobIDsFromTableEvents([]tableEvents{
		{jobIDs: []int64{1, 2}},
		{jobIDs: []int64{3}},
	}))
}

func TestCheckForDuplicateIDsInEvents(t *testing.T) {
	withID := func(id any) *event {
		e := &event{}
		e.Message.Data = map[string]any{idColumnName: id}
		return e
	}
	events := []*event{withID("x"), withID("y"), withID("x"), withID("x"), {}}
	require.Equal(t, 2, checkForDuplicateIDsInEvents(events))
	require.Equal(t, 0, checkForDuplicateIDsInEvents([]*event{withID("x"), withID("y")}))
}

func TestGetDiscardedRecordsFromEvent(t *testing.T) {
	e := &event{}
	e.Message.Metadata.Columns = map[string]string{"age": "string", "title": "int", "tags": "json"}
	e.Message.Data = map[string]any{
		"age":                "abc", // string -> int conversion fails, discarded
		"title":              42,    // int -> string conversion succeeds
		"tags":               []any{"a", "b"},
		idColumnName:         "row-1",
		receivedAtColumnName: "2025-01-02T03:04:05.000Z",
	}
	warehouseSchema := whutils.ModelTableSchema{
		"age":                "int",
		"title":              "string",
		"tags":               "json",
		idColumnName:         "string",
		receivedAtColumnName: "datetime",
	}

	discards := getDiscardedRecordsFromEvent(logger.NOP, e, warehouseSchema, "pages", "ts-now")
	require.Len(t, discards, 1)

	d := discards[0]
	require.Equal(t, "pages", d.tableName)
	require.Equal(t, "age", d.columnName)
	require.Equal(t, "abc", d.columnValue)
	require.NotEmpty(t, d.reason)
	require.Equal(t, "ts-now", d.uuidTS)
	require.Equal(t, "row-1", d.rowID)
	require.Equal(t, "2025-01-02T03:04:05.000Z", d.receivedAt)

	require.Nil(t, e.Message.Data["age"])                 // discarded value is cleared
	require.Equal(t, "42", e.Message.Data["title"])       // converted in place
	require.Equal(t, `["a","b"]`, e.Message.Data["tags"]) // []any marshalled to a JSON string
}

func TestConvertDiscardedEventsToRows(t *testing.T) {
	rows := convertDiscardedEventsToRows([]discardEvent{{
		tableName:   "pages",
		columnName:  "age",
		columnValue: 12,
		reason:      "incompatible schema conversion",
		uuidTS:      "ts-now",
		rowID:       7,
		receivedAt:  "2025-01-02T03:04:05.000Z",
	}})
	require.Equal(t, []Row{{
		"column_name":  "age",
		"column_value": "12",
		"reason":       "incompatible schema conversion",
		"received_at":  "2025-01-02T03:04:05.000Z",
		"row_id":       "7",
		"table_name":   "pages",
		"uuid_ts":      "ts-now",
	}}, rows)
}
