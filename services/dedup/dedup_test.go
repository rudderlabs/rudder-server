package dedup_test

import (
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/services/dedup"
)

func Test_Dedup(t *testing.T) {
	config.Reset()
	logger.Reset()

	dbPath := os.TempDir() + "/dedup_test"
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)

	d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Hour))
	defer d.Close()

	t.Run("not marked as processed", func(t *testing.T) {
		testCases := []struct {
			name             string
			infos            []dedup.Info
			allMessageIDsSet map[string]dedup.Payload
			expected         map[int]dedup.Payload
		}{
			{
				name: "sample",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "c", Size: 3},
				},
				allMessageIDsSet: nil,
				expected:         map[int]dedup.Payload{},
			},
			{
				name: "sample with different allMessageIDsSet",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "c", Size: 3},
				},
				allMessageIDsSet: map[string]dedup.Payload{
					"d": {Size: 4},
					"e": {Size: 5},
				},
				expected: map[int]dedup.Payload{},
			},
			{
				name: "sample with extra info",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "c", Size: 3},
					{MessageID: "d", Size: 4},
					{MessageID: "e", Size: 5},
				},
				allMessageIDsSet: nil,
				expected:         map[int]dedup.Payload{},
			},
			{
				name: "duplicate with same size",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "a", Size: 1},
					{MessageID: "c", Size: 3},
					{MessageID: "a", Size: 1},
				},
				allMessageIDsSet: nil,
				expected: map[int]dedup.Payload{
					2: {Size: 0},
					4: {Size: 0},
				},
			},
			{
				name: "duplicate with different size",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "a", Size: 2},
					{MessageID: "c", Size: 3},
					{MessageID: "a", Size: 3},
				},
				allMessageIDsSet: nil,
				expected: map[int]dedup.Payload{
					2: {Size: 0},
					4: {Size: 0},
				},
			},
			{
				name: "duplicate with different size and some in allMessageIDsSet",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "a", Size: 2},
					{MessageID: "c", Size: 3},
					{MessageID: "a", Size: 3},
				},
				allMessageIDsSet: map[string]dedup.Payload{
					"b": {Size: 2},
					"c": {Size: 3},
				},
				expected: map[int]dedup.Payload{
					1: {Size: 0},
					2: {Size: 0},
					3: {Size: 0},
					4: {Size: 0},
				},
			},
			{
				name: "duplicate with different size and all in allMessageIDsSet",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "a", Size: 2},
					{MessageID: "c", Size: 3},
					{MessageID: "a", Size: 3},
				},
				allMessageIDsSet: map[string]dedup.Payload{
					"a": {Size: 1},
					"b": {Size: 2},
					"c": {Size: 3},
				},
				expected: map[int]dedup.Payload{
					0: {Size: 0},
					1: {Size: 0},
					2: {Size: 0},
					3: {Size: 0},
					4: {Size: 0},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				dups := d.FindDuplicates(tc.infos, tc.allMessageIDsSet)
				require.Equal(t, tc.expected, dups)
			})
		}
	})

	t.Run("marked as processed", func(t *testing.T) {
		err := d.MarkProcessed(
			[]dedup.Info{
				{MessageID: "a", Size: 1},
				{MessageID: "b", Size: 2},
				{MessageID: "c", Size: 3},
			},
		)
		require.NoError(t, err)

		testCases := []struct {
			name             string
			infos            []dedup.Info
			allMessageIDsSet map[string]dedup.Payload
			expected         map[int]dedup.Payload
		}{
			{
				name: "sample",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "c", Size: 3},
				},
				allMessageIDsSet: nil,
				expected: map[int]dedup.Payload{
					0: {Size: 1},
					1: {Size: 2},
					2: {Size: 3},
				},
			},
			{
				name: "sample with different allMessageIDsSet",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "c", Size: 3},
				},
				allMessageIDsSet: map[string]dedup.Payload{
					"d": {Size: 4},
					"e": {Size: 5},
				},
				expected: map[int]dedup.Payload{
					0: {Size: 1},
					1: {Size: 2},
					2: {Size: 3},
				},
			},
			{
				name: "sample with extra info",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "c", Size: 3},
					{MessageID: "d", Size: 4},
					{MessageID: "e", Size: 5},
				},
				allMessageIDsSet: nil,
				expected: map[int]dedup.Payload{
					0: {Size: 1},
					1: {Size: 2},
					2: {Size: 3},
				},
			},
			{
				name: "duplicate with same size",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "a", Size: 1},
					{MessageID: "c", Size: 3},
					{MessageID: "a", Size: 1},
				},
				allMessageIDsSet: nil,
				expected: map[int]dedup.Payload{
					0: {Size: 1},
					1: {Size: 2},
					2: {Size: 1},
					3: {Size: 3},
					4: {Size: 1},
				},
			},
			{
				name: "duplicate with different size",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "a", Size: 2},
					{MessageID: "c", Size: 3},
					{MessageID: "a", Size: 3},
				},
				allMessageIDsSet: nil,
				expected: map[int]dedup.Payload{
					0: {Size: 1},
					1: {Size: 2},
					2: {Size: 1},
					3: {Size: 3},
					4: {Size: 1},
				},
			},
			{
				name: "duplicate with different size and some in allMessageIDsSet",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "a", Size: 2},
					{MessageID: "c", Size: 3},
					{MessageID: "a", Size: 3},
				},
				allMessageIDsSet: map[string]dedup.Payload{
					"b": {Size: 2},
					"c": {Size: 3},
				},
				expected: map[int]dedup.Payload{
					0: {Size: 1},
					1: {Size: 2},
					2: {Size: 1},
					3: {Size: 3},
					4: {Size: 1},
				},
			},
			{
				name: "duplicate with different size and all in allMessageIDsSet",
				infos: []dedup.Info{
					{MessageID: "a", Size: 1},
					{MessageID: "b", Size: 2},
					{MessageID: "a", Size: 2},
					{MessageID: "c", Size: 3},
					{MessageID: "a", Size: 3},
				},
				allMessageIDsSet: map[string]dedup.Payload{
					"a": {Size: 1},
					"b": {Size: 2},
					"c": {Size: 3},
				},
				expected: map[int]dedup.Payload{
					0: {Size: 1},
					1: {Size: 2},
					2: {Size: 1},
					3: {Size: 3},
					4: {Size: 1},
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				dups := d.FindDuplicates(tc.infos, tc.allMessageIDsSet)
				require.Equal(t, tc.expected, dups)
			})
		}
	})
}

func Test_Dedup_Window(t *testing.T) {
	config.Reset()
	logger.Reset()

	dbPath := os.TempDir() + "/dedup_test"
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)

	d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Second))
	defer d.Close()

	infos := []dedup.Info{
		{MessageID: "to be deleted", Size: 1},
	}

	err := d.MarkProcessed(infos)
	require.NoError(t, err)

	dups := d.FindDuplicates(infos, nil)
	require.Equal(t, map[int]dedup.Payload{0: {Size: 1}}, dups)

	require.Eventually(t, func() bool {
		return len(
			d.FindDuplicates(infos, nil),
		) == 0
	}, 2*time.Second, 100*time.Millisecond)

	dupsAfter := d.FindDuplicates(infos, nil)
	require.Equal(t, map[int]dedup.Payload{}, dupsAfter)
}

func Test_Dedup_ClearDB(t *testing.T) {
	config.Reset()
	logger.Reset()

	dbPath := os.TempDir() + "/dedup_test"
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)

	info := []dedup.Info{
		{MessageID: "a", Size: 1},
	}

	t.Run("with clearDB and time window options", func(t *testing.T) {
		d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Hour))
		err := d.MarkProcessed(info)
		require.NoError(t, err)
		d.Close()
	})
	t.Run("no options", func(t *testing.T) {
		d := dedup.New(dbPath)
		dups := d.FindDuplicates(info, nil)
		require.Equal(t, map[int]dedup.Payload{0: {Size: 1}}, dups)
		d.Close()
	})
	t.Run("with clearDB options", func(t *testing.T) {
		d := dedup.New(dbPath, dedup.WithClearDB())
		dups := d.FindDuplicates(info, nil)
		require.Equal(t, map[int]dedup.Payload{}, dups)
		d.Close()
	})
}

func Test_Dedup_ErrTxnTooBig(t *testing.T) {
	config.Reset()
	logger.Reset()

	dbPath := os.TempDir() + "/dedup_test_errtxntoobig"
	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.RemoveAll(dbPath)

	d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Hour))
	defer d.Close()

	size := 105_000
	messageIDs := make([]dedup.Info, size)
	for i := 0; i < size; i++ {
		messageIDs[i] = dedup.Info{
			MessageID: uuid.New().String(),
			Size:      1,
		}
	}
	err := d.MarkProcessed(messageIDs)
	require.NoError(t, err)
}

var duplicateIndexes map[int]dedup.Payload

func Benchmark_Dedup(b *testing.B) {
	config.Reset()
	logger.Reset()

	dbPath := path.Join("./testdata", "tmp", rand.String(10), "/DB_Benchmark_Dedup")
	b.Logf("using path %s, since tmpDir has issues in macOS\n", dbPath)

	defer func() { _ = os.RemoveAll(dbPath) }()
	_ = os.MkdirAll(dbPath, 0o750)

	d := dedup.New(dbPath, dedup.WithClearDB(), dedup.WithWindow(time.Minute))

	b.Run("no duplicates 1000 batch unique", func(b *testing.B) {
		batchSize := 1000

		msgIDs := make([]dedup.Info, batchSize)

		for i := 0; i < b.N; i++ {
			msgIDs[i%batchSize] = dedup.Info{
				MessageID: uuid.New().String(),
				Size:      1,
			}

			if i%batchSize == batchSize-1 || i == b.N-1 {
				duplicateIndexes = d.FindDuplicates(msgIDs[:i%batchSize], nil)
				err := d.MarkProcessed(msgIDs[:i%batchSize])
				require.NoError(b, err)
			}
		}
		b.ReportMetric(float64(b.N), "events")
		b.ReportMetric(float64(b.N*len(uuid.New().String())), "bytes")
	})

	d.Close()

	cmd := exec.Command("du", "-sh", dbPath)
	out, err := cmd.Output()
	if err != nil {
		b.Log(err)
	}

	b.Log("db size:", string(out))
}
