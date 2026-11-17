package snowpipestreaming

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

func TestChunkChannelIDsForBulkStatus(t *testing.T) {
	short := []string{"a", "b", "a", "c", "d"}
	for _, tc := range []struct {
		name       string
		ids        []string
		count      int
		soft, hard int
		want       [][]string
	}{
		{name: "empty", ids: nil, count: 100, soft: 3072, hard: 4000},
		{name: "count boundary and repeats", ids: short, count: 2, soft: 100, hard: 100, want: [][]string{{"a", "b"}, {"a", "c"}, {"d"}}},
		{name: "exact byte boundary", ids: short[:3], count: 100, soft: 25, hard: 100, want: [][]string{{"a", "b"}, {"a"}}},
		{name: "escaped and multibyte", ids: []string{"a b", "é", "&", "ok"}, count: 100, soft: 32, hard: 100, want: [][]string{{"a b", "é"}, {"&", "ok"}}},
		{name: "single ID exceeds soft budget", ids: []string{"abcdef", "z"}, count: 100, soft: 10, hard: 20, want: [][]string{{"abcdef"}, {"z"}}},
		{name: "single ID uses hard budget", ids: []string{"abcdefghij"}, count: 100, soft: 1, hard: 21, want: [][]string{{"abcdefghij"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chunks, err := chunkChannelIDsForBulkStatus(tc.ids, tc.count, tc.soft, tc.hard)
			require.NoError(t, err)
			require.Equal(t, tc.want, chunks)
		})
	}

	_, err := chunkChannelIDsForBulkStatus([]string{"too-long"}, 100, 1, 18)
	require.ErrorContains(t, err, "only 18 fit")
}

func bulkStatusTestIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("00000000-0000-0000-0000-%012d", i)
	}
	return ids
}

func TestBulkStatusDefaultBudget(t *testing.T) {
	ids := bulkStatusTestIDs(537)
	chunks, err := chunkChannelIDsForBulkStatus(ids, 100, 3072, 4000)
	require.NoError(t, err)
	require.Len(t, chunks[0], 64)
	var flattened []string
	for _, chunk := range chunks {
		require.LessOrEqual(t, len(chunk), 100)
		values := url.Values{"channelIds": chunk}
		require.LessOrEqual(t, len(values.Encode()), 3072)
		flattened = append(flattened, chunk...)
	}
	require.Equal(t, ids, flattened)
}

func TestBulkStatusPollRequestLineAndReload(t *testing.T) {
	var received []string
	var querySizes []int
	var chunkSizes []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		line := r.Method + " " + r.URL.RequestURI() + " HTTP/1.1\r\n"
		if len(line) > 4096 || len(r.URL.Query()["channelIds"]) > 100 {
			w.WriteHeader(http.StatusRequestURITooLong)
			return
		}
		received = append(received, r.URL.Query()["channelIds"]...)
		querySizes = append(querySizes, len(r.URL.RawQuery))
		chunkSizes = append(chunkSizes, len(r.URL.Query()["channelIds"]))
		_, _ = w.Write([]byte(`{"success":true,"statuses":{}}`))
	}))
	defer server.Close()

	conf := config.New()
	conf.Set("SnowpipeStreaming.Client.URL", server.URL+"/prefix%20with%20spaces")
	conf.Set("SnowpipeStreaming.bulkStatusBatchSize", 200)
	destination := &backendconfig.DestinationT{DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "SNOWPIPE_STREAMING"}}
	m := New(conf, logger.NOP, stats.NOP, destination)
	ids := bulkStatusTestIDs(210)
	_, _, err := m.getBulkStatus(context.Background(), ids)
	require.NoError(t, err)
	require.Equal(t, ids, received)
	for _, size := range querySizes {
		require.LessOrEqual(t, size, 3072)
	}
	// Reloading the soft budget affects the next poll without recreating the manager.
	received = nil
	querySizes = nil
	conf.Set("SnowpipeStreaming.bulkStatusMaxQueryBytes", 60)
	_, _, err = m.getBulkStatus(context.Background(), ids[:3])
	require.NoError(t, err)
	require.Equal(t, ids[:3], received)
	require.Len(t, querySizes, 3)
	for _, size := range querySizes {
		require.LessOrEqual(t, size, 60)
	}

	// Even with short IDs and a large budget, the server's 100-ID cap wins.
	received, chunkSizes = nil, nil
	conf.Set("SnowpipeStreaming.bulkStatusMaxQueryBytes", 4000)
	shortIDs := make([]string, 205)
	for i := range shortIDs {
		shortIDs[i] = fmt.Sprint(i)
	}
	_, _, err = m.getBulkStatus(context.Background(), shortIDs)
	require.NoError(t, err)
	require.Equal(t, shortIDs, received)
	require.Equal(t, []int{100, 100, 5}, chunkSizes)

	// A long prefix reduces the effective budget below the reloadable setting.
	prefix := strings.Repeat("p", 3950)
	m.config.client.url = server.URL + "/" + prefix
	received = nil
	_, _, err = m.getBulkStatus(context.Background(), ids[:3])
	require.NoError(t, err)
	require.Equal(t, ids[:3], received)

	m.config.client.url = server.URL + "/" + strings.Repeat("p", 4100)
	received = nil
	_, _, err = m.getBulkStatus(context.Background(), ids[:1])
	require.ErrorContains(t, err, "request target leaves no space")
	require.Empty(t, received)
}

func TestBulkStatusUnfitID(t *testing.T) {
	longID := strings.Repeat("x", 4096)
	_, err := chunkChannelIDsForBulkStatus([]string{longID}, 100, 3072, 4000)
	require.ErrorContains(t, err, "request line")
}
