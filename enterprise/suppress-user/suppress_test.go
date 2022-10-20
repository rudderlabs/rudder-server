package suppression_test

import (
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

func BenchmarkSuppress(b *testing.B) {
	config.Reset()
	logger.Reset()
	backendconfig.Init()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		load := &suppressLoad{T: b, UserCount: 40_000_000, Done: make(chan struct{})}
		s := httptest.NewServer(load)
		defer s.Close()
		b.StartTimer()

		r := suppression.SuppressFast{
			RegulationBackendURL: s.URL,
			Client:               s.Client(),
			PageSize:             "10000",
		}

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			<-load.Done

			f, _ := os.Create("./profile.pb.gz")
			defer f.Close()
			pprof.WriteHeapProfile(f)

			time.Sleep(time.Second)

			runtime.GC()
			f, _ = os.Create("./profile-2.pb.gz")
			defer f.Close()
			pprof.WriteHeapProfile(f)

			cancel()
		}()

		r.Run(ctx)

		b.StopTimer()
	}
}

type suppressLoad struct {
	T         testing.TB
	UserCount int
	Done      chan struct{}
	once      sync.Once
}

func (s *suppressLoad) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	pageToken := r.URL.Query().Get("pageToken")
	if pageToken == "" {
		pageToken = "0"
	}
	pageSize := r.URL.Query().Get("pageSize")
	if pageSize == "" {
		pageSize = "10000"
	}

	size, err := strconv.Atoi(pageSize)
	require.NoError(s.T, err)

	startFrom, err := strconv.Atoi(pageToken)
	require.NoError(s.T, err)

	end := startFrom + size

	w.Header().Set("Content-Type", "application/json")
	if startFrom >= s.UserCount {
		s.once.Do(func() {
			fmt.Println("server ", s.UserCount)
			close(s.Done)
		})
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"items": [], "token": "` + pageToken + `"}`))
		return
	}

	if end > s.UserCount {
		end = s.UserCount
	}

	body := gzip.NewWriter(w)
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Set("X-Page-Token", strconv.Itoa(end))
	w.WriteHeader(http.StatusOK)

	body.Write([]byte(`{"items": [`))
	for i := startFrom; i < end; i++ {
		body.Write([]byte(`{"userId": "user-` + uuid.New().String() + `"},`))
	}
	body.Write([]byte(`{"userId": "user-` + uuid.New().String() + `"}`))
	body.Write([]byte(`], "token": "` + strconv.Itoa(end) + `"}`))

	body.Close()
}

func TestJSONDecode(t *testing.T) {
	jsonStream := `{"items": [{"userId": "user-1"}, {"userId": "user-2"}], "token": "1"}`

	iter := suppression.RegulationIter{
		Decoder: json.NewDecoder(strings.NewReader(jsonStream)),
	}

	var regs []any

	for iter.Next() {
		regs = append(regs, iter.Item())
	}

	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}

	require.Len(t, regs, 2)
}
