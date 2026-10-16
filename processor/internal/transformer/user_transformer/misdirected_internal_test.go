package user_transformer

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bufbuild/httplb"
	"github.com/bufbuild/httplb/resolver"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	transformerclient "github.com/rudderlabs/rudder-server/internal/transformer-client"
	transformerutils "github.com/rudderlabs/rudder-server/processor/internal/transformer"
	"github.com/rudderlabs/rudder-server/processor/types"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

// fakeDNS stands in for cluster DNS: a host resolves to the address it points at, and fails like NXDOMAIN, as the name
// of a workspace scaled to zero does, when it points nowhere.
type fakeDNS struct {
	mu      sync.Mutex
	answers map[string]string
}

func (d *fakeDNS) point(host, addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.answers[host] = addr
}

func (d *fakeDNS) remove(host string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.answers, host)
}

func (d *fakeDNS) ResolveOnce(_ context.Context, _, hostPort string) ([]resolver.Address, time.Duration, error) {
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil {
		host = hostPort
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	addr, ok := d.answers[host]
	if !ok {
		return nil, 0, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
	}
	return []resolver.Address{{HostPort: addr}}, 0, nil
}

// httplbFactory builds real httplb clients resolving through fakeDNS, and records per target how many were built and
// closed.
type httplbFactory struct {
	t      testing.TB
	dns    *fakeDNS
	mu     sync.Mutex
	built  map[string]int
	closed map[string]int
}

func (f *httplbFactory) newClient(target string) (transformerclient.Client, func()) {
	// A short TTL so clients keep re-resolving during a test: that is when httplb holds on to a stale address.
	client := httplb.NewClient(httplb.WithResolver(resolver.NewPollingResolver(f.dns, 10*time.Millisecond)))
	f.mu.Lock()
	f.built[target]++
	f.mu.Unlock()
	var once sync.Once
	closeClient := func() {
		once.Do(func() {
			_ = client.Close()
			f.mu.Lock()
			f.closed[target]++
			f.mu.Unlock()
		})
	}
	f.t.Cleanup(closeClient)
	return client, closeClient
}

func (f *httplbFactory) counts(target string) (built, closed int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.built[target], f.closed[target]
}

// pod stands in for a pyt pod. It refuses the hits misdirect picks with 421, the way a pod refuses work that isn't its
// own, and serves every other hit: /customTransform echoes the events, any other path answers "done".
type pod struct {
	srv  *httptest.Server
	hits atomic.Int32
}

func newPod(t *testing.T, misdirect func(hit int32) bool) *pod {
	t.Helper()
	p := &pod{}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit := p.hits.Add(1)
		if misdirect != nil && misdirect(hit) {
			w.Header().Set(transformerclient.HeaderErrorReason, "workspace_mismatch")
			w.Header().Set("Connection", "close")
			http.Error(w, "misdirected", http.StatusMisdirectedRequest)
			return
		}
		if r.URL.Path != "/customTransform" {
			_, _ = w.Write([]byte("done"))
			return
		}
		var events []types.TransformerEvent
		if err := jsonrs.NewDecoder(r.Body).Decode(&events); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		responses := make([]types.TransformerResponse, len(events))
		for i := range events {
			responses[i] = types.TransformerResponse{
				Output: events[i].Message, Metadata: events[i].Metadata, StatusCode: http.StatusOK,
			}
		}
		w.Header().Set("apiVersion", strconv.Itoa(reportingtypes.SupportedTransformerApiVersion))
		_ = jsonrs.NewEncoder(w).Encode(responses)
	}))
	t.Cleanup(p.srv.Close)
	return p
}

func (p *pod) addr() string { return p.srv.Listener.Addr().String() }

func always(int32) bool { return true }

func firstHits(n int32) func(int32) bool { return func(hit int32) bool { return hit <= n } }

type misdirectFixture struct {
	conf    *config.Config
	stats   *memstats.Store
	dns     *fakeDNS
	factory *httplbFactory
}

func newMisdirectFixture(t *testing.T) *misdirectFixture {
	t.Helper()
	statsStore, err := memstats.New()
	require.NoError(t, err)
	conf := config.New()
	conf.Set("USER_TRANSFORM_URL", "http://js-stub:9090")
	conf.Set("Processor.UserTransformer.perWorkspacePyTEnabled", true)
	conf.Set("Processor.UserTransformer.perWorkspacePyTURLTemplate", "http://pyt-{workspaceID}:8080")
	conf.Set("Processor.UserTransformer.maxRetry", 2)
	conf.Set("Processor.UserTransformer.maxRetryBackoffInterval", "1ms")
	dns := &fakeDNS{answers: map[string]string{}}
	return &misdirectFixture{
		conf:    conf,
		stats:   statsStore,
		dns:     dns,
		factory: &httplbFactory{t: t, dns: dns, built: map[string]int{}, closed: map[string]int{}},
	}
}

// client builds the user transformer on the fixture's config; set any config before calling it.
func (f *misdirectFixture) client(opts ...Opt) *Client {
	u := New(f.conf, logger.NOP, f.stats, opts...)
	u.pytClients.newClient = f.factory.newClient
	return u
}

func (f *misdirectFixture) metrics(name string) []memstats.Metric { return f.stats.GetByName(name) }

func events(language, workspaceID string, n int) []types.TransformerEvent {
	evs := make([]types.TransformerEvent, n)
	for i := range evs {
		msgID := fmt.Sprintf("%s-msg-%d", workspaceID, i)
		evs[i] = types.TransformerEvent{
			Metadata: types.Metadata{MessageID: msgID, WorkspaceID: workspaceID},
			Message:  map[string]any{"messageId": msgID},
			Destination: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "test-destination"},
				Transformations: []backendconfig.TransformationT{{
					ID: "transformation-1", VersionID: "version-1", Language: language,
				}},
			},
		}
	}
	return evs
}

func counter(name string, tags stats.Tags, value int) memstats.Metric {
	return memstats.Metric{Name: name, Tags: tags, Value: float64(value)}
}

func TestMisdirectedEventPath(t *testing.T) {
	const (
		targetA = "http://pyt-ws-a:8080"
		targetB = "http://pyt-ws-b:8080"
	)
	misdirectedA := func(n int) memstats.Metric {
		return counter(misdirectedRequestsMetric, stats.Tags{"workspaceID": "ws-a", "language": "python"}, n)
	}
	resetsA := func(n int) memstats.Metric {
		return counter(clientResetsMetric, stats.Tags{"workspaceID": "ws-a", "reason": "misdirected"}, n)
	}

	t.Run("a 421 resets only its target's client, once however many chunks got it", func(t *testing.T) {
		const chunks = 4
		// Hold the first 421s until every chunk is waiting on one, so they all got it from the same client.
		allArrived := make(chan struct{})
		podA := newPod(t, func(hit int32) bool {
			if hit > chunks {
				return false
			}
			if hit == chunks {
				close(allArrived)
			}
			select {
			case <-allArrived:
			case <-time.After(10 * time.Second):
			}
			return true
		})
		podB := newPod(t, nil)
		f := newMisdirectFixture(t)
		f.conf.Set("Processor.UserTransformer.batchSize", 1)
		f.dns.point("pyt-ws-a", podA.addr())
		f.dns.point("pyt-ws-b", podB.addr())
		u := f.client()

		// ws-b first, so it holds a client the ws-a reset could disturb.
		require.Len(t, u.Transform(context.Background(), events("pythonfaas", "ws-b", 1)).Events, 1)

		rsp := u.Transform(context.Background(), events("pythonfaas", "ws-a", chunks))
		require.Len(t, rsp.Events, chunks)
		require.Empty(t, rsp.FailedEvents)
		require.Equal(t, []memstats.Metric{misdirectedA(chunks)}, f.metrics(misdirectedRequestsMetric))
		require.Equal(t, []memstats.Metric{resetsA(1)}, f.metrics(clientResetsMetric))
		require.Empty(t, f.metrics(coldStartErrorsMetric), "a 421 is not a cold start")
		built, _ := f.factory.counts(targetA)
		require.Equal(t, 2, built, "one fresh client for all the chunks")
		require.Eventually(t, func() bool {
			_, closed := f.factory.counts(targetA)
			return closed == 1
		}, 5*time.Second, 10*time.Millisecond, "the replaced client is closed")

		require.Len(t, u.Transform(context.Background(), events("pythonfaas", "ws-b", 1)).Events, 1)
		built, closed := f.factory.counts(targetB)
		require.Equal(t, 1, built, "ws-b keeps its client")
		require.Zero(t, closed)
		require.EqualValues(t, 2, podB.hits.Load())
	})

	t.Run("a 421 from rudder-transformer stays a final 4xx", func(t *testing.T) {
		js := newPod(t, always)
		f := newMisdirectFixture(t)
		f.conf.Set("USER_TRANSFORM_URL", js.srv.URL)
		u := f.client()

		rsp := u.Transform(context.Background(), events("javascript", "ws-a", 1))
		require.Empty(t, rsp.Events)
		require.Len(t, rsp.FailedEvents, 1)
		require.Equal(t, http.StatusMisdirectedRequest, rsp.FailedEvents[0].StatusCode)
		require.EqualValues(t, 1, js.hits.Load(), "not retried")
		require.Empty(t, f.metrics(misdirectedRequestsMetric))
		require.Empty(t, f.metrics(clientResetsMetric))
		require.Empty(t, f.factory.built, "JS stays on the shared client")
	})

	t.Run("premise: an httplb client keeps a stale address once DNS stops answering", func(t *testing.T) {
		// Why a 421 resets the client rather than just retrying: this client would keep going to the same pod.
		stale := newPod(t, nil)
		f := newMisdirectFixture(t)
		f.dns.point("pyt-ws-a", stale.addr())
		client, _ := f.factory.newClient(targetA)
		get := func() error {
			req, err := http.NewRequest(http.MethodGet, targetA+"/health", nil)
			require.NoError(t, err)
			resp, err := client.Do(req)
			if err != nil {
				return err
			}
			defer func() { _ = resp.Body.Close() }()
			_, err = io.Copy(io.Discard, resp.Body)
			return err
		}
		require.NoError(t, get())

		f.dns.remove("pyt-ws-a")
		time.Sleep(50 * time.Millisecond) // several resolver TTLs, each failing
		require.NoError(t, get())
		require.EqualValues(t, 2, stale.hits.Load())
	})

	t.Run("after a reset, a target that no longer resolves is a cold start", func(t *testing.T) {
		f := newMisdirectFixture(t)
		// The workspace is at zero: its name stopped resolving, and only the old client still has the stale IP,
		// which another pod now holds.
		stale := newPod(t, func(int32) bool {
			f.dns.remove("pyt-ws-a")
			return true
		})
		f.dns.point("pyt-ws-a", stale.addr())
		f.conf.Set("Processor.UserTransformer.perWorkspacePyTEndlessRetries", false)
		u := f.client()

		rsp := u.Transform(context.Background(), events("pythonfaas", "ws-a", 1))
		require.Empty(t, rsp.Events)
		require.Len(t, rsp.FailedEvents, 1)
		require.Equal(t, transformerutils.StatusColdStartWindowFailure, rsp.FailedEvents[0].StatusCode)
		require.Contains(t, rsp.FailedEvents[0].Error, "not reachable")
		require.EqualValues(t, 1, stale.hits.Load(), "the fresh client never went back to the stale pod")
		require.Equal(t, []memstats.Metric{misdirectedA(1)}, f.metrics(misdirectedRequestsMetric))
		require.Equal(t, []memstats.Metric{resetsA(1)}, f.metrics(clientResetsMetric))
		require.Equal(t, []memstats.Metric{
			counter(coldStartErrorsMetric, stats.Tags{"workspaceID": "ws-a", "language": "python"}, 2),
		}, f.metrics(coldStartErrorsMetric), "the two attempts after the reset failed the lookup")
	})

	t.Run("after a reset, the retry reaches the target's current pod", func(t *testing.T) {
		f := newMisdirectFixture(t)
		current := newPod(t, nil)
		stale := newPod(t, func(int32) bool {
			f.dns.point("pyt-ws-a", current.addr())
			return true
		})
		f.dns.point("pyt-ws-a", stale.addr())
		u := f.client()

		rsp := u.Transform(context.Background(), events("pythonfaas", "ws-a", 1))
		require.Len(t, rsp.Events, 1)
		require.Empty(t, rsp.FailedEvents)
		require.EqualValues(t, 1, stale.hits.Load())
		require.EqualValues(t, 1, current.hits.Load())
		require.Equal(t, []memstats.Metric{resetsA(1)}, f.metrics(clientResetsMetric))
		require.Empty(t, f.metrics(coldStartErrorsMetric))
	})

	t.Run("a 421 that outlasts the retries becomes 819", func(t *testing.T) {
		stale := newPod(t, always)
		f := newMisdirectFixture(t)
		f.dns.point("pyt-ws-a", stale.addr())
		f.conf.Set("Processor.UserTransformer.perWorkspacePyTEndlessRetries", false)
		u := f.client()

		rsp := u.Transform(context.Background(), events("pythonfaas", "ws-a", 1))
		require.Empty(t, rsp.Events)
		require.Len(t, rsp.FailedEvents, 1)
		require.Equal(t, transformerutils.StatusColdStartWindowFailure, rsp.FailedEvents[0].StatusCode)
		require.Contains(t, rsp.FailedEvents[0].Error, "misdirected")
		// maxRetry=2: three attempts, each refused by the client it was made on.
		require.EqualValues(t, 3, stale.hits.Load())
		require.Equal(t, []memstats.Metric{misdirectedA(3)}, f.metrics(misdirectedRequestsMetric))
		require.Equal(t, []memstats.Metric{resetsA(3)}, f.metrics(clientResetsMetric))
		require.Empty(t, f.metrics(coldStartErrorsMetric))
	})

	t.Run("with endless retries, a 421 that outlasts doPost's retries is retried until it clears", func(t *testing.T) {
		// Five 421s span two doPost calls (three attempts each), so the second one only runs because sendBatch
		// retried the 819 instead of panicking.
		stale := newPod(t, firstHits(5))
		f := newMisdirectFixture(t)
		f.dns.point("pyt-ws-a", stale.addr())
		u := f.client()

		rsp := u.Transform(context.Background(), events("pythonfaas", "ws-a", 1))
		require.Len(t, rsp.Events, 1)
		require.Empty(t, rsp.FailedEvents)
		require.EqualValues(t, 6, stale.hits.Load())
		require.Equal(t, []memstats.Metric{resetsA(5)}, f.metrics(clientResetsMetric))
		require.Empty(t, f.metrics(coldStartErrorsMetric))
	})
}

func TestMisdirectedForwardTest(t *testing.T) {
	const (
		host   = "pyt-test-run-1"
		target = "http://" + host + ":8080"
	)
	misdirected := func(n int) memstats.Metric {
		return counter(misdirectedRequestsMetric,
			stats.Tags{"workspaceID": "ws-a", "language": "python", "path": "/test"}, n)
	}
	resets := func(n int) memstats.Metric {
		return counter(clientResetsMetric, stats.Tags{"workspaceID": "ws-a", "reason": "misdirected"}, n)
	}
	fastBackoff := WithMaxRetryBackoffInterval(config.SingleValueLoader(5 * time.Millisecond))

	t.Run("a 421 resets the target's client and is retried", func(t *testing.T) {
		p := newPod(t, firstHits(1))
		f := newMisdirectFixture(t)
		f.dns.point(host, p.addr())
		u := f.client(fastBackoff)

		status, body, err := u.Test(context.Background(), target, "ws-a", []byte(`{}`))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "done", string(body))
		require.Equal(t, []memstats.Metric{misdirected(1)}, f.metrics(misdirectedRequestsMetric))
		require.Equal(t, []memstats.Metric{resets(1)}, f.metrics(clientResetsMetric))
		require.Empty(t, f.metrics(coldStartErrorsMetric))
		built, _ := f.factory.counts(target)
		require.Equal(t, 2, built)
		require.Eventually(t, func() bool {
			_, closed := f.factory.counts(target)
			return closed == 1
		}, 5*time.Second, 10*time.Millisecond, "the replaced client is closed")
	})

	t.Run("a 421 that outlasts maxRetry fails with ErrMisdirected", func(t *testing.T) {
		p := newPod(t, always)
		f := newMisdirectFixture(t)
		f.dns.point(host, p.addr())
		u := f.client(fastBackoff, WithMaxRetry(config.SingleValueLoader(2)))

		_, _, err := u.Test(context.Background(), target, "ws-a", []byte(`{}`))
		require.ErrorIs(t, err, transformerutils.ErrMisdirected)
		require.ErrorContains(t, err, "workspace_mismatch")
		require.EqualValues(t, 3, p.hits.Load())
		require.Equal(t, []memstats.Metric{resets(3)}, f.metrics(clientResetsMetric))
	})

	t.Run("a 421 is retried no longer than the caller's deadline", func(t *testing.T) {
		p := newPod(t, always)
		f := newMisdirectFixture(t)
		f.dns.point(host, p.addr())
		u := f.client(fastBackoff, WithMaxRetry(config.SingleValueLoader(1000)))

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_, _, err := u.Test(ctx, target, "ws-a", []byte(`{}`))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Greater(t, p.hits.Load(), int32(1))
	})
}
