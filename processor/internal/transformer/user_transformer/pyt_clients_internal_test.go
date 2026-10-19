package user_transformer

import (
	"errors"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	transformerclient "github.com/rudderlabs/rudder-server/internal/transformer-client"
)

func TestPyTClients(t *testing.T) {
	const (
		targetA = "http://pyt-ws-a:8080"
		targetB = "http://pyt-ws-b:8080"
	)

	t.Run("one client per target, reused while current", func(t *testing.T) {
		p, f := newTestPyTClients(time.Hour)
		a1, a2, b := p.acquire(targetA), p.acquire(targetA), p.acquire(targetB)
		require.Same(t, a1, a2)
		require.NotSame(t, a1, b)
		require.Len(t, f.clients(), 2)
		p.release(a1)
		p.release(a2)
		p.release(b)
		require.Same(t, a1, p.acquire(targetA), "a released client stays current until it idles out")
	})

	t.Run("retire resets only its target, once per client", func(t *testing.T) {
		p, f := newTestPyTClients(time.Hour)
		a, b := p.acquire(targetA), p.acquire(targetB)
		other := p.acquire(targetA) // a second request that got the same 421 from a
		require.Same(t, a, other)

		require.True(t, p.retire(a), "the first 421 from a client resets it")
		require.False(t, p.retire(other), "a 421 from an already-retired client must not reset again")

		fresh := p.acquire(targetA)
		require.NotSame(t, a, fresh, "the next request for the target gets a fresh client")
		require.False(t, p.retire(a), "a stale 421 must not reset the fresh client")
		require.Same(t, fresh, p.acquire(targetA))
		require.Same(t, b, p.acquire(targetB), "other targets keep their client")
		require.Len(t, f.clients(), 3)
		require.False(t, stubOf(b).closed())
	})

	t.Run("a retired client is closed only once its last holder releases it", func(t *testing.T) {
		p, _ := newTestPyTClients(time.Hour)
		held := p.acquire(targetA)
		require.Same(t, held, p.acquire(targetA), "two requests hold the same client")
		require.True(t, p.retire(held))

		p.release(held)
		require.Never(t, stubOf(held).closed, 50*time.Millisecond, 5*time.Millisecond,
			"closed while a request still held it")
		require.NoError(t, sendOnStub(held))

		p.release(held)
		require.Eventually(t, stubOf(held).closed, time.Second, 5*time.Millisecond)
	})

	t.Run("an idle client is closed after the idle timeout and rebuilt on next use", func(t *testing.T) {
		p, f := newTestPyTClients(10 * time.Millisecond)
		a := p.acquire(targetA)
		p.release(a)
		require.Eventually(t, stubOf(a).closed, time.Second, 5*time.Millisecond)

		fresh := p.acquire(targetA)
		require.NotSame(t, a, fresh)
		require.False(t, stubOf(fresh).closed())
		require.Len(t, f.clients(), 2)
		require.EqualValues(t, 1, stubOf(a).closes.Load(), "closed exactly once")
	})

	t.Run("a client used again before its idle timeout is not closed while held", func(t *testing.T) {
		p, _ := newTestPyTClients(50 * time.Millisecond)
		a := p.acquire(targetA)
		p.release(a)
		require.Same(t, a, p.acquire(targetA))
		require.Never(t, stubOf(a).closed, 100*time.Millisecond, 5*time.Millisecond)
	})

	t.Run("an idle timer that fired while the client was used again closes nothing", func(t *testing.T) {
		// The race itself can't be scheduled deterministically, so drive closeIfIdle the way a timer would.
		p, _ := newTestPyTClients(time.Hour)
		a := p.acquire(targetA)
		p.release(a)
		staleEpoch := a.idleEpoch

		p.acquire(targetA)
		p.closeIfIdle(a, staleEpoch)
		require.False(t, stubOf(a).closed(), "in use")

		p.release(a)
		p.closeIfIdle(a, staleEpoch)
		require.False(t, stubOf(a).closed(), "idle again, but its countdown restarted")

		p.closeIfIdle(a, a.idleEpoch)
		require.True(t, stubOf(a).closed())
		require.NotSame(t, a, p.acquire(targetA))
	})

	t.Run("concurrent use never closes a client under a request", func(t *testing.T) {
		// Zero idle timeout: every release that leaves a client unused races its close against the next acquire.
		p, f := newTestPyTClients(0)
		targets := []string{targetA, targetB}
		var wg sync.WaitGroup
		var closedUnderRequest atomic.Int32
		for range 16 {
			wg.Go(func() {
				for range 200 {
					c := p.acquire(targets[rand.IntN(len(targets))])
					if err := sendOnStub(c); err != nil {
						closedUnderRequest.Add(1)
					}
					if rand.IntN(4) == 0 {
						p.retire(c)
					}
					p.release(c)
				}
			})
		}
		wg.Wait()

		require.Zero(t, closedUnderRequest.Load())
		require.Greater(t, len(f.clients()), len(targets), "the run must have replaced clients to mean anything")
		require.Eventually(t, func() bool {
			for _, c := range f.clients() {
				if c.closes.Load() != 1 {
					return false
				}
			}
			return true
		}, 5*time.Second, 10*time.Millisecond, "every client that left the registry is closed exactly once")
	})
}

func TestPyTClientIdleTimeoutConfig(t *testing.T) {
	conf := config.New()
	u := New(conf, logger.NOP, stats.NOP)
	require.Equal(t, 5*time.Minute, u.pytClients.idleTimeout.Load(), "default")

	conf.Set("Processor.UserTransformer.pytClientIdleTimeout", "42s")
	require.Equal(t, 42*time.Second, u.pytClients.idleTimeout.Load(), "reloaded without a restart")
}

func TestPyTClientsSlowCloseStaysOffTheRequestPath(t *testing.T) {
	const (
		targetA = "http://pyt-ws-a:8080"
		targetB = "http://pyt-ws-b:8080"
	)

	t.Run("closing a retired client holds up neither its release nor other targets", func(t *testing.T) {
		p, closesStarted := newSlowClosePyTClients(t, time.Hour)
		a := p.acquire(targetA)
		require.True(t, p.retire(a))

		requireReturns(t, func() { p.release(a) }, "release waited for the close")
		require.Eventually(t, func() bool { return closesStarted.Load() == 1 }, time.Second, time.Millisecond)
		requireReturns(t, func() { p.release(p.acquire(targetB)) }, "another target waited for the close")
	})

	t.Run("an idle close holds up no other target", func(t *testing.T) {
		p, closesStarted := newSlowClosePyTClients(t, time.Millisecond)
		p.release(p.acquire(targetA))
		require.Eventually(t, func() bool { return closesStarted.Load() == 1 }, time.Second, time.Millisecond)

		requireReturns(t, func() { p.release(p.acquire(targetB)) }, "another target waited for the idle close")
	})
}

func TestWithClientServesPyTTargetsWithoutClosingTheClient(t *testing.T) {
	const target = "http://pyt-ws-a:8080"
	injected := &callerOwnedClient{}
	conf := config.New()
	conf.Set("Processor.UserTransformer.pytClientIdleTimeout", "1ms")
	u := New(conf, logger.NOP, stats.NOP, WithClient(injected))

	retired := u.pytClients.acquire(target)
	require.Same(t, injected, retired.Client)
	require.True(t, u.pytClients.retire(retired))
	u.pytClients.release(retired) // its close func runs in the background

	idle := u.pytClients.acquire(target)
	require.NotSame(t, retired, idle)
	require.Same(t, injected, idle.Client)
	u.pytClients.release(idle) // its close func runs from the idle timer
	require.Eventually(t, func() bool {
		u.pytClients.mu.Lock()
		defer u.pytClients.mu.Unlock()
		_, current := u.pytClients.clients[target]
		return !current
	}, time.Second, time.Millisecond, "the idle client must have left the registry")

	require.Never(t, injected.closed, 50*time.Millisecond, 5*time.Millisecond, "the caller owns the client")
	require.NoError(t, sendOnStub(idle))
}

// stubClient is a client the registry tests hand out. Its Do fails once the client is closed, so a test can catch a
// close landing under a request.
type stubClient struct {
	closes atomic.Int32
}

var errClosedClientUsed = errors.New("request on a closed client")

func (s *stubClient) Do(*http.Request) (*http.Response, error) {
	if s.closes.Load() > 0 {
		return nil, errClosedClientUsed
	}
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

func (s *stubClient) closed() bool { return s.closes.Load() > 0 }

type stubFactory struct {
	mu    sync.Mutex
	built []*stubClient
}

func (f *stubFactory) newClient(string) (transformerclient.Client, func()) {
	c := &stubClient{}
	f.mu.Lock()
	f.built = append(f.built, c)
	f.mu.Unlock()
	return c, func() { c.closes.Add(1) }
}

func (f *stubFactory) clients() []*stubClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*stubClient(nil), f.built...)
}

func newTestPyTClients(idleTimeout time.Duration) (*pytClients, *stubFactory) {
	f := &stubFactory{}
	return newPyTClients(f.newClient, config.SingleValueLoader(idleTimeout)), f
}

func stubOf(c *pytClient) *stubClient { return c.Client.(*stubClient) }

// sendOnStub sends a request on c, which fails if c was closed under the caller holding it.
func sendOnStub(c *pytClient) error {
	resp, err := c.Do(nil)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// newSlowClosePyTClients builds a registry whose clients take until the end of the test to close, like an httplb Close
// waiting on a slow request still in flight. It reports how many closes have started.
func newSlowClosePyTClients(t *testing.T, idleTimeout time.Duration) (*pytClients, *atomic.Int32) {
	t.Helper()
	var closesStarted atomic.Int32
	unblock := make(chan struct{})
	t.Cleanup(func() { close(unblock) })
	return newPyTClients(func(string) (transformerclient.Client, func()) {
		return &stubClient{}, func() {
			closesStarted.Add(1)
			<-unblock
		}
	}, config.SingleValueLoader(idleTimeout)), &closesStarted
}

// requireReturns fails the test unless f returns promptly. f runs in its own goroutine, so a regression fails the test
// instead of hanging it.
func requireReturns(t *testing.T, f func(), msg string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, time.Millisecond, msg)
}

// callerOwnedClient is a client passed to WithClient. Its caller owns it, so any close through the methods httplb's and
// the stdlib's clients have counts against the registry.
type callerOwnedClient struct{ stubClient }

func (c *callerOwnedClient) Close() error {
	c.closes.Add(1)
	return nil
}

func (c *callerOwnedClient) CloseIdleConnections() { c.closes.Add(1) }
