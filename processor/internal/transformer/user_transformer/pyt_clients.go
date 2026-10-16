package user_transformer

import (
	"net/url"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"

	transformerclient "github.com/rudderlabs/rudder-server/internal/transformer-client"
)

// pytClients hands out one transformer client per pyt target, so a 421 can discard what the client knows about one
// target's addresses without disturbing any other target. A client is built on first use and closed once nothing has
// used it for the idle timeout, which keeps the per-run hosts of ephemeral test pods from piling up.
//
// Clients are reference counted: one is closed only once it has left the registry (idle, or retired after a 421) and
// its last holder has released it, so a close never lands under a request.
//
// There is no janitor to stop on shutdown, only a timer per idle client: like the shared JS client, the registry lives
// as long as the process.
type pytClients struct {
	newClient   func(target string) (client transformerclient.Client, closeClient func())
	idleTimeout config.ValueLoader[time.Duration]

	mu      sync.Mutex
	clients map[string]*pytClient
}

// pytClient is a client checked out of [pytClients]. All fields but the embedded client are guarded by pytClients.mu.
type pytClient struct {
	transformerclient.Client
	target      string
	closeClient func()

	inUse int
	// retired clients have left the registry and are closed once inUse drops to zero. A client that is not retired is
	// always the registry's current client for its target.
	retired bool
	idle    *time.Timer
	// idleEpoch invalidates an idle timer that fired while the client was being used again.
	idleEpoch uint64
}

func newPyTClients(
	newClient func(target string) (transformerclient.Client, func()), idleTimeout config.ValueLoader[time.Duration],
) *pytClients {
	return &pytClients{newClient: newClient, idleTimeout: idleTimeout, clients: make(map[string]*pytClient)}
}

// pytTarget is the registry key of a request: one client per scheme://host:port, the same granularity httplb pools at.
func pytTarget(u *url.URL) string {
	return u.Scheme + "://" + u.Host
}

// acquire returns target's current client, building it if there is none. The caller must [pytClients.release] it once
// the response body is closed.
func (p *pytClients) acquire(target string) *pytClient {
	p.mu.Lock()
	defer p.mu.Unlock()
	c := p.clients[target]
	if c == nil {
		client, closeClient := p.newClient(target)
		c = &pytClient{Client: client, target: target, closeClient: closeClient}
		p.clients[target] = c
	}
	if c.idle != nil {
		c.idle.Stop()
		c.idle = nil
	}
	c.inUse++
	return c
}

// release hands c back. Once nobody holds it, a retired client is closed in the background, which keeps httplb's
// Close (it waits for in-flight requests on the client) off the request path; any other starts its idle countdown.
func (p *pytClients) release(c *pytClient) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c.inUse--
	if c.inUse > 0 {
		return
	}
	if c.retired {
		go c.closeClient()
		return
	}
	c.idleEpoch++
	epoch := c.idleEpoch
	c.idle = time.AfterFunc(p.idleTimeout.Load(), func() { p.closeIfIdle(c, epoch) })
}

func (p *pytClients) closeIfIdle(c *pytClient, epoch uint64) {
	p.mu.Lock()
	if c.inUse > 0 || c.retired || c.idleEpoch != epoch {
		p.mu.Unlock()
		return
	}
	c.retired = true
	c.idle = nil
	delete(p.clients, c.target)
	p.mu.Unlock()
	c.closeClient()
}

// retire drops c, which the caller must hold, from the registry, so the next acquire for its target builds a fresh
// client; c itself is closed once its last holder releases it. It reports whether this call retired c, which holds for
// exactly one caller per client however many concurrent requests got the same 421 from it.
func (p *pytClients) retire(c *pytClient) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c.retired {
		return false
	}
	c.retired = true
	delete(p.clients, c.target)
	return true
}
