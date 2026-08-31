package rsources

import (
	"context"
	"fmt"
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// The delegate's view of the control plane.
//
// The setting lives on the connection, at
// `config.source.syncSettings.errorDetailsConfig.enabled`. The component keeps its own
// backend-config subscription rather than borrowing the processor's: the decision is
// now taken on the router/batch-router status-update path, which has no processor in
// it, and the flag no longer rides the job's parameters.

// ConfigSubscriberRoutine consumes the component's backend-config subscription and
// rebuilds the connection index on every push.
//
// It returns when the subscription closes (utils/pubsub closes it when the context
// handed to Subscribe - the constructor's - is done), when ctx is cancelled, or when
// Stop is called. The last of those matters: without it, a Stop that does not also
// cancel the constructor's context would block forever in wg.Wait().
func (d *syncSettingDelegate) ConfigSubscriberRoutine(ctx context.Context) error {
	if !d.routineStarted() {
		return nil
	}
	defer d.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-d.stop:
			return nil
		case data, open := <-d.configCh:
			if !open {
				return nil
			}
			workspaces, ok := data.Data.(map[string]backendconfig.ConfigT)
			if !ok {
				// TopicProcessConfig always carries map[string]ConfigT; a different
				// shape means the topic contract changed under us and every decision
				// from here on would silently be false.
				d.log.Errorn("rsources sync settings: unexpected backend config payload")
				continue
			}
			d.indexConnections(workspaces)
		}
	}
}

// indexConnections rebuilds the {sourceID, destinationID} -> enabled index wholesale.
//
// Rebuilt rather than merged so that a connection removed from the control plane
// disappears here too; a partial update would leave a deleted connection answering
// `true` forever. The index spans every workspace in the push - one process serves
// many workspaces, and the connection identity is already globally unique.
func (d *syncSettingDelegate) indexConnections(workspaces map[string]backendconfig.ConfigT) {
	index := make(map[connectionKey]bool)
	for _, wConfig := range workspaces {
		for _, conn := range wConfig.Connections {
			enabled, _ := misc.MapLookup(
				conn.Config, "source", "syncSettings", "errorDetailsConfig", "enabled",
			).(bool)
			index[connectionKey{sourceID: conn.SourceID, destinationID: conn.DestinationID}] = enabled
		}
	}
	d.connectionsMu.Lock()
	d.connections = index
	d.connectionsMu.Unlock()
	d.configLoadedOnce.Do(func() { close(d.configLoaded) })
}

// connectionEnabled resolves the setting for one connection. It fails closed: an
// unknown connection, a missing path or a non-boolean value all resolve to false.
func (d *syncSettingDelegate) connectionEnabled(sourceID, destinationID string) bool {
	d.connectionsMu.RLock()
	defer d.connectionsMu.RUnlock()
	return d.connections[connectionKey{sourceID: sourceID, destinationID: destinationID}]
}

// configWaitTimeout bounds the startup-window wait in awaitConfig. The caller's context
// belongs to a router status-update transaction and can be minutes long, which is far
// too long to hold that path; failing after a few seconds turns the wait into a normal
// batch error that the router retries, by which time the config has landed.
const configWaitTimeout = 30 * time.Second

// awaitConfig blocks until the first backend-config push has been indexed.
//
// Only the pin-computing path calls it, and only on a genuine cache miss. Without it a
// failed record arriving in the window between construction and the first push would
// resolve against an empty index and pin the run to false for the whole retention -
// permanently wrong, with nothing to indicate it. After the first push this is a read
// from a closed channel.
func (d *syncSettingDelegate) awaitConfig(ctx context.Context) error {
	select {
	case <-d.configLoaded:
		return nil
	default:
	}
	ctx, cancel := context.WithTimeout(ctx, configWaitTimeout)
	defer cancel()
	select {
	case <-d.configLoaded:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for the first backend config push: %w", ctx.Err())
	}
}
