package batchrouter

import (
	"time"

	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
)

// asyncDestination is what an async worker needs to know about its destination. The config subscriber replaces all
// destinations at once, and workers only read them.
type asyncDestination struct {
	routerutils.DestinationWithSources
	manager        asynccommon.AsyncDestinationManager
	uploadInterval time.Duration
}

// asyncDestination returns the destination, or false if it isn't in the config
func (brt *Handle) asyncDestination(destinationID string) (*asyncDestination, bool) {
	brt.configSubscriberMu.RLock()
	defer brt.configSubscriberMu.RUnlock()
	dest, ok := brt.asyncDestinations[destinationID]
	return dest, ok
}

// newAsyncDestinations returns the async destinations of a config. A destination keeps its manager while its revision
// doesn't change, unless it is an invalid manager due for a retry.
func (brt *Handle) newAsyncDestinations(destinationsMap map[string]*routerutils.DestinationWithSources, uploadIntervals map[string]time.Duration) map[string]*asyncDestination {
	destinations := make(map[string]*asyncDestination, len(destinationsMap))
	for id, dest := range destinationsMap {
		destinations[id] = &asyncDestination{
			DestinationWithSources: *dest,
			manager:                brt.refreshAsyncManager(id, dest),
			uploadInterval:         uploadIntervals[id],
		}
	}
	return destinations
}

func (brt *Handle) refreshAsyncManager(destinationID string, dest *routerutils.DestinationWithSources) asynccommon.AsyncDestinationManager {
	if previous, ok := brt.asyncDestination(destinationID); ok && previous.Destination.RevisionID == dest.Destination.RevisionID {
		invalidManager, isInvalid := previous.manager.(*asynccommon.InvalidManager)
		if !isInvalid || !brt.shouldRetryInvalidManager(invalidManager) {
			return previous.manager
		}
	}
	return brt.newAsyncDestinationManager(&dest.Destination)
}
