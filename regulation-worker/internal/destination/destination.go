package destination

import (
	"context"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

var pkgLogger = logger.NewLogger().Child("client")

//go:generate mockgen -source=destination.go -destination=mock_destination.go -package=destination github.com/rudderlabs/rudder-server/regulation-worker/internal/Destination/destination
type destMiddleware interface {
	Subscribe(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel
}

type DestinationConfig struct {
	mu           sync.RWMutex
	destinations map[string]model.Destination
	Dest         destMiddleware
}

func (d *DestinationConfig) GetDestDetails(destID string) (model.Destination, error) {
	pkgLogger.Debugf("getting destination details for destinationId: %v", destID)
	d.mu.RLock()
	defer d.mu.RUnlock()
	destination, ok := d.destinations[destID]
	if !ok {
		return model.Destination{}, model.ErrInvalidDestination
	}
	return destination, nil
}

// Start starts listening for configuration updates and updates the destinations.
// The method blocks until the first update is received.
func (d *DestinationConfig) Start(ctx context.Context) {
	initialized := make(chan struct{})
	ch := d.Dest.Subscribe(ctx, backendconfig.TopicBackendConfig)
	rruntime.Go(func() {
		for data := range ch {
			destinations := make(map[string]model.Destination)
			configs := data.Data.(map[string]backendconfig.ConfigT)
			for _, config := range configs {
				for _, source := range config.Sources {
					for _, dest := range source.Destinations {
						destinations[dest.ID] = model.Destination{
							DestinationID: dest.ID,
							Config:        dest.Config,
							Name:          dest.DestinationDefinition.Name,
							DestDefConfig: dest.DestinationDefinition.Config,
						}
					}
				}
			}
			d.mu.Lock()
			d.destinations = destinations
			d.mu.Unlock()
			select {
			case <-initialized:
			default:
				close(initialized)
			}
		}
	})
	<-initialized
}
