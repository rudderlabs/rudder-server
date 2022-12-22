package destination

import (
	"context"
	"sync"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

var pkgLogger = logger.NewLogger().Child("client")

//go:generate mockgen -source=destination.go -destination=mock_destination.go -package=destination github.com/rudderlabs/rudder-server/regulation-worker/internal/Destination/destination
type destMiddleware interface {
	Identity() identity.Identifier
	StartWithIDs(ctx context.Context, workspaces string)
	Subscribe(ctx context.Context, topic backendconfig.Topic) pubsub.DataChannel
}

type DestinationConfig struct {
	mu     sync.RWMutex
	Config backendconfig.ConfigT
	Dest   destMiddleware
}

func (d *DestinationConfig) GetDestDetails(ctx context.Context, destID string) (model.Destination, error) {
	pkgLogger.Debugf("getting destination details for destinationId: %v", destID)
	d.mu.RLock()
	config := d.Config
	d.mu.RUnlock()
	for _, source := range config.Sources {
		for _, dest := range source.Destinations {
			if dest.ID == destID {
				var destDetail model.Destination
				destDetail.Config = dest.Config
				destDetail.DestinationID = dest.ID
				destDetail.Name = dest.DestinationDefinition.Name
				destDetail.DestDefConfig = dest.DestinationDefinition.Config
				pkgLogger.Debugf("obtained destination detail: %v", destDetail)
				return destDetail, nil
			}
		}
	}
	return model.Destination{}, model.ErrInvalidDestination
}

func (d *DestinationConfig) BackendConfigSubscriber(ctx context.Context) {
	workspaceId := d.Dest.Identity().ID()
	d.Dest.StartWithIDs(ctx, workspaceId)
	ch := d.Dest.Subscribe(ctx, backendconfig.TopicBackendConfig)
	for data := range ch {
		d.mu.Lock()
		tmp := data.Data.(map[string]backendconfig.ConfigT)
		d.Config = tmp[workspaceId]
		d.mu.Unlock()
	}
}
