package destination

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var pkgLogger = logger.NewLogger().Child("client")

//go:generate mockgen -source=destination.go -destination=mock_destination_test.go -package=destination github.com/rudderlabs/rudder-server/regulation-worker/internal/Destination/destination
type destinationMiddleware interface {
	Get(ctx context.Context) (map[string]backendconfig.ConfigT, error)
}

type DestMiddleware struct {
	Dest destinationMiddleware
}

// This is supposed to be used in casae of singleTenant mode, it will use the set variable `WORKSPACE_TOKEN`.
func (d *DestMiddleware) GetWorkspaceId(ctx context.Context) (string, error) {
	pkgLogger.Debugf("getting destination Id")
	config, err := d.getWorkspacesDetail(ctx)
	if err != nil {
		pkgLogger.Errorf("error while getting destination details from backend config: %v", err)
		return "", err
	}
	if len(config) == 1 {
		for workspaceID := range config {
			pkgLogger.Debugf("workspaceId=", workspaceID)
			return workspaceID, nil
		}
	}

	pkgLogger.Error("workspaceId not found in config")
	return "", fmt.Errorf("workspaceId not found in config")
}

// GetDestDetails makes api call to get json and then parse it to get destination related details
// like: dest_type, auth details,
// return destination Type enum{file, api}
func (d *DestMiddleware) GetDestDetails(ctx context.Context, destID, workspaceID string) (model.Destination, error) {
	pkgLogger.Debugf("getting destination details for destinationId: %v", destID)
	configs, err := d.getWorkspacesDetail(ctx)
	if err != nil {
		return model.Destination{}, err
	}
	workspaceConfig, ok := configs[workspaceID]
	if !ok {
		pkgLogger.Errorf("workspaceId not found in config")
		return model.Destination{}, fmt.Errorf("workspaceId not found in config")
	}

	for _, source := range workspaceConfig.Sources {
		for _, dest := range source.Destinations {
			if dest.ID == destID {
				var destDetail model.Destination
				destDetail.Config = dest.Config
				destDetail.DestinationID = dest.ID
				destDetail.Name = dest.DestinationDefinition.Name
				pkgLogger.Debugf("obtained destination detail: %v", destDetail)
				return destDetail, nil
			}
		}
	}
	return model.Destination{}, model.ErrInvalidDestination
}

func (d *DestMiddleware) getWorkspacesDetail(ctx context.Context) (map[string]backendconfig.ConfigT, error) {
	pkgLogger.Debugf("getting destination details with exponential backoff")

	var err error
	maxWait := time.Minute * 10
	bo := backoff.NewExponentialBackOff()
	boCtx := backoff.WithContext(bo, ctx)
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = maxWait
	var configs map[string]backendconfig.ConfigT
	if err = backoff.Retry(func() error {
		pkgLogger.Debugf("Fetching backend-config...")
		configs, err = d.Dest.Get(ctx)
		if err != nil {
			return fmt.Errorf("error while getting destination details: %w", err)
		}
		return nil
	}, boCtx); err != nil {
		if bo.NextBackOff() == backoff.Stop {
			pkgLogger.Debugf("reached retry limit...")
			return map[string]backendconfig.ConfigT{}, err
		}
	}
	return configs, nil
}
