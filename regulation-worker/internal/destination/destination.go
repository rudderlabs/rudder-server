package destination

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var pkgLogger = logger.NewLogger().Child("client")

//go:generate mockgen -source=destination.go -destination=mock_destination_test.go -package=destination github.com/rudderlabs/rudder-server/regulation-worker/internal/Destination/destination
type destinationMiddleware interface {
	Get(workspace string) (backendconfig.ConfigT, bool)
}

type DestMiddleware struct {
	Dest destinationMiddleware
}

func (d *DestMiddleware) GetWorkspaceId(ctx context.Context) (string, error) {
	pkgLogger.Debugf("getting destination Id")
	destConfig, err := d.getDestDetails(ctx)
	if err != nil {
		pkgLogger.Errorf("error while getting destination details from backend config: %v", err)
		return "", err
	}
	if destConfig.WorkspaceID != "" {
		pkgLogger.Debugf("workspaceId=", destConfig.WorkspaceID)
		return destConfig.WorkspaceID, nil
	}

	pkgLogger.Error("workspaceId not found in config")
	return "", fmt.Errorf("workspaceId not found in config")
}

//make api call to get json and then parse it to get destination related details
//like: dest_type, auth details,
//return destination Type enum{file, api}
func (d *DestMiddleware) GetDestDetails(ctx context.Context, destID string) (model.Destination, error) {
	pkgLogger.Debugf("getting destination details for destinationId: %v", destID)
	destConf, err := d.getDestDetails(ctx)
	if err != nil {
		return model.Destination{}, err
	}

	destDetail := model.Destination{}
	for _, source := range destConf.Sources {
		for _, dest := range source.Destinations {
			if dest.ID == destID {
				destDetail.Config = dest.Config
				destDetail.DestinationID = dest.ID
				destDetail.Name = dest.DestinationDefinition.Name
			}
		}
	}
	if destDetail.Name == "" {
		return model.Destination{}, model.ErrInvalidDestination
	}

	pkgLogger.Debugf("obtained destination detail: %v", destDetail)
	return destDetail, nil
}

func (d *DestMiddleware) getDestDetails(ctx context.Context) (backendconfig.ConfigT, error) {
	pkgLogger.Debugf("getting destination details with exponential backoff")

	maxWait := time.Minute * 10
	var err error
	bo := backoff.NewExponentialBackOff()
	boCtx := backoff.WithContext(bo, ctx)
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = maxWait
	var destConf backendconfig.ConfigT
	var ok bool
	if err = backoff.Retry(func() error {
		pkgLogger.Debugf("Fetching backend-config...")
		// TODO : Revisit the Implementation for Regulation Worker in case of MultiTenant Deployment
		destConf, ok = d.Dest.Get(config.GetWorkspaceToken())
		if !ok {
			return fmt.Errorf("error while getting destination details")
		}
		return nil
	}, boCtx); err != nil {
		if bo.NextBackOff() == backoff.Stop {
			pkgLogger.Debugf("reached retry limit...")
			return backendconfig.ConfigT{}, err
		}

	}
	return destConf, nil
}
