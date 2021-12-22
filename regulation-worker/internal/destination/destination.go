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
	Get() (backendconfig.ConfigT, bool)
}

type DestMiddleware struct {
	Dest destinationMiddleware
}

//make api call to get json and then parse it to get destination related details
//like: dest_type, auth details,
//return destination Type enum{file, api}
func (d *DestMiddleware) GetDestDetails(ctx context.Context, destID string) (model.Destination, error) {
	pkgLogger.Debugf("getting destination details for destinationId: %w", destID)
	config, err := d.getDestDetails(ctx, destID)
	if err != nil {
		return model.Destination{}, err
	}

	destDetail := model.Destination{}
	for _, source := range config.Sources {
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

	pkgLogger.Debugf("obtained destination detail: %w", destDetail)
	return destDetail, nil
}

func (d *DestMiddleware) getDestDetails(ctx context.Context, destId string) (backendconfig.ConfigT, error) {

	maxWait := time.Minute * 10
	var err error
	bo := backoff.NewExponentialBackOff()
	boCtx := backoff.WithContext(bo, ctx)
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = maxWait
	var config backendconfig.ConfigT
	var ok bool
	if err = backoff.Retry(func() error {
		config, ok = d.Dest.Get()
		pkgLogger.Debugf("trying to update status...")
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
	return config, nil
}
