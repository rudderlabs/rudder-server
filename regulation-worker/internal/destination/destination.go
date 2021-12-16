package destination

import (
	"fmt"

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
func (d *DestMiddleware) GetDestDetails(destID string) (model.Destination, error) {
	pkgLogger.Debugf("getting destination details for destinationId: %w", destID)
	config, ok := d.Dest.Get()
	if !ok {
		pkgLogger.Errorf("error while getting destination details")
		return model.Destination{}, fmt.Errorf("error while getting destination details")
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

	pkgLogger.Debugf("obtained destination detail: %w", destDetail)
	return destDetail, nil
}
