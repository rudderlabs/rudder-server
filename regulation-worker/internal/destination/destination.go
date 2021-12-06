package destination

import (
	"fmt"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//go:generate mockgen -source=destination.go -destination=mock_destination_test.go -package=destination github.com/rudderlabs/rudder-server/regulation-worker/internal/Destination/destination
type destinationMiddleware interface {
	Get() (backendconfig.ConfigT, bool)
}

type DestMiddleware struct {
	Dest    destinationMiddleware
	DestCat destType
}

type destType interface {
	LoadBatchList() []string
	DestType(batchdest []string, destName string) string
}

type DestCategory struct {
}

//make api call to get json and then parse it to get destination related details
//like: dest_type, auth details,
//return destination Type enum{file, api}
func (d *DestMiddleware) GetDestDetails(destID string) (model.Destination, error) {
	config, ok := d.Dest.Get()
	if !ok {
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
	batchDestinations := d.DestCat.LoadBatchList()
	destDetail.Type = d.DestCat.DestType(batchDestinations, destDetail.Name)
	return destDetail, nil
}

func (dc *DestCategory) LoadBatchList() []string {
	batchDest, _ := misc.LoadDestinations()
	return batchDest
}

func (dc *DestCategory) DestType(batchdest []string, destName string) string {
	if misc.Contains(batchdest, destName) {
		return "batch"
	} else {
		return "API"
	}
}
