package enricher

import (
	"github.com/rudderlabs/rudder-server/utils/types"
)

type PipelineEnricher interface {
	Enrich(sourceId string, request *types.GatewayBatchRequest) error
}
