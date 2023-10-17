package enricher

import (
	"github.com/rudderlabs/rudder-server/utils/types"
)

// PipelineEnricher is a new paradigm under which the gateway events in
// processing pipeline are enriched with new information based on the handler passed.
type PipelineEnricher interface {
	Enrich(sourceId string, request *types.GatewayBatchRequest) error
}
