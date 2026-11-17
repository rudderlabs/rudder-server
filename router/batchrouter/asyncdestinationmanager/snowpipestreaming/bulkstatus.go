package snowpipestreaming

import (
	"fmt"

	snowpipeapi "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/api"
)

// chunkChannelIDsForBulkStatus bounds both the encoded query and the endpoint's
// ID count. A singleton may exceed the configured budget, but never the HTTP
// request-line budget.
func chunkChannelIDsForBulkStatus(ids []string, maxCount, softBudget, hardBudget int) ([][]string, error) {
	var chunks [][]string
	var chunk []string
	queryBytes := 0
	for _, id := range ids {
		idBytes := snowpipeapi.BulkStatusChannelIDQueryBytes(id)
		if idBytes > hardBudget {
			return nil, fmt.Errorf("bulk status channel ID needs %d query bytes, only %d fit in the request line", idBytes, hardBudget)
		}
		if len(chunk) > 0 && (len(chunk) == maxCount || queryBytes+1+idBytes > min(softBudget, hardBudget)) {
			chunks = append(chunks, chunk)
			chunk = nil
			queryBytes = 0
		}
		if len(chunk) > 0 {
			queryBytes++ // '&' between channelIds parameters
		}
		chunk = append(chunk, id)
		queryBytes += idBytes
	}
	if len(chunk) > 0 {
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}
