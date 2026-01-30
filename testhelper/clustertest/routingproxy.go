package clustertest

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/partmap"
)

type PartitionRoutingProxy interface {
	// Close stops the routing proxy server
	Close()
	// UpdatePartitionMapping updates the mapping of a partition to a node index
	UpdatePartitionMapping(partitionIdx, nodeIndex int)
	// SetPartitionMappings sets the entire partition to node index mapping
	SetPartitionMappings(partitionMappings map[int]int)
}

// NewRoutingProxy creates a new routing proxy that routes requests to different backends based on partition mappings.
// The proxy expects requests to have an "X-Partition-Key" header, which it uses to determine the partition index.
//
// Parameters:
// - numPartitions: Total number of partitions.
// - partitionMappings: Initial mapping of partition indices to backend node indices.
// - backendUrls: URLs of the backend servers to route requests to. Order matters and should correspond to node indices.
//
// Returns:
// - A PartitionRoutingProxy instance that can be used to manage the routing proxy.
func NewRoutingProxy(t *testing.T, numPartitions int, partitionMappings map[int]int, backendUrls ...string) *routingProxy {
	require.GreaterOrEqualf(t, len(backendUrls), 1, "At least one backend URL must be provided")
	rp := &routingProxy{
		numPartitions:     numPartitions,
		partitionMappings: maps.Clone(partitionMappings),
	}
	for _, b := range backendUrls {
		backendUrl, _ := url.Parse(b)
		rp.backends = append(rp.backends, httputil.NewSingleHostReverseProxy(backendUrl))
	}
	rp.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		partitionKey := r.Header.Get("X-Partition-Key")
		if partitionKey == "" {
			http.Error(w, "missing X-Partition-Key header", http.StatusBadRequest)
			return
		}
		partitionIdx, _ := partmap.Murmur3Partition32(partitionKey, uint32(numPartitions))
		rp.partitionMappingsMu.RLock()
		nodeIndex, ok := rp.partitionMappings[int(partitionIdx)]
		rp.partitionMappingsMu.RUnlock()
		if !ok || nodeIndex < 0 || nodeIndex >= len(rp.backends) {
			http.Error(w, "no backend for partition", http.StatusBadGateway)
			return
		}
		rp.backends[nodeIndex].ServeHTTP(w, r)
	}))
	return rp
}

type routingProxy struct {
	*httptest.Server
	numPartitions       int
	partitionMappingsMu sync.RWMutex
	partitionMappings   map[int]int
	backends            []*httputil.ReverseProxy
}

func (rp *routingProxy) UpdatePartitionMapping(partitionIdx, nodeIndex int) {
	rp.partitionMappingsMu.Lock()
	defer rp.partitionMappingsMu.Unlock()
	rp.partitionMappings[partitionIdx] = nodeIndex
}

func (rp *routingProxy) SetPartitionMappings(partitionMappings map[int]int) {
	rp.partitionMappingsMu.Lock()
	defer rp.partitionMappingsMu.Unlock()
	rp.partitionMappings = maps.Clone(partitionMappings)
}
