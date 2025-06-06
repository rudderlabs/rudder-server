package collector

import (
	"sync"

	proctypes "github.com/rudderlabs/rudder-server/processor/types"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

type MetricsCollector interface {
	Collect(response proctypes.TransformerResponse, stage string) error
	CollectMultiple(responses []proctypes.TransformerResponse, stage string) error
	GetMetrics() []*reportingtypes.PUReportedMetric
	Reset()
}

type MetricsStore struct {
	sync.RWMutex
	statusDetailsMap     map[string]map[string]*reportingtypes.StatusDetail
	connectionDetailsMap map[string]*reportingtypes.ConnectionDetails
	puDetailsMap         map[string]*reportingtypes.PUDetails
	metrics              []*reportingtypes.PUReportedMetric
}

func NewMetricsStore() *MetricsStore {
	return &MetricsStore{
		statusDetailsMap:     make(map[string]map[string]*reportingtypes.StatusDetail),
		connectionDetailsMap: make(map[string]*reportingtypes.ConnectionDetails),
		puDetailsMap:         make(map[string]*reportingtypes.PUDetails),
		metrics:              make([]*reportingtypes.PUReportedMetric, 0),
	}
}
