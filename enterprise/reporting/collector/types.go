package collector

import (
	"sync"

	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

type MetricsCollector interface {
	Collect(event *reportingtypes.MetricEvent) error
	CollectMultiple(events []*reportingtypes.MetricEvent) error
	GetMetrics() []*reportingtypes.PUReportedMetric
	Report() error
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
