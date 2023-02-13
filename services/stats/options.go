package stats

import (
	"time"
)

type statsConfig struct {
	enabled        bool
	serviceName    string
	serviceVersion string
	instanceName   string
	excludedTags   map[string]struct{}

	namespaceIdentifier     string
	runtimeEnabled          bool
	statsCollectionInterval int64
	enableCPUStats          bool
	enableMemStats          bool
	enableGCStats           bool

	otelConfig otelStatsConfig
}

type otelStatsConfig struct {
	tracesEndpoint        string
	tracingSamplingRate   float64
	metricsEndpoint       string
	metricsExportInterval time.Duration
}

type Option func(*statsConfig)

// WithServiceName sets the service name for the stats service.
func WithServiceName(name string) Option {
	return func(c *statsConfig) {
		c.serviceName = name
	}
}

// WithServiceVersion sets the service version for the stats service.
func WithServiceVersion(version string) Option {
	return func(c *statsConfig) {
		c.serviceVersion = version
	}
}

// WithInstanceName sets the instance name for the stats service.
func WithInstanceName(name string) Option {
	return func(c *statsConfig) {
		c.instanceName = name
	}
}

// WithNamespace sets the namespace identifier for the stats service.
func WithNamespace(namespace string) Option {
	return func(c *statsConfig) {
		c.namespaceIdentifier = namespace
	}
}
