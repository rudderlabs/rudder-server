package stats

type statsConfig struct {
	enabled             bool
	serviceName         string
	serviceVersion      string
	instanceName        string
	namespaceIdentifier string
	excludedTags        map[string]struct{}

	periodicStatsConfig periodicStatsConfig
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
