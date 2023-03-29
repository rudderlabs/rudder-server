package payload

import (
	"github.com/rudderlabs/rudder-go-kit/logger"
)

// FreeMemory is a function that returns the free memory in percentage (0-100)
type FreeMemory func() (float64, error)

type AdaptiveLimiterConfig struct {
	// FreeMemThresholdLimit is the threshold for the free memory in percentage. Default is 30%
	FreeMemThresholdLimit float64
	// CriticalFreeMemory is the critical threshold for the free memory in percentage. Default is 10%
	FreeMemCriticalLimit float64
	// MaxThresholdFactor is the maximum threshold factor. Values 1-9 are valid. Default is 9
	MaxThresholdFactor int
	// FreeMemory is the function to use for getting the free memory in percentage. Default implementation will be used if not provided
	FreeMemory FreeMemory
	// Log
	Log logger.Logger
}

func (c *AdaptiveLimiterConfig) parse() {
	if c.FreeMemThresholdLimit == 0 {
		c.FreeMemThresholdLimit = 30
	}
	if c.FreeMemCriticalLimit == 0 {
		c.FreeMemCriticalLimit = 10
	}
	if c.MaxThresholdFactor < 1 || c.MaxThresholdFactor > 9 {
		c.MaxThresholdFactor = 9
	}
	if c.Log == nil {
		c.Log = logger.NewLogger().Child("payload-limiter")
	}
	if c.FreeMemory == nil {
		c.FreeMemory = func() (float64, error) { return 100.0, nil }
	}
}
