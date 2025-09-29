package switcher

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/types"
)

// NewThrottlerSwitcher constructs a new throttler that can switch between two throttlers based on a configuration value.
func NewThrottlerSwitcher(
	useAlternative config.ValueLoader[bool],
	main, alternative types.PickupThrottler,
) types.PickupThrottler {
	return &throttlerSwitcher{
		useAlternative: useAlternative,
		main:           main,
		alternative:    alternative,
	}
}

type throttlerSwitcher struct {
	useAlternative config.ValueLoader[bool]
	main           types.PickupThrottler
	alternative    types.PickupThrottler
}

// CheckLimitReached checks the limit using the currently active throttler.
func (t *throttlerSwitcher) CheckLimitReached(ctx context.Context, cost int64) (limited bool, retErr error) {
	return t.throttler().CheckLimitReached(ctx, cost)
}

// ResponseCodeReceived forwards the response code to both main and alternative throttlers.
func (t *throttlerSwitcher) ResponseCodeReceived(code int) {
	t.main.ResponseCodeReceived(code)
	t.alternative.ResponseCodeReceived(code)
}

// Shutdown stops both main and alternative throttlers.
func (t *throttlerSwitcher) Shutdown() {
	t.main.Shutdown()
	t.alternative.Shutdown()
}

// GetLimitPerSecond returns the limit of the currently active throttler.
func (t *throttlerSwitcher) GetLimitPerSecond() int64 {
	return t.throttler().GetLimitPerSecond()
}

// GetEventType returns the event type of the currently active throttler.
func (t *throttlerSwitcher) GetEventType() string {
	return t.throttler().GetEventType()
}

// GetLastUsed returns the last used time of the currently active throttler.
func (t *throttlerSwitcher) GetLastUsed() time.Time {
	return t.throttler().GetLastUsed()
}

// throttler returns the currently active throttler based on the useAlternative config.
func (t *throttlerSwitcher) throttler() types.PickupThrottler {
	if t.useAlternative.Load() {
		return t.alternative
	}
	return t.main
}
