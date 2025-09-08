package observability

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/shared"
	. "github.com/rudderlabs/rudder-server/utils/tx"
	rt "github.com/rudderlabs/rudder-server/utils/types"
)

const (
	DefaultMetrics      string = "default_metrics"
	MTUMetrics          string = "mtu_metrics"
	ErrorDetailsMetrics string = "error_details_metrics"
	DataMapperMetrics   string = "data_mapper_metrics"
	LiveEvents          string = "live_events"
)

// ObserverMediator acts as a mediator for multiple Observer implementations
type ObserverMediator struct {
	observers map[string]Observer
}

// NewObserverMediator creates a new mediator with enabled observers based on configuration
func NewObserverMediator(stage rt.StageDetails, inputEvents []*shared.InputEvent, config *config.Config) *ObserverMediator {
	observers := make(map[string]Observer)

	// Create observers based on configuration
	if config != nil {
		// if config.GetBool("Observability.DefaultMetrics.Enabled", false) {
		//     observers[DefaultMetrics] = NewDefaultObserver(stage, inputEvents, config)
		// }
	}

	return &ObserverMediator{
		observers: observers,
	}
}

// ObserveInputEvents implements Observer.ObserveInputEvents
func (m *ObserverMediator) ObserveInputEvents(events ...*shared.InputEvent) error {
	for observerType, observer := range m.observers {
		if err := observer.ObserveInputEvents(events...); err != nil {
			return fmt.Errorf("observer %s input observation failed: %w", observerType, err)
		}
	}

	return nil
}

// ObserveOutputEvents implements Observer.ObserveOutputEvents
func (m *ObserverMediator) ObserveOutputEvents(events ...*shared.OutputEvent) error {
	for observerType, observer := range m.observers {
		if err := observer.ObserveOutputEvents(events...); err != nil {
			return fmt.Errorf("observer %s output observation failed: %w", observerType, err)
		}
	}

	return nil
}

// NextStage implements Observer.NextStage
func (m *ObserverMediator) NextStage(stage rt.StageDetails) error {
	for observerType, observer := range m.observers {
		if err := observer.NextStage(stage); err != nil {
			return fmt.Errorf("observer %s next stage failed: %w", observerType, err)
		}
	}

	return nil
}

// End implements Observer.End
func (m *ObserverMediator) End() error {
	for observerType, observer := range m.observers {
		if err := observer.End(); err != nil {
			return fmt.Errorf("observer %s end failed: %w", observerType, err)
		}
	}

	return nil
}

// Merge implements Observer.Merge
func (m *ObserverMediator) Merge(other Observer) error {
	otherMediator, ok := other.(*ObserverMediator)
	if !ok {
		return fmt.Errorf("cannot merge different mediator types")
	}

	// Merge observers - call merge on all observers in this mediator
	// The individual observers will handle the case where the other observer is nil
	for observerType, observer := range m.observers {
		var otherObserver Observer
		if obs, exists := otherMediator.observers[observerType]; exists {
			otherObserver = obs
		}
		// otherObserver will be nil if it doesn't exist in the other mediator
		if err := observer.Merge(otherObserver); err != nil {
			return fmt.Errorf("failed to merge observer %s: %w", observerType, err)
		}
	}

	return nil
}

// Flush implements Observer.Flush
func (m *ObserverMediator) Flush(ctx context.Context, tx *Tx) error {
	for observerType, observer := range m.observers {
		if err := observer.Flush(ctx, tx); err != nil {
			return fmt.Errorf("observer %s flush failed: %w", observerType, err)
		}
	}

	return nil
}

// GetObserver returns the observer of the specified type, if it exists
func (m *ObserverMediator) GetObserver(observerType string) (Observer, bool) {
	observer, exists := m.observers[observerType]
	return observer, exists
}

// IsObserverEnabled checks if an observer of the specified type is enabled
func (m *ObserverMediator) IsObserverEnabled(observerType string) bool {
	_, exists := m.observers[observerType]
	return exists
}

// GetEnabledObserverTypes returns a slice of all enabled observer types
func (m *ObserverMediator) GetEnabledObserverTypes() []string {
	types := make([]string, 0, len(m.observers))
	for observerType := range m.observers {
		types = append(types, observerType)
	}
	return types
}

// GetObserverCount returns the number of enabled observers
func (m *ObserverMediator) GetObserverCount() int {
	return len(m.observers)
}
