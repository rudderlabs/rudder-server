package metric

import "fmt"

// Measurement acts as a key in a Registry.
type Measurement interface {
	// GetName gets the name of the measurement
	GetName() string
	// GetTags gets the tags of the measurement
	GetTags() map[string]string
}

// GetPendingEventsMeasurement gets the measurement for pending events metric
func GetPendingEventsMeasurement(tablePrefix string, workspace string, destType string) Gauge {
	return GetManager().GetRegistry(PUBLISHED_METRICS).MustGetGauge(pendingEventsMeasurement{tablePrefix, workspace, destType})
}

type pendingEventsMeasurement struct {
	tablePrefix string
	workspace   string
	destType    string
}

func (r pendingEventsMeasurement) GetName() string {
	return fmt.Sprintf("%s_pending_events", r.tablePrefix)
}
func (r pendingEventsMeasurement) GetTags() map[string]string {
	res := map[string]string{
		"workspace": r.workspace,
		"destType":  r.destType,
	}
	return res
}
