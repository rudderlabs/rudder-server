package rsources

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

// staticSyncSettingDelegate answers every question the same way.
//
// The SyncSettingDelegate interface takes an unexported type on purpose, so the only
// real implementation is the one in this package; this is the stand-in for the two
// cases that do not need it - tests, and the collectors that never collect failed
// records at all.
type staticSyncSettingDelegate struct {
	errorResponse string
	err           error
}

// NewStaticSyncSettingDelegate returns a SyncSettingDelegate that always answers with
// errorResponse and err. Intended for tests.
func NewStaticSyncSettingDelegate(errorResponse string, err error) SyncSettingDelegate {
	return &staticSyncSettingDelegate{errorResponse: errorResponse, err: err}
}

// NewUnsupportedSyncSettingDelegate returns the delegate every collector starts with
// until WithSyncSettingDelegate replaces it.
//
// The gateway's collectors and the processor's only ever report Stats, so requiring
// them to name a delegate would have meant either a compile-time parameter at every
// call site or a real component - a database connection, a table and a cleanup routine
// - in processes (the gateway-only pod) that have no use for any of it. Instead they
// build a collector the ordinary way and get this.
//
// It fails loud rather than quiet: if such a collector ever does reach
// CollectFailedRecords, the very first aborted rETL record returns this error, naming
// the component, and the strict propagation path aborts the batch - instead of
// publishing durable failed records with a silently empty error_response.
func NewUnsupportedSyncSettingDelegate(component string) SyncSettingDelegate {
	return &staticSyncSettingDelegate{
		err: fmt.Errorf(
			"the %q stats collector was built without a sync setting delegate and cannot collect failed records",
			component,
		),
	}
}

func (s *staticSyncSettingDelegate) GetErrorResponse(_ context.Context, _ statKey, _ *jobsdb.JobStatusT) (string, error) {
	return s.errorResponse, s.err
}
