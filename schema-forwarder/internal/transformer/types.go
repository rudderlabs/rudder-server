package transformer

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

type transformer struct {
	cancel context.CancelFunc // cancel function for the Start context (used to stop all goroutines during Stop)
	g      *errgroup.Group    // errgroup for the Start context (used to wait for all goroutines to exit)

	backendConfig backendconfig.BackendConfig
	conf          *config.Config // config used to resolve per-source settings at runtime

	mu                      sync.RWMutex      // protects the following fields
	sourceWriteKeyMap       map[string]string // map of sourceID to writeKey
	newPIIReportingSettings map[string]bool   // map of writeKey to PII protection enabled setting

	keysLimit                 int
	identifierLimit           int
	captureNilAsUnknowns      bool                                // if true, nil values will be captured as unknowns
	collapseArrayIndicesCache map[string]*config.Reloadable[bool] // sourceID -> reloadable "collapse array indices" setting, resolved hierarchically; only accessed by Transform, which the forwarder calls from a single goroutine
}
