package schematransformer

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

type SchemaTransformer struct {
	cancel                     context.CancelFunc
	g                          *errgroup.Group
	backendConfig              backendconfig.BackendConfig
	sourceWriteKeyMap          map[string]string
	newPIIReportingSettings    map[string]bool
	writeKeyMapLock            sync.RWMutex
	config                     *config.Config
	shouldCaptureNilAsUnknowns bool
	initialisedOnce            sync.Once
	initialised                chan struct{}
}
