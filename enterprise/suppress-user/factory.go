package suppression

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	EnterpriseToken string
	Log             logger.Logger
}

// Setup initializes the user suppression feature
func (m *Factory) Setup(ctx context.Context, backendConfig backendconfig.BackendConfig) (types.UserSuppression, error) {
	if m.Log == nil {
		m.Log = logger.NewLogger().Child("enterprise").Child("suppress-user")
	}
	if m.EnterpriseToken == "" {
		m.Log.Info("Suppress User feature is enterprise only")
		return &NOOP{}, nil
	}
	m.Log.Info("Setting up Suppress User Feature")
	backendConfig.WaitForConfig(ctx)
	var repository Repository
	if config.GetBool("BackendConfig.Regulations.useBadgerDB", true) {
		tmpDir, err := misc.CreateTMPDIR()
		if err != nil {
			return nil, fmt.Errorf("could not create tmp dir: %w", err)
		}
		path := path.Join(tmpDir, "suppression")

		// TODO: implement seeder source, to retrieve the initial state from a persisted backup
		var seederSource func() (io.Reader, error)

		repository, err = NewBadgerRepository(
			path,
			m.Log,
			WithSeederSource(seederSource),
			WithMaxSeedWait(config.GetDuration("BackendConfig.Regulations.maxSeedWait", 5, time.Second)))
		if err != nil {
			return nil, fmt.Errorf("could not create badger repository: %w", err)
		}
	} else {
		repository = NewMemoryRepository(m.Log)
	}

	var pollInterval time.Duration
	config.RegisterDurationConfigVariable(300, &pollInterval, true, time.Second, "BackendConfig.Regulations.pollInterval")

	syncer, err := NewSyncer(
		config.GetString("SUPPRESS_USER_BACKEND_URL", "https://api.rudderstack.com"),
		backendConfig.Identity(),
		repository,
		WithLogger(m.Log),
		WithHttpClient(&http.Client{Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 30, time.Second)}),
		WithPageSize(config.GetInt("BackendConfig.Regulations.pageSize", 5000)),
		WithPollIntervalFn(func() time.Duration { return pollInterval }),
	)
	if err != nil {
		return nil, err
	}

	h := newHandler(repository, m.Log)

	rruntime.Go(func() {
		syncer.SyncLoop(ctx)
		_ = repository.Stop()
	})

	return h, nil
}
