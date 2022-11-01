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
}

// New initializes Suppress User feature
func (m *Factory) Setup(ctx context.Context, backendConfig backendconfig.BackendConfig) (types.UserSuppression, error) {
	log := logger.NewLogger().Child("enterprise").Child("suppress-user")
	if m.EnterpriseToken == "" {
		log.Info("Suppress User feature is enterprise only")
		return &NOOP{}, nil
	}
	log.Info("Setting up Suppress User Feature")
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
			log,
			WithSeederSource(seederSource),
			WithMaxSeedWait(config.GetDuration("BackendConfig.Regulations.maxSeedWait", 5, time.Second)))
		if err != nil {
			return nil, fmt.Errorf("could not create badger repository: %w", err)
		}
	} else {
		repository = NewMemoryRepository(log)
	}

	var pollInterval time.Duration
	config.RegisterDurationConfigVariable(300, &pollInterval, true, time.Second, "BackendConfig.Regulations.pollInterval")

	syncer, err := NewSyncer(
		config.GetString("SUPPRESS_USER_BACKEND_URL", "https://api.rudderlabs.com"),
		backendConfig.Identity(),
		repository,
		WithLogger(log),
		WithHttpClient(&http.Client{Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 30, time.Second)}),
		WithPageSize(config.GetInt("BackendConfig.Regulations.pageSize", 5000)),
		WithPollIntervalFn(func() time.Duration { return pollInterval }),
	)
	if err != nil {
		return nil, err
	}

	h := &handler{
		log: log,
		r:   repository,
	}

	rruntime.Go(func() {
		syncer.SyncLoop(ctx)
		_ = repository.Stop()
	})

	return h, nil
}
