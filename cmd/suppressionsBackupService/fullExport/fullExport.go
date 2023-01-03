package fullExport

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func SetupSync(
	ctx context.Context,
	backendConfig backendconfig.BackendConfig,
) error {
	log := logger.NewLogger().Child("fullExport")
	log.Info("setting up badgerdb repository")

	tmpDir, err := misc.CreateTMPDIR()
	if err != nil {
		return fmt.Errorf("could not create tmp dir: %w", err)
	}
	path := path.Join(tmpDir, "suppression")
	repository, err := suppression.NewBadgerRepository(
		path,
		log,
		suppression.WithMaxSeedWait(
			config.GetDuration(
				"BackendConfig.Regulations.maxSeedWait",
				5,
				time.Second,
			),
		),
	)
	if err != nil {
		return fmt.Errorf("could not create badger repository: %w", err)
	}

	var pollInterval time.Duration
	config.RegisterDurationConfigVariable(300, &pollInterval, true, time.Second, "BackendConfig.Regulations.pollInterval")

	syncer, err := suppression.NewSyncer(
		config.GetString("SUPPRESS_USER_BACKEND_URL", "https://api.rudderstack.com"),
		backendConfig.Identity(),
		repository,
		suppression.WithLogger(log),
		suppression.WithHttpClient(
			&http.Client{
				Timeout: config.GetDuration(
					"HttpClient.suppressUser.timeout",
					30,
					time.Second,
				),
			},
		),
		suppression.WithPageSize(config.GetInt("BackendConfig.Regulations.pageSize", 5000)),
		suppression.WithPollIntervalFn(func() time.Duration { return pollInterval }),
	)
	if err != nil {
		return fmt.Errorf("could not create syncer: %w", err)
	}

	return nil
}
