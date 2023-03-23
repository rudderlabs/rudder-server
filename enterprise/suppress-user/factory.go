package suppression

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	EnterpriseToken string
	Log             logger.Logger
}

func latestDataSeed() (io.Reader, error) {
	return seederSource("latest-export")
}

func fullDataSeed() (io.Reader, error) {
	return seederSource("full-export")
}

func seederSource(endpoint string) (io.Reader, error) {
	client := http.Client{
		Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 600, time.Second),
	}
	baseURL := config.GetString("SUPPRESS_BACKUP_URL", "https://api.rudderstack.com")
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s", baseURL, endpoint), nil)
	if err != nil {
		return nil, fmt.Errorf("could not create request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not perform request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read response body: %w", err)
	}
	return bytes.NewReader(respBody), nil
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
	var latestRepo Repository
	if config.GetBool("BackendConfig.Regulations.useBadgerDB", true) {
		tmpDir, err := misc.CreateTMPDIR()
		if err != nil {
			return nil, fmt.Errorf("could not create tmp dir: %w", err)
		}
		path := path.Join(tmpDir, "latestSuppression")
		latestRepo, err = NewBadgerRepository(
			path,
			m.Log,
			WithSeederSource(latestDataSeed),
			WithMaxSeedWait(config.GetDuration("BackendConfig.Regulations.maxSeedWait", 600, time.Second)))
		if err != nil {
			return nil, fmt.Errorf("could not create badger repository: %w", err)
		}

	} else {
		latestRepo = NewMemoryRepository(m.Log)
	}

	var pollInterval time.Duration
	config.RegisterDurationConfigVariable(300, &pollInterval, true, time.Second, "BackendConfig.Regulations.pollInterval")

	syncer, err := NewSyncer(
		config.GetString("SUPPRESS_USER_BACKEND_URL", "https://api.rudderstack.com"),
		backendConfig.Identity(),
		latestRepo,
		WithLogger(m.Log),
		WithHttpClient(&http.Client{Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 30, time.Second)}),
		WithPageSize(config.GetInt("BackendConfig.Regulations.pageSize", 5000)),
		WithPollIntervalFn(func() time.Duration { return pollInterval }),
	)
	if err != nil {
		return nil, err
	}

	repo := &latestRepo
	subCtx, LatestSyncCancel := context.WithCancel(ctx)
	rruntime.Go(func() {
		syncer.SyncLoop(subCtx)
		err = latestRepo.Stop()
		defer LatestSyncCancel()
	})

	if config.GetBool("BackendConfig.Regulations.useBadgerDB", true) {
		rruntime.Go(func() {
			tmpDir, err := misc.CreateTMPDIR()
			if err != nil {
				m.Log.Error("Complete Synce failed: could not create tmp dir: %w", err)
				return
			}
			path := path.Join(tmpDir, "fullSuppression")
			fullRepo, err := NewBadgerRepository(
				path,
				m.Log,
				WithSeederSource(fullDataSeed),
				WithMaxSeedWait(config.GetDuration("BackendConfig.Regulations.maxSeedWait", 600, time.Second)))
			if err != nil {
				m.Log.Error("Complete Synce failed: could not create badger repository: %w", err)
				return
			}
			syncer, err := NewSyncer(
				config.GetString("SUPPRESS_USER_BACKEND_URL", "https://api.rudderstack.com"),
				backendConfig.Identity(),
				fullRepo,
				WithLogger(m.Log),
				WithHttpClient(&http.Client{Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 30, time.Second)}),
				WithPageSize(config.GetInt("BackendConfig.Regulations.pageSize", 5000)),
				WithPollIntervalFn(func() time.Duration { return pollInterval }),
			)
			if err != nil {
				m.Log.Error("Complete Synce failed: could not create syncer: %w", err)
				return
			}

			rruntime.Go(func() {
				atomic.SwapPointer((*unsafe.Pointer)(unsafe.Pointer(repo)), unsafe.Pointer(&fullRepo))
				LatestSyncCancel()
			})

			syncer.SyncLoop(ctx)
			_ = fullRepo.Stop()
		})
	}

	h := newHandler(*repo, m.Log)

	return h, nil
}
