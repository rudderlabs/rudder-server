package suppression

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	EnterpriseToken string
	Log             logger.Logger
}

func latestDataSeed() (io.ReadCloser, error) {
	return seederSource("latest-export")
}

func fullDataSeed() (io.ReadCloser, error) {
	return seederSource("full-export")
}

func seederSource(endpoint string) (io.ReadCloser, error) {
	client := http.Client{}
	baseURL := config.GetString("SUPPRESS_BACKUP_URL", "https://api.rudderstack.com")
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s", baseURL, endpoint), http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("could not create request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not perform request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	// close body afterwards.
	return resp.Body, nil
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
			WithSeederSource(latestDataSeed))
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

	// repo := &latestRepo
	subCtx, latestSyncCancel := context.WithCancel(ctx)
	rruntime.Go(func() {
		syncer.SyncLoop(subCtx)
		err = latestRepo.Stop()
		defer latestSyncCancel()
	})
	repo := &RepoHolder{
		Repository: latestRepo,
		mu:         sync.RWMutex{},
	}
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
				WithSeederSource(fullDataSeed))
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
			if err = syncer.Sync(ctx); err != nil {
				m.Log.Error("Complete Synce failed: could not sync: %w", err)
			}
			latestSyncCancel()
			repo.Switcher(fullRepo)
			syncer.SyncLoop(ctx)
			_ = fullRepo.Stop()
		})
	}

	h := newHandler(repo, m.Log)

	return h, nil
}

type RepoHolder struct {
	Repository
	mu sync.RWMutex
}

func (rh *RepoHolder) Stop() error {
	rh.mu.Lock()
	defer rh.mu.Unlock()
	return rh.Repository.Stop()
}

func (rh *RepoHolder) GetToken() ([]byte, error) {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return rh.Repository.GetToken()
}

func (rh *RepoHolder) Add(suppressions []model.Suppression, token []byte) error {
	rh.mu.Lock()
	defer rh.mu.Unlock()
	return rh.Repository.Add(suppressions, token)
}

func (rh *RepoHolder) Suppressed(workspaceID, userID, sourceID string) (bool, error) {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return rh.Repository.Suppressed(workspaceID, userID, sourceID)
}

func (rh *RepoHolder) Backup(w io.Writer) error {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return rh.Repository.Backup(w)
}

func (rh *RepoHolder) Restore(r io.Reader) error {
	rh.mu.Lock()
	defer rh.mu.Unlock()
	return rh.Repository.Restore(r)
}

func (rh *RepoHolder) Switcher(newRepo Repository) error {
	rh.mu.Lock()
	defer rh.mu.Unlock()
	rh.Repository = newRepo
	return nil
}
