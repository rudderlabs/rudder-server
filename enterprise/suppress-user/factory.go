package suppression

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
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
	var pollInterval time.Duration
	config.RegisterDurationConfigVariable(300, &pollInterval, true, time.Second, "BackendConfig.Regulations.pollInterval")

	var latestRepo Repository
	var repo *RepoSwitcher
	if config.GetBool("BackendConfig.Regulations.useBadgerDB", true) {
		fullSuppressionPath, latestSuppressionPath, err := getRepoPath()
		if err != nil {
			return nil, fmt.Errorf("could not get repo path: %w", err)
		}
		if _, err = os.Stat(fullSuppressionPath); err != nil && os.IsNotExist(err) {
			// start NewBadergerRepository with latestSuppressionPath
			latestRepo, err = NewBadgerRepository(
				latestSuppressionPath,
				m.Log,
				WithSeederSource(latestDataSeed))
			if err != nil {
				return nil, fmt.Errorf("could not create badger repository: %w", err)
			}
			// start syncLoop of latest repo
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
			subCtx, latestSyncCancel := context.WithCancel(ctx)
			rruntime.Go(func() {
				syncer.SyncLoop(subCtx)
				err = latestRepo.Stop()
				defer latestSyncCancel()
			})
			tmp := RepoSwitcher{
				Repository: latestRepo,
				mu:         sync.RWMutex{},
			}
			repo = &tmp
			// in a go routine
			rruntime.Go(func() {
				// create NewBadgerRepository with fullSuppressionPath
				fullRepo, err := NewBadgerRepository(
					fullSuppressionPath,
					m.Log,
					WithSeederSource(fullDataSeed))
				if err != nil {
					m.Log.Error("Complete Synce failed: could not create badger repository: %w", err)
					return
				}
				// create syncer with fullRepo
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
				// complete it's 1st sync
				if err = syncer.Sync(ctx); err != nil {
					m.Log.Error("Complete Synce failed: could not sync: %w", err)
				}
				// stop latest repo sync loop
				latestSyncCancel()
				// switch repo
				repo.Switch(fullRepo)
				// start syncLoop of full repo
				syncer.SyncLoop(ctx)
				_ = fullRepo.Stop()
			})
		} else {
			// start NewBadgerRepository with fullSuppressionPath
			fullRepo, err := NewBadgerRepository(
				fullSuppressionPath,
				m.Log)
			if err != nil {
				m.Log.Error("Complete Synce failed: could not create badger repository: %w", err)
				return nil, err
			}

			// start sync of this repo
			// start syncLoop of latest repo
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
				return nil, err
			}
			rruntime.Go(func() {
				syncer.SyncLoop(ctx)
				_ = fullRepo.Stop()
			})
		}
	} else {
		memoryRepo := NewMemoryRepository(m.Log)
		syncer, err := NewSyncer(
			config.GetString("SUPPRESS_USER_BACKEND_URL", "https://api.rudderstack.com"),
			backendConfig.Identity(),
			memoryRepo,
			WithLogger(m.Log),
			WithHttpClient(&http.Client{Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 30, time.Second)}),
			WithPageSize(config.GetInt("BackendConfig.Regulations.pageSize", 5000)),
			WithPollIntervalFn(func() time.Duration { return pollInterval }),
		)
		if err != nil {
			return nil, err
		}
		rruntime.Go(func() {
			syncer.SyncLoop(ctx)
			err = memoryRepo.Stop()
		})
		h := newHandler(memoryRepo, m.Log)

		return h, nil
	}
	h := newHandler(repo, m.Log)
	return h, nil
}

func getRepoPath() (fullSuppressionPath, latestSuppressionPath string, err error) {
	tmpDir, err := misc.CreateTMPDIR()
	if err != nil {
		return "", "", fmt.Errorf("could not create tmp dir: %w", err)
	}
	fullSuppressionPath = path.Join(tmpDir, "fullSuppression")
	latestSuppressionPath = path.Join(tmpDir, "latestSuppression")
	return
}

type RepoSwitcher struct {
	Repository
	mu sync.RWMutex
}

func (rh *RepoSwitcher) Stop() error {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return rh.Repository.Stop()
}

func (rh *RepoSwitcher) GetToken() ([]byte, error) {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return rh.Repository.GetToken()
}

func (rh *RepoSwitcher) Add(suppressions []model.Suppression, token []byte) error {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return rh.Repository.Add(suppressions, token)
}

func (rh *RepoSwitcher) Suppressed(workspaceID, userID, sourceID string) (bool, error) {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return rh.Repository.Suppressed(workspaceID, userID, sourceID)
}

func (rh *RepoSwitcher) Backup(w io.Writer) error {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return rh.Repository.Backup(w)
}

func (rh *RepoSwitcher) Restore(r io.Reader) error {
	rh.mu.RLock()
	defer rh.mu.RUnlock()
	return rh.Repository.Restore(r)
}

func (rh *RepoSwitcher) Switch(newRepo Repository) error {
	rh.mu.Lock()
	defer rh.mu.Unlock()
	rh.Repository = newRepo
	return nil
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
