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
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

type Factory struct {
	EnterpriseToken string
	Log             logger.Logger
}

func (m *Factory) NewSyncerWithBadgerRepo(repoPath string, seederSource func() (io.ReadCloser, error), identity identity.Identifier, pollInterval time.Duration) (*Syncer, Repository, error) {
	repo, err := NewBadgerRepository(
		repoPath,
		m.Log,
		WithSeederSource(seederSource))
	if err != nil {
		return nil, nil, fmt.Errorf("could not create badger repository: %w", err)
	}
	syncer, err := NewSyncer(
		config.GetString("SUPPRESS_USER_BACKUP_SERVICE_URL", "https://api.rudderstack.com"),
		identity,
		repo,
		WithLogger(m.Log),
		WithHttpClient(&http.Client{Timeout: config.GetDuration("HttpClient.suppressUser.timeout", 30, time.Second)}),
		WithPageSize(config.GetInt("BackendConfig.Regulations.pageSize", 5000)),
		WithPollIntervalFn(func() time.Duration { return pollInterval }),
	)
	if err != nil {
		return nil, nil, err
	}
	return syncer, repo, nil
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

	useBadgerDB := config.GetBool("BackendConfig.Regulations.useBadgerDB", true)
	if useBadgerDB {
		identity := backendConfig.Identity()

		fullSuppressionPath, latestSuppressionPath, err := getRepoPath()
		if err != nil {
			return nil, fmt.Errorf("could not get repo path: %w", err)
		}

		if _, err = os.Stat(fullSuppressionPath); os.IsNotExist(err) && config.IsSet("SUPPRESS_USER_BACKUP_SERVICE_URL") {
			repo := &RepoSwitcher{
				mu: sync.RWMutex{},
			}

			latestSyncer, latestRepo, err := m.NewSyncerWithBadgerRepo(latestSuppressionPath, latestDataSeed, identity, pollInterval)
			if err != nil {
				return nil, err
			}

			subCtx, latestSyncCancel := context.WithCancel(ctx)
			rruntime.Go(func() {
				latestSyncer.SyncLoop(subCtx)
				err = latestRepo.Stop()
				if err != nil {
					m.Log.Error("Latest Sync failed: could not stop repo: %w", err)
				}
				err = os.RemoveAll(latestSuppressionPath)
				if err != nil {
					m.Log.Error("Latest Sync failed: could not remove repo: %w", err)
				}
			})
			repo.Repository = latestRepo
			rruntime.Go(func() {
				fullSyncer, fullRepo, err := m.NewSyncerWithBadgerRepo(fullSuppressionPath, fullDataSeed, identity, pollInterval)
				if err != nil {
					m.Log.Error("Complete Synce failed: could not create syncer: %w", err)
					return
				}
				if err = fullSyncer.Sync(ctx); err != nil {
					m.Log.Error("Complete Synce failed: could not sync: %w", err)
				}
				err = repo.Switch(fullRepo)
				if err != nil {
					m.Log.Error("Complete Synce failed: could not switch repo: %w", err)
				}
				latestSyncCancel()
				fullSyncer.SyncLoop(ctx)
				_ = fullRepo.Stop()
			})
			h := newHandler(repo, m.Log)
			return h, nil
		} else {
			var seederSource func() (io.ReadCloser, error)
			syncer, fullRepo, err := m.NewSyncerWithBadgerRepo(fullSuppressionPath, seederSource, identity, pollInterval)
			if err != nil {
				return nil, err
			}
			rruntime.Go(func() {
				syncer.SyncLoop(ctx)
				_ = fullRepo.Stop()
			})
			h := newHandler(fullRepo, m.Log)
			return h, nil
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
