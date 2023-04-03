package suppression

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
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

		if !alreadySeeded(fullSuppressionPath) && config.IsSet("SUPPRESS_USER_BACKUP_SERVICE_URL") {
			repo := &RepoSwitcher{
				mu: sync.RWMutex{},
			}
			// First starting a repository seeded with the latest data which is faster to load
			latestSyncer, latestRepo, err := m.newSyncerWithBadgerRepo(latestSuppressionPath, latestDataSeed, config.GetDuration("BackendConfig.Regulations.maxSeedWait", 5, time.Second), identity, pollInterval)
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
				var fullSyncer *Syncer
				var fullRepo Repository
				var err error

				m.retry(ctx,
					func() error {
						fullSyncer, fullRepo, err = m.newSyncerWithBadgerRepo(fullSuppressionPath, fullDataSeed, 0, identity, pollInterval)
						return err
					}, 5*time.Second)

				m.retry(ctx,
					func() error { return fullSyncer.Sync(ctx) },
					5*time.Second)
				_, err = os.Create(filepath.Join(fullSuppressionPath, model.SyncDoneMarker))
				if err != nil {
					m.Log.Error("Could not create sync done marker: %w", err)
				}
				repo.Switch(fullRepo)
				latestSyncCancel()
				fullSyncer.SyncLoop(ctx)
				_ = fullRepo.Stop()
			})
			return newHandler(repo, m.Log), nil
		} else {
			syncer, fullRepo, err := m.newSyncerWithBadgerRepo(fullSuppressionPath, nil, 0, identity, pollInterval)
			if err != nil {
				return nil, err
			}
			rruntime.Go(func() {
				syncer.SyncLoop(ctx)
				_ = fullRepo.Stop()
			})
			return newHandler(fullRepo, m.Log), nil
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

func alreadySeeded(repoPath string) bool {
	_, err := os.Stat(repoPath)
	if os.IsNotExist(err) {
		return false
	}
	_, err = os.Stat(path.Join(repoPath, model.SyncDoneMarker))
	if os.IsNotExist(err) {
		os.RemoveAll(repoPath)
		return false
	}
	return true
}

func (m *Factory) retry(ctx context.Context, f func() error, wait time.Duration) {
	var err error
	for {
		err = f()
		if err == nil {
			return
		}
		m.Log.Errorf("retry failed: %v", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (m *Factory) newSyncerWithBadgerRepo(repoPath string, seederSource func() (io.ReadCloser, error), maxSeedWaitTime time.Duration, identity identity.Identifier, pollInterval time.Duration) (*Syncer, Repository, error) {
	repo, err := NewBadgerRepository(
		repoPath,
		m.Log,
		WithSeederSource(seederSource),
		WithMaxSeedWait(maxSeedWaitTime),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create badger repository: %w", err)
	}
	syncer, err := NewSyncer(
		config.GetString("SUPPRESS_USER_BACKEND_URL", "https://api.rudderstack.com"),
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

func getRepoPath() (fullSuppressionPath, latestSuppressionPath string, err error) {
	tmpDir, err := misc.CreateTMPDIR()
	if err != nil {
		return "", "", fmt.Errorf("could not create tmp dir: %w", err)
	}
	fullSuppressionPath = path.Join(tmpDir, "fullSuppression")
	latestSuppressionPath = path.Join(tmpDir, "latestSuppression")
	return
}

func latestDataSeed() (io.ReadCloser, error) {
	return seederSource("latest-export")
}

func fullDataSeed() (io.ReadCloser, error) {
	return seederSource("full-export")
}

func seederSource(endpoint string) (io.ReadCloser, error) {
	client := http.Client{}
	baseURL := config.GetString("SUPPRESS_USER_BACKUP_SERVICE_URL", "https://api.rudderstack.com")
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
