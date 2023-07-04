package suppression

import (
	"io"
	"sync"

	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
)

// RepoSwitcher is a repository that can be be used to switch repository at runtime
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

func (rh *RepoSwitcher) Suppressed(workspaceID, userID, sourceID string) (*model.Metadata, error) {
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

func (rh *RepoSwitcher) Switch(newRepo Repository) {
	rh.mu.Lock()
	defer rh.mu.Unlock()
	rh.Repository = newRepo
}
