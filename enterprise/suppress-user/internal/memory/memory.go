package memory

import (
	"io"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
)

// Repository is a repository backed by memory.

type Repository struct {
	log            logger.Logger
	token          []byte
	suppressionsMu sync.RWMutex
	suppressions   map[string]map[string]map[string]model.Metadata
}

// NewRepository returns a new repository backed by memory.
func NewRepository(log logger.Logger) *Repository {
	m := &Repository{
		log:          log,
		suppressions: make(map[string]map[string]map[string]model.Metadata),
	}
	return m
}

// GetToken returns the current token
func (m *Repository) GetToken() ([]byte, error) {
	return m.token, nil
}

// Suppressed returns true if the given user is suppressed, false otherwise
func (m *Repository) Suppressed(workspaceID, userID, sourceID string) (*model.Metadata, error) {
	m.suppressionsMu.RLock()
	defer m.suppressionsMu.RUnlock()
	workspace, ok := m.suppressions[workspaceID]
	if !ok {
		return nil, model.ErrKeyNotFound
	}
	sourceIDs, ok := workspace[userID]
	if !ok {
		return nil, model.ErrKeyNotFound
	}
	if metadata, ok := sourceIDs[model.Wildcard]; ok {
		return &metadata, nil
	}
	if metadata, ok := sourceIDs[sourceID]; ok {
		return &metadata, nil
	}
	return nil, model.ErrKeyNotFound
}

// Add adds the given suppressions to the repository
func (m *Repository) Add(suppressions []model.Suppression, token []byte) error {
	m.suppressionsMu.Lock()
	defer m.suppressionsMu.Unlock()
	for i := range suppressions {
		suppression := suppressions[i]
		var keys []string
		if len(suppression.SourceIDs) == 0 {
			keys = []string{model.Wildcard}
		} else {
			keys = make([]string, len(suppression.SourceIDs))
			copy(keys, suppression.SourceIDs)
		}
		workspace, ok := m.suppressions[suppression.WorkspaceID]
		if !ok {
			workspace = make(map[string]map[string]model.Metadata)
			m.suppressions[suppression.WorkspaceID] = workspace
		}
		user, ok := workspace[suppression.UserID]
		if !ok {
			user = make(map[string]model.Metadata)
			m.suppressions[suppression.WorkspaceID][suppression.UserID] = user
		}
		if suppression.Canceled {
			for _, key := range keys {
				delete(user, key)
			}
		} else {
			for _, key := range keys {
				user[key] = model.Metadata{
					CreatedAt: suppression.CreatedAt,
				}
			}
		}
	}
	m.token = token
	return nil
}

// Stop is a no-op for the memory repository.
func (*Repository) Stop() error {
	return nil
}

// Backup is not supported for the memory repository.
func (*Repository) Backup(_ io.Writer) error {
	return model.ErrNotSupported
}

// Restore is not supported for the memory repository.
func (*Repository) Restore(_ io.Reader) error {
	return model.ErrNotSupported
}
