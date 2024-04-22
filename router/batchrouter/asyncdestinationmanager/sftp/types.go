package sftp

import (
	"github.com/rudderlabs/rudder-go-kit/logger"
	sftp "github.com/rudderlabs/rudder-go-kit/sftp"
)

// DefaultManager is the default manager for SFTP
type DefaultManager struct {
	FileManager sftp.FileManager
	logger      logger.Logger
}

// Record represents a single JSON record.
type Record map[string]interface{}
