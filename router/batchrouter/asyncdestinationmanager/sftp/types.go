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

type destConfig struct {
	AuthMethod string `json:"authMethod"`
	Username   string `json:"username"`
	Host       string `json:"host"`
	Port       int    `json:"port,string"`
	Password   string `json:"password,omitempty"`
	PrivateKey string `json:"privateKey,omitempty"`
}

// Record represents a single JSON record.
type record map[string]interface{}
