package sftp

import (
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/sftp"
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
	Port       string `json:"port"`
	Password   string `json:"password"`
	PrivateKey string `json:"privateKey"`
	FileFormat string `json:"fileFormat"`
	FilePath   string `json:"filePath"`
}

// Record represents a single JSON record.
type record map[string]interface{}
