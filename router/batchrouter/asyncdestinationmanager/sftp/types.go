package sftp

import (
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/sftp"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

// defaultManager is the default manager for SFTP
type defaultManager struct {
	logger         logger.Logger
	statsFactory   stats.Stats
	FileManager    sftp.FileManager
	filePathPrefix string
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
/*
{
    "action": "insert",
    "channel": "sources",
    "context": {
        "destinationFields": "identifier, C_NAME, C_EMAIL",
        "externalId": [
            {
                "identifierType": "identifier",
                "type": "SFTP-record"
            }
        ],
        "mappedToDestination": "true"
    },
    "fields": {
        "C_NAME": "john doe",
		"C_EMAIL": "john.doe@gmail.com",
        "identifier": "e440921e-967c-40d0-abb2-8e0090dfc9ff"
    },
    "messageId": "d073ab8b-3393-448c-82ca-c43bf8631fac",
    "recordId": "1",
    "rudderId": "853ae90f-0351-424b-973e-a615e6487517",
    "type": "record"
}
*/
type record map[string]any
