package tunnelling

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	stunnel "github.com/rudderlabs/sql-tunnels/driver/ssh"
)

var (
	ErrMissingKey     = errors.New("missing mandatory key")
	ErrUnexpectedType = errors.New("unexpected type")
)

const (
	sshUser       = "sshUser"
	sshPort       = "sshPort"
	sshHost       = "sshHost"
	sshPrivateKey = "sshPrivateKey"
)

type (
	Config     map[string]interface{}
	TunnelInfo struct {
		Config Config
	}
)

// ExtractTunnelInfoFromDestinationConfig extracts TunnelInfo from destination config if tunnel is enabled for the destination.
func ExtractTunnelInfoFromDestinationConfig(config Config) *TunnelInfo {
	if tunnelEnabled := whutils.ReadAsBool("useSSH", config); !tunnelEnabled {
		return nil
	}

	return &TunnelInfo{
		Config: config,
	}
}

// Connect establishes a database connection over an SSH tunnel.
func Connect(dsn string, config Config) (*sql.DB, error) {
	tunnelConfig, err := extractTunnelConfig(config)
	if err != nil {
		return nil, fmt.Errorf("reading ssh tunnel config: %w", err)
	}

	encodedDSN, err := tunnelConfig.EncodeWithDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("encoding with dsn: %w", err)
	}

	db, err := sql.Open("sql+ssh", encodedDSN)
	if err != nil {
		return nil, fmt.Errorf("opening warehouse connection sql+ssh driver: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("pinging warehouse connection: %w", err)
	}

	return db, nil
}

func extractTunnelConfig(config Config) (*stunnel.Config, error) {
	var user, host, port, privateKey *string
	var err error

	if user, err = readString(sshUser, config); err != nil {
		return nil, err
	}
	if host, err = readString(sshHost, config); err != nil {
		return nil, err
	}
	if port, err = readString(sshPort, config); err != nil {
		return nil, err
	}
	if privateKey, err = readString(sshPrivateKey, config); err != nil {
		return nil, err
	}

	portInt, err := strconv.Atoi(*port)
	if err != nil {
		return nil, err
	}

	return &stunnel.Config{
		User:       *user,
		Host:       *host,
		PrivateKey: []byte(*privateKey),
		Port:       portInt,
	}, nil
}

func readString(key string, config Config) (*string, error) {
	val, ok := config[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMissingKey, key)
	}

	resp, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("%w: %s expected string", ErrUnexpectedType, key)
	}
	return &resp, nil
}
