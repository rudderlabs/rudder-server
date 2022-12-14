package tunnelling

import (
	"errors"
	"fmt"
	"strconv"

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
	Type   string
	Config map[string]interface{}
)

type TunnelInfo struct {
	Config Config
}

func ReadSSHTunnelConfig(config Config) (conf *stunnel.Config, err error) {
	var user, host, port, privateKey *string

	if user, err = ReadString(sshUser, config); err != nil {
		return nil, err
	}

	if host, err = ReadString(sshHost, config); err != nil {
		return nil, err
	}

	if port, err = ReadString(sshPort, config); err != nil {
		return nil, err
	}

	if privateKey, err = ReadString(sshPrivateKey, config); err != nil {
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

func ReadString(key string, config Config) (*string, error) {
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
