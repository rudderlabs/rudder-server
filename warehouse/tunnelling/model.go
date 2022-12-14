package tunnelling

import (
	"errors"
	"fmt"
	"strconv"

	stunnel "github.com/rudderlabs/sql-tunnels/driver/ssh"
)

var ErrMissingKey = errors.New("missing mandatory key")
var ErrUnexpectedType = errors.New("unexpected type")

const (
	SSHForward Type = "ssh_forward"
)

type Type string
type Config map[string]interface{}

type TunnelInfo struct {
	Config Config
}

func ReadSSHTunnelConfig(config map[string]interface{}) (conf *stunnel.Config, err error) {

	var (
		user, host, port, privateKey *string
	)

	if user, err = ReadString("sshUser", config); err != nil {
		return nil, err
	}

	if host, err = ReadString("sshHost", config); err != nil {
		return nil, err
	}

	if port, err = ReadString("sshPort", config); err != nil {
		return nil, err
	}

	if privateKey, err = ReadString("sshPrivateKey", config); err != nil {
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

func ReadString(key string, ip map[string]interface{}) (*string, error) {
	val, ok := ip[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMissingKey, key)
	}

	resp, ok := val.(string)
	if !ok {
		return nil, fmt.Errorf("%w: %s expected string", ErrUnexpectedType, key)
	}

	return &resp, nil
}
