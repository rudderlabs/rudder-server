package tunnelling

import (
	"errors"
	"strconv"

	stunnel "github.com/rudderlabs/rudder-server/warehouse/sql-tunnels/driver/ssh"
)

var ErrMissingKey = errors.New("Missing mandatory key")

const (
	SSHForward Type = "ssh_forward"
)

type Type string
type Config map[string]string

type TunnelInfo struct {
	Type   Type
	Config Config
}

func ReadConfigFromMap(config map[string]string) Config {
	return nil
}

func ReadSSHTunnelConfig(config map[string]string) (*stunnel.SSHConfig, error) {

	user := readMustString("ssh_user", config)
	if user == nil {
		return nil, ErrMissingKey
	}

	host := readMustString("ssh_host", config)
	if host == nil {
		return nil, ErrMissingKey
	}

	portStr := readMustString("ssh_port", config)
	if portStr == nil {
		return nil, ErrMissingKey
	}

	privateKey := readMustString("private_key", config)
	if privateKey == nil {
		return nil, ErrMissingKey
	}

	port, _ := strconv.Atoi(*portStr)
	return &stunnel.SSHConfig{
		SshUser:    *user,
		SshHost:    *host,
		PrivateKey: []byte(*privateKey),
		SshPort:    port,
	}, nil
}

func readMustString(key string, ip map[string]string) *string {
	val, ok := ip[key]
	if !ok {
		return nil
	}
	return &val
}
