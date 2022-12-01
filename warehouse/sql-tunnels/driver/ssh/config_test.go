package ssh

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSSHConfigEncodeWithDSN(t *testing.T) {
	dsn := "postgres://user:password@host:5439/db?query1=val1&query2=val2"

	config := SSHConfig{
		SshUser:    "ssh_user",
		SshPort:    22,
		SshHost:    "ssh_host",
		PrivateKey: []byte("myprivate_key&val"),
	}

	result, err := config.EncodeWithDSN(dsn)
	require.Nil(t, err)
	require.Equal(t, "postgres://ssh_user@ssh_host:22/user:password@host:5439/db?query1=val1&query2=val2&ssh_private_key=myprivate_key%26val", result)
}

func TestSSHConfigDecodeWithDSN(t *testing.T) {
	encodedDSN := "postgres://ssh_user@ssh_host:22/user:password@host:5439/db?query1=val1&query2=val2&ssh_private_key=myprivate_key%26val"
	config := &SSHConfig{}

	whDSN, err := config.DecodeFromDSN(encodedDSN)
	require.Nil(t, err)
	require.Equal(t, "postgres://user:password@host:5439/db?query1=val1&query2=val2", whDSN)

	require.Equal(t, config.SshHost, "ssh_host")
	require.Equal(t, config.SshPort, 22)
	require.Equal(t, config.SshUser, "ssh_user")
	require.Equal(t, string(config.PrivateKey), "myprivate_key&val")
}
