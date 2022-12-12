package ssh

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSSHConfigEncodeWithDSN(t *testing.T) {
	dsn := "postgres://user:password@host:5439/db?query1=val1&query2=val2"

	config := Config{
		User:       "ssh_user",
		Port:       22,
		Host:       "ssh_host",
		PrivateKey: []byte("myprivate_key&val"),
	}

	result, err := config.EncodeWithDSN(dsn)
	require.Nil(t, err)
	require.Equal(t, "postgres://ssh_user@ssh_host:22/user:password@host:5439/db?query1=val1&query2=val2&ssh_private_key=myprivate_key%26val", result)
}

func TestSSHConfigDecodeWithDSN(t *testing.T) {
	encodedDSN := "postgres://ssh_user@ssh_host:22/user:password@host:5439/db?query1=val1&query2=val2&ssh_private_key=myprivate_key%26val"
	config := &Config{}

	whDSN, err := config.DecodeFromDSN(encodedDSN)
	require.Nil(t, err)
	require.Equal(t, "postgres://user:password@host:5439/db?query1=val1&query2=val2", whDSN)

	require.Equal(t, config.Host, "ssh_host")
	require.Equal(t, config.Port, 22)
	require.Equal(t, config.User, "ssh_user")
	require.Equal(t, string(config.PrivateKey), "myprivate_key&val")
}
