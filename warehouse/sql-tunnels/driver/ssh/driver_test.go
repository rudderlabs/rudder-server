package ssh_test

import (
	"database/sql"
	"io/ioutil"
	"testing"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/warehouse/sql-tunnels/driver/ssh"
	"github.com/stretchr/testify/require"
)

var (
	privateKey []byte
)

func TestMain(m *testing.M) {
	privateKey, _ = ioutil.ReadFile("testdata/test_key")
	m.Run()
}

func TestSqlSSHPingsPostgresDB(t *testing.T) {
	config := ssh.SSHConfig{
		SshUser:    "root",
		SshPort:    2222,
		SshHost:    "0.0.0.0",
		PrivateKey: privateKey,
	}

	encoded, err := config.EncodeWithDSN("postgres://postgres:postgres@db_postgres:5432/postgres?sslmode=disable")
	require.Nil(t, err)

	db, err := sql.Open("sql+ssh", encoded)
	require.Nil(t, err)

	err = db.Ping()
	require.Nil(t, err)

	err = db.Close()
	require.Nil(t, err)
}

// Clickhouse
func TestSqlSSHPingsClickhouseDB(t *testing.T) {
	config := ssh.SSHConfig{
		SshUser:    "root",
		SshPort:    2222,
		SshHost:    "0.0.0.0",
		PrivateKey: privateKey,
	}

	encoded, err := config.EncodeWithDSN("clickhouse://clickhouse:clickhouse@db_clickhouse:9000/db?skip_verify=false")
	require.Nil(t, err)

	db, err := sql.Open("sql+ssh", encoded)
	require.Nil(t, err)

	err = db.Ping()
	require.Nil(t, err)

	err = db.Close()
	require.Nil(t, err)
}
