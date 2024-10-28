package misc_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func TestSetApplicationNameInDBConnectionURL(t *testing.T) {
	type args struct {
		dns     string
		appName string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "invalid dns url",
			args: args{
				dns:     "abc@example.com:5432",
				appName: "rsources",
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "add app name in dns url",
			args: args{
				dns:     "postgresql://rudder:password@prousmtusmt-rs-shared-postgresql:5432/jobsdb?sslmode=disable",
				appName: "rsources",
			},
			want:    "postgresql://rudder:password@prousmtusmt-rs-shared-postgresql:5432/jobsdb?application_name=rsources&sslmode=disable",
			wantErr: false,
		},
		{
			name: "update app name in dns url",
			args: args{
				dns:     "postgresql://rudder:password@prousmtusmt-rs-shared-postgresql:5432/jobsdb?application_name=random&sslmode=disable",
				appName: "rsources",
			},
			want:    "postgresql://rudder:password@prousmtusmt-rs-shared-postgresql:5432/jobsdb?application_name=rsources&sslmode=disable",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := misc.SetAppNameInDBConnURL(tt.args.dns, tt.args.appName)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetAppNameInDBConnURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SetAppNameInDBConnURL() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIdleTxTimeout(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	conf := config.New()
	conf.Set("DB.host", postgresContainer.Host)
	conf.Set("DB.user", postgresContainer.User)
	conf.Set("DB.name", postgresContainer.Database)
	conf.Set("DB.port", postgresContainer.Port)
	conf.Set("DB.password", postgresContainer.Password)

	txTimeout := 2 * time.Millisecond

	conf.Set("DB.IdleTxTimeout", txTimeout)

	dsn := misc.GetConnectionString(conf, "test")

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)

	var sessionTimeout string
	err = db.QueryRow("SHOW idle_in_transaction_session_timeout;").Scan(&sessionTimeout)
	require.NoError(t, err)
	require.Equal(t, txTimeout.String(), sessionTimeout)

	t.Run("timeout tx", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)

		var pid int
		err = tx.QueryRow(`select pg_backend_pid();`).Scan(&pid)
		require.NoError(t, err)

		_, err = tx.Exec("select 1")
		require.NoError(t, err)
		t.Log("sleep double the timeout to close connection")
		time.Sleep(2 * txTimeout)

		err = tx.Commit()
		require.EqualError(t, err, "driver: bad connection")

		var count int
		err = db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE pid = $1`, pid).Scan(&count)
		require.NoError(t, err)

		require.Zero(t, count)
	})

	t.Run("successful tx", func(t *testing.T) {
		tx, err := db.Begin()
		require.NoError(t, err)
		_, err = tx.Exec("select 1")
		require.NoError(t, err)
		_, err = tx.Exec(fmt.Sprintf("select pg_sleep(%f)", txTimeout.Seconds()))
		require.NoError(t, err)

		require.NoError(t, tx.Commit())
	})
}

func TestCommonPool(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	conf := config.New()
	conf.Set("DB.host", postgresContainer.Host)
	conf.Set("DB.user", postgresContainer.User)
	conf.Set("DB.name", postgresContainer.Database)
	conf.Set("DB.port", postgresContainer.Port)
	conf.Set("DB.password", postgresContainer.Password)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conf.Set("db.test.pool.configUpdateInterval", 10*time.Millisecond)
	db, err := misc.NewDatabaseConnectionPool(ctx, conf, stats.NOP, "test")
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	defer db.Close()
	require.Equal(t, 40, db.Stats().MaxOpenConnections)

	conf.Set("db.test.pool.maxOpenConnections", 5)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 5, db.Stats().MaxOpenConnections)
}
