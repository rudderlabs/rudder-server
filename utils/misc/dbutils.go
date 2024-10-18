package misc

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/collectors"
	"github.com/rudderlabs/rudder-server/rruntime"
)

// GetConnectionString Returns Jobs DB connection configuration
func GetConnectionString(c *config.Config, componentName string) string {
	host := c.GetString("DB.host", "localhost")
	user := c.GetString("DB.user", "ubuntu")
	dbname := c.GetString("DB.name", "ubuntu")
	port := c.GetInt("DB.port", 5432)
	password := c.GetString("DB.password", "ubuntu") // Reading secrets from
	sslmode := c.GetString("DB.sslMode", "disable")
	idleTxTimeout := c.GetDuration("DB.IdleTxTimeout", 5, time.Minute)

	// Application Name can be any string of less than NAMEDATALEN characters (64 characters in a standard PostgreSQL build).
	// There is no need to truncate the string on our own though since PostgreSQL auto-truncates this identifier and issues a relevant notice if necessary.
	appName := DefaultString("rudder-server").OnError(os.Hostname())
	if len(componentName) > 0 {
		appName = fmt.Sprintf("%s-%s", componentName, appName)
	}
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s application_name=%s "+
		" options='-c idle_in_transaction_session_timeout=%d'",
		host, port, user, password, dbname, sslmode, appName,
		idleTxTimeout.Milliseconds(),
	)
}

func NewDatabaseConnectionPool(
	ctx context.Context,
	conf *config.Config,
	stat stats.Stats,
	componentName string,
) (*sql.DB, error) {
	connStr := GetConnectionString(conf, componentName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("opening connection to database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("Error pinging database: %w", err)
	}
	if err := stat.RegisterCollector(
		collectors.NewDatabaseSQLStats(
			componentName,
			db,
		),
	); err != nil {
		return nil, fmt.Errorf("Error registering database stats collector: %w", err)
	}

	maxConnsVar := conf.GetReloadableIntVar(40, 1, "db."+componentName+".pool.maxOpenConnections", "db.pool.maxOpenConnections")
	maxConns := maxConnsVar.Load()
	db.SetMaxOpenConns(maxConns)

	maxIdleConnsVar := conf.GetReloadableIntVar(5, 1, "db."+componentName+".pool.maxIdleConnections", "db.pool.maxIdleConnections")
	maxIdleConns := maxIdleConnsVar.Load()
	db.SetMaxIdleConns(maxIdleConns)

	maxIdleTimeVar := conf.GetReloadableDurationVar(15, time.Minute, "db."+componentName+".pool.maxIdleTime", "db.pool.maxIdleTime")
	maxIdleTime := maxIdleTimeVar.Load()
	db.SetConnMaxIdleTime(maxIdleTime)

	maxConnLifetimeVar := conf.GetReloadableDurationVar(0, 0, "db."+componentName+".pool.maxConnLifetime", "db.pool.maxConnLifetime")
	maxConnLifetime := maxConnLifetimeVar.Load()
	db.SetConnMaxLifetime(maxConnLifetime)

	rruntime.Go(func() {
		ticker := time.NewTicker(
			conf.GetDurationVar(
				5,
				time.Second,
				"db."+componentName+".pool.configUpdateInterval",
				"db.pool.configUpdateInterval",
			),
		)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				updatePoolConfig(db.SetMaxOpenConns, &maxConns, maxConnsVar)
				updatePoolConfig(db.SetConnMaxIdleTime, &maxIdleTime, maxIdleTimeVar)
				updatePoolConfig(db.SetMaxIdleConns, &maxIdleConns, maxIdleConnsVar)
				updatePoolConfig(db.SetConnMaxLifetime, &maxConnLifetime, maxConnLifetimeVar)
			}
		}
	})
	return db, nil
}

func updatePoolConfig[T comparable](setter func(T), current *T, conf config.ValueLoader[T]) {
	newValue := conf.Load()
	if newValue != *current {
		setter(newValue)
		*current = newValue
	}
}

// SetAppNameInDBConnURL sets application name in db connection url
// if application name is already present in dns it will get override by the appName
func SetAppNameInDBConnURL(connectionUrl, appName string) (string, error) {
	connUrl, err := url.Parse(connectionUrl)
	if err != nil {
		return "", err
	}
	queryParams := connUrl.Query()
	queryParams.Set("application_name", appName)
	connUrl.RawQuery = queryParams.Encode()
	return connUrl.String(), nil
}

func QuoteLiteral(literal string) string {
	return pq.QuoteLiteral(literal)
}
