package misc

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/lib/pq"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/collectors"
	"github.com/rudderlabs/rudder-server/rruntime"
)

// GetConnectionString Returns Jobs DB connection configuration
func GetConnectionString(c *config.Config, componentName string) string {
	host := c.GetStringVar("localhost", "DB.host")
	user := c.GetStringVar("ubuntu", "DB.user")
	dbname := c.GetStringVar("ubuntu", "DB.name")
	port := c.GetIntVar(5432, 1, "DB.port")
	password := c.GetStringVar("ubuntu", "DB.password")
	sslmode := c.GetStringVar("disable", "DB.sslMode")
	// same database with potentially different idle tx timeouts per component
	dbConfigKeys := func(key string) (keys []string) {
		if componentName != "" {
			keys = append(keys, "DB."+componentName+"."+key)
		}
		keys = append(keys, "DB."+key)
		return keys
	}
	idleTxTimeout := c.GetDurationVar(5, time.Minute, dbConfigKeys("idleTxTimeout")...)

	hostname := c.GetString("HOSTNAME", DefaultString("rudder-server").OnError(os.Hostname()))

	// [application_name] can be any string of less than NAMEDATALEN characters (64 characters in a standard PostgreSQL build).
	// Format: [component_prefix][hostname_suffix]
	//   - component_prefix: first 2 characters of componentName + "-" (omitted if componentName is empty)
	//   - hostname_suffix:  first 60 characters of hostname
	// It is important not to rely on postgresql's own truncation mechanism, since we are using the [app_name] for terminating dangling connections
	// and we need all connections from the same host to be terminated, regardless of the component name, thus the last part of the [app_name] needs to be the same
	// across all components.
	var componentPart string
	if componentName != "" {
		componentPart = lo.Substring(componentName, 0, 2) + "-"
	}
	appName := componentPart + lo.Substring(hostname, 0, 60)

	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s application_name=%s "+
		" options='-c idle_in_transaction_session_timeout=%d'",
		host, port, user, password, dbname, sslmode, appName,
		idleTxTimeout.Milliseconds(),
	)
}

type DatabaseConnectionPoolConfig struct {
	MaxOpenConns    config.ValueLoader[int]           // MaxOpenConns controls the maximum number of open connections to the database. Setting it to zero means unlimited.
	MaxIdleConns    config.ValueLoader[int]           // MaxIdleConns controls the maximum number of connections in the idle connection pool. Setting it to zero means no idle connections are retained.
	ConnMaxIdleTime config.ValueLoader[time.Duration] // ConnMaxIdleTime sets the maximum amount of time a connection may be idle. Setting it to zero means no limit.
	ConnMaxLifetime config.ValueLoader[time.Duration] // ConnMaxLifetime sets the maximum amount of time a connection may be reused. Setting it to zero means no limit.
	UpdateInterval  time.Duration                     // UpdateInterval sets the interval at which the connection pool configuration is reloaded. Setting it to zero means no reloads.
}

func NewDatabaseConnectionPool(ctx context.Context, componentName string, conf DatabaseConnectionPoolConfig, c *config.Config, stat stats.Stats) (*sql.DB, error) {
	connStr := GetConnectionString(c, componentName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("opening connection to database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("Error pinging database: %w", err)
	}
	if err := stat.RegisterCollector(
		collectors.NewDatabaseSQLStats(componentName, db),
	); err != nil {
		return nil, fmt.Errorf("Error registering database stats collector: %w", err)
	}
	maxOpenConns := conf.MaxOpenConns.Load()
	maxIdleConns := conf.MaxIdleConns.Load()
	maxIdleTime := conf.ConnMaxIdleTime.Load()
	maxConnLifetime := conf.ConnMaxLifetime.Load()
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxIdleTime(maxIdleTime)
	db.SetConnMaxLifetime(maxConnLifetime)
	if conf.UpdateInterval > 0 {
		rruntime.Go(func() {
			ticker := time.NewTicker(conf.UpdateInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					updatePoolConfig(db.SetMaxOpenConns, &maxOpenConns, conf.MaxOpenConns)
					updatePoolConfig(db.SetConnMaxIdleTime, &maxIdleTime, conf.ConnMaxIdleTime)
					updatePoolConfig(db.SetMaxIdleConns, &maxIdleConns, conf.MaxIdleConns)
					updatePoolConfig(db.SetConnMaxLifetime, &maxConnLifetime, conf.ConnMaxLifetime)
				}
			}
		})
	}
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
