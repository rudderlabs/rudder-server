package clickhouse

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"strings"

	clickhouse "github.com/rudderlabs/clickhouse-go/v2"

	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
)

// connect opens a connection and wraps it in the shared middleware.
// includeDatabase is false when connecting in order to create the database.
func (ch *ClickhouseV2) connect(includeDatabase bool) (*sqlmw.DB, error) {
	opts := &clickhouse.Options{
		Addr: []string{net.JoinHostPort(
			ch.Warehouse.GetStringDestinationConfig(ch.conf, model.HostSetting),
			ch.Warehouse.GetStringDestinationConfig(ch.conf, model.PortSetting),
		)},
		Auth: clickhouse.Auth{
			Username: ch.Warehouse.GetStringDestinationConfig(ch.conf, model.UserSetting),
			Password: ch.Warehouse.GetStringDestinationConfig(ch.conf, model.PasswordSetting),
		},
		Protocol:    clickhouse.Native, // default protocol is TCP
		DialTimeout: ch.connectTimeout,
		ReadTimeout: ch.config.readTimeout,
		Debug:       ch.config.queryDebugLogs,
	}
	if includeDatabase {
		opts.Auth.Database = ch.Warehouse.GetStringDestinationConfig(ch.conf, model.DatabaseSetting)
	}
	if ch.Warehouse.GetBoolDestinationConfig(model.SecureSetting) {
		tlsConfig, err := ch.tlsConfig()
		if err != nil {
			return nil, fmt.Errorf("creating TLS config: %w", err)
		}
		opts.TLS = tlsConfig
	}
	if ch.config.compress {
		opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	}

	db := clickhouse.OpenDB(opts)
	db.SetMaxOpenConns(ch.config.poolSize)
	db.SetMaxIdleConns(ch.config.poolSize)

	return sqlmw.New(
		db,
		sqlmw.WithStats(ch.stats),
		sqlmw.WithLogger(ch.logger),
		sqlmw.WithKeyAndValues(
			logfield.SourceID, ch.Warehouse.Source.ID,
			logfield.SourceType, ch.Warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, ch.Warehouse.Destination.ID,
			logfield.DestinationType, ch.Warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, ch.Warehouse.WorkspaceID,
			logfield.Namespace, ch.Namespace,
		),
		sqlmw.WithQueryTimeout(ch.connectTimeout),
		sqlmw.WithSlowQueryThreshold(ch.config.slowQueryThreshold),
	), nil
}

func (ch *ClickhouseV2) tlsConfig() (*tls.Config, error) {
	conf := &tls.Config{
		InsecureSkipVerify: ch.Warehouse.GetBoolDestinationConfig(model.SkipVerifySetting),
		MinVersion:         tls.VersionTLS12,
	}

	certificate := ch.Warehouse.GetStringDestinationConfig(ch.conf, model.CACertificateSetting)
	if strings.TrimSpace(certificate) == "" {
		return conf, nil
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM([]byte(certificate)) {
		return nil, errAppendingCACertificate
	}
	conf.RootCAs = caCertPool
	return conf, nil
}
