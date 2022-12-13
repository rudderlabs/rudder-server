package tunnelling

import (
	"database/sql"
	"fmt"
)

func SQLConnectThroughTunnel(dsn string, tunnelConfig Config) (*sql.DB, error) {

	conf, err := ReadSSHTunnelConfig(tunnelConfig)
	if err != nil {
		return nil, fmt.Errorf("reading ssh tunnel config: %w", err)
	}
	encodedDSN, err := conf.EncodeWithDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("encoding with dsn: %w", err)
	}
	db, err := sql.Open("sql+ssh", encodedDSN)
	if err != nil {
		return nil, fmt.Errorf("opening warehouse connection sql+ssh driver: %w", err)
	}
	return db, nil
}
