package clickhouse

import (
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("clickhouse", new(chDriver))
}

// chDriver implements sql.Driver interface
type chDriver struct {
}

// Open returns new db connection
func (d *chDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return newConn(cfg), nil
}
