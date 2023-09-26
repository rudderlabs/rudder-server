package dbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/databricks/databricks-sql-go/internal/config"
	_ "github.com/databricks/databricks-sql-go/logger"
)

func init() {
	sql.Register("databricks", &databricksDriver{})
}

var DriverVersion = "1.3.1" // update version before each release

type databricksDriver struct{}

// Open returns a new connection to Databricks database with a DSN string.
// Use sql.Open("databricks", <dsn string>) after importing this driver package.
func (d *databricksDriver) Open(dsn string) (driver.Conn, error) {
	cn, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return cn.Connect(context.Background())
}

// OpenConnector returns a new Connector.
// Used by sql.DB to obtain a Connector and invoke its Connect method to obtain each needed connection.
func (d *databricksDriver) OpenConnector(dsn string) (driver.Connector, error) {
	ucfg, err := config.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return NewConnector(withUserConfig(ucfg))
}

var _ driver.Driver = (*databricksDriver)(nil)
var _ driver.DriverContext = (*databricksDriver)(nil)
