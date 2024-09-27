package misc

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-go-kit/config"
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
