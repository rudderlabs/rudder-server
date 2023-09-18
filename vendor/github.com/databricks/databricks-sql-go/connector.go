package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/auth"
	"github.com/databricks/databricks-sql-go/auth/pat"
	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/databricks/databricks-sql-go/logger"
)

type connector struct {
	cfg    *config.Config
	client *http.Client
}

// Connect returns a connection to the Databricks database from a connection pool.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	var catalogName *cli_service.TIdentifier
	var schemaName *cli_service.TIdentifier
	if c.cfg.Catalog != "" {
		catalogName = cli_service.TIdentifierPtr(cli_service.TIdentifier(c.cfg.Catalog))
	}
	if c.cfg.Schema != "" {
		schemaName = cli_service.TIdentifierPtr(cli_service.TIdentifier(c.cfg.Schema))
	}

	tclient, err := client.InitThriftClient(c.cfg, c.client)
	if err != nil {
		return nil, dbsqlerrint.NewDriverError(ctx, dbsqlerr.ErrThriftClient, err)
	}
	protocolVersion := int64(c.cfg.ThriftProtocolVersion)
	session, err := tclient.OpenSession(ctx, &cli_service.TOpenSessionReq{
		ClientProtocolI64: &protocolVersion,
		Configuration:     make(map[string]string),
		InitialNamespace: &cli_service.TNamespace{
			CatalogName: catalogName,
			SchemaName:  schemaName,
		},
		CanUseMultipleCatalogs: &c.cfg.CanUseMultipleCatalogs,
	})

	if err != nil {
		return nil, dbsqlerrint.NewRequestError(ctx, fmt.Sprintf("error connecting: host=%s port=%d, httpPath=%s", c.cfg.Host, c.cfg.Port, c.cfg.HTTPPath), err)
	}

	conn := &conn{
		id:      client.SprintGuid(session.SessionHandle.GetSessionId().GUID),
		cfg:     c.cfg,
		client:  tclient,
		session: session,
	}
	log := logger.WithContext(conn.id, driverctx.CorrelationIdFromContext(ctx), "")

	log.Info().Msgf("connect: host=%s port=%d httpPath=%s", c.cfg.Host, c.cfg.Port, c.cfg.HTTPPath)

	for k, v := range c.cfg.SessionParams {
		setStmt := fmt.Sprintf("SET `%s` = `%s`;", k, v)
		_, err := conn.ExecContext(ctx, setStmt, []driver.NamedValue{})
		if err != nil {
			return nil, dbsqlerrint.NewExecutionError(ctx, fmt.Sprintf("error setting session param: %s", setStmt), err, nil)
		}
		log.Info().Msgf("set session parameter: param=%s value=%s", k, v)
	}
	return conn, nil
}

// Driver returns underlying databricksDriver for compatibility with sql.DB Driver method
func (c *connector) Driver() driver.Driver {
	return &databricksDriver{}
}

var _ driver.Connector = (*connector)(nil)

type connOption func(*config.Config)

// NewConnector creates a connection that can be used with `sql.OpenDB()`.
// This is an easier way to set up the DB instead of having to construct a DSN string.
func NewConnector(options ...connOption) (driver.Connector, error) {
	// config with default options
	cfg := config.WithDefaults()
	cfg.DriverVersion = DriverVersion

	for _, opt := range options {
		opt(cfg)
	}

	client := client.RetryableClient(cfg)

	return &connector{cfg: cfg, client: client}, nil
}

func withUserConfig(ucfg config.UserConfig) connOption {
	return func(c *config.Config) {
		c.UserConfig = ucfg
	}
}

// WithServerHostname sets up the server hostname. Mandatory.
func WithServerHostname(host string) connOption {
	return func(c *config.Config) {
		protocol, hostname := parseHostName(host)
		if protocol != "" {
			c.Protocol = protocol
		}

		c.Host = hostname
	}
}

func parseHostName(host string) (protocol, hostname string) {
	hostname = host
	if strings.HasPrefix(host, "https") {
		hostname = strings.TrimPrefix(host, "https")
		protocol = "https"
	} else if strings.HasPrefix(host, "http") {
		hostname = strings.TrimPrefix(host, "http")
		protocol = "http"
	}

	if protocol != "" {
		hostname = strings.TrimPrefix(hostname, ":")
		hostname = strings.TrimPrefix(hostname, "//")
	}

	if hostname == "localhost" && protocol == "" {
		protocol = "http"
	}

	return
}

// WithPort sets up the server port. Mandatory.
func WithPort(port int) connOption {
	return func(c *config.Config) {
		c.Port = port
	}
}

// WithRetries sets up retrying logic. Sane defaults are provided. Negative retryMax will disable retry behavior
// By default retryWaitMin = 1 * time.Second
// By default retryWaitMax = 30 * time.Second
// By default retryMax = 4
func WithRetries(retryMax int, retryWaitMin time.Duration, retryWaitMax time.Duration) connOption {
	return func(c *config.Config) {
		c.RetryWaitMax = retryWaitMax
		c.RetryWaitMin = retryWaitMin
		c.RetryMax = retryMax
	}
}

// WithAccessToken sets up the Personal Access Token. Mandatory for now.
func WithAccessToken(token string) connOption {
	return func(c *config.Config) {
		if token != "" {
			c.AccessToken = token
			pat := &pat.PATAuth{
				AccessToken: token,
			}
			c.Authenticator = pat
		}
	}
}

// WithHTTPPath sets up the endpoint to the warehouse. Mandatory.
func WithHTTPPath(path string) connOption {
	return func(c *config.Config) {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		c.HTTPPath = path
	}
}

// WithMaxRows sets up the max rows fetched per request. Default is 10000
func WithMaxRows(n int) connOption {
	return func(c *config.Config) {
		if n != 0 {
			c.MaxRows = n
		}
	}
}

// WithTimeout adds timeout for the server query execution. Default is no timeout.
func WithTimeout(n time.Duration) connOption {
	return func(c *config.Config) {
		c.QueryTimeout = n
	}
}

// Sets the initial catalog name and schema name in the session.
// Use <select * from foo> instead of <select * from catalog.schema.foo>
func WithInitialNamespace(catalog, schema string) connOption {
	return func(c *config.Config) {
		c.Catalog = catalog
		c.Schema = schema
	}
}

// Used to identify partners. Set as a string with format <isv-name+product-name>.
func WithUserAgentEntry(entry string) connOption {
	return func(c *config.Config) {
		c.UserAgentEntry = entry
	}
}

// Sessions params will be set upon opening the session by calling SET function.
// If using connection pool, session params can avoid successive calls of "SET ..."
func WithSessionParams(params map[string]string) connOption {
	return func(c *config.Config) {
		for k, v := range params {
			if strings.ToLower(k) == "timezone" {
				if loc, err := time.LoadLocation(v); err != nil {
					logger.Error().Msgf("timezone %s is not valid", v)
				} else {
					c.Location = loc
				}

			}
		}
		c.SessionParams = params
	}
}

// WithAuthenticator sets up the Authentication. Mandatory if access token is not provided.
func WithAuthenticator(authr auth.Authenticator) connOption {
	return func(c *config.Config) {
		c.Authenticator = authr
	}
}

// WithTransport sets up the transport configuration to be used by the httpclient.
func WithTransport(t http.RoundTripper) connOption {
	return func(c *config.Config) {
		c.Transport = t
	}
}

// WithCloudFetch sets up the use of cloud fetch for query execution. Default is false.
func WithCloudFetch(useCloudFetch bool) connOption {
	return func(c *config.Config) {
		c.UseCloudFetch = useCloudFetch
	}
}

// WithMaxDownloadThreads sets up maximum download threads for cloud fetch. Default is 10.
func WithMaxDownloadThreads(numThreads int) connOption {
	return func(c *config.Config) {
		c.MaxDownloadThreads = numThreads
	}
}
