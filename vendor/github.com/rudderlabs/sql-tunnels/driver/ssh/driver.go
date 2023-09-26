package ssh

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"

	"github.com/rudderlabs/sql-tunnels/tunnel"
)

// sql -> Open(name)-> conn
// sql -> DriverCtx -> Connector -> OpenCtx(name) ->driver.Conn

// Issues:
// 1. The connection string would change, so we would need to lock it down to use postgres one ?
// 2. The next thing is getting the driver.Conn object from the sql.Open() or pq.Open(). This needs to be fixed,
// to allow for the streamlining of the fetch of driver.Conn

var (
	_ driver.DriverContext = &Driver{}
	_ driver.Connector     = &Connector{}
)

func init() {
	sql.Register("sql+ssh", &Driver{})
}

type Driver struct{}

func (driver *Driver) Open(string) (driver.Conn, error) {
	return nil, fmt.Errorf("not implemented")
}

// OpenConnector creates a connector for the encoded dsn provided which contains
// the sshconfig
func (driver *Driver) OpenConnector(name string) (driver.Connector, error) {
	// Decode the SSH Config from the encoded name.
	sshConfig := &Config{}
	remoteHostDSN, err := sshConfig.DecodeFromDSN(name)
	if err != nil {
		return nil, fmt.Errorf("decoding config from dsn: %w", err)
	}

	remoteParsed, err := url.Parse(remoteHostDSN)
	if err != nil {
		return nil, fmt.Errorf("parsing the remote host dsn: %w", err)
	}

	remotePort, _ := strconv.Atoi(remoteParsed.Port())
	// Setup the tunnel config from the ssh config.
	config := &tunnel.SSHConfig{
		User:       sshConfig.User,
		Host:       sshConfig.Host,
		Port:       sshConfig.Port,
		PrivateKey: sshConfig.PrivateKey,
		RemoteHost: remoteParsed.Hostname(),
		RemotePort: remotePort,
	}

	t, err := tunnel.ListenAndForward(config)
	if err != nil {
		return nil, fmt.Errorf("creating instance of tunnel: %w", err)
	}

	var host, port string
	host, port, err = net.SplitHostPort(t.Addr())
	if err != nil {
		return nil, err
	}

	return &Connector{
		tunnel:   t,
		dsn:      remoteHostDSN,
		localDSN: replaceHostPort(remoteHostDSN, host, port),
	}, nil
}

type Connector struct {
	dsn      string
	db       *sql.DB
	tunnel   *tunnel.SSH
	localDSN string

	mu sync.Mutex
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error

	if c.db == nil {
		parsed, _ := url.Parse(c.localDSN)
		c.db, err = sql.Open(parsed.Scheme, c.localDSN)
		if err != nil {
			return nil, fmt.Errorf("over ssh tunnel: %w", err)
		}
	}

	conn, err := c.db.Driver().Open(c.localDSN)
	if err != nil {
		return nil, fmt.Errorf("opening connection: %w", err)
	}

	return conn, nil
}

func (c *Connector) Driver() driver.Driver {
	return c.db.Driver()
}

// Close needs to run best effort protocol to try and close
// the underlying resources. In case it's unable to close the connection,
// we need to be able to send the combined errors to layers above.
func (c *Connector) Close() (resultErr error) {
	var combiner ErrorCombiner
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			combiner.Combine(fmt.Errorf("closing underlying db connection: %w", err))
		}
	}

	if err := c.tunnel.Close(); err != nil {
		combiner.Combine(fmt.Errorf("closing underlying tunnel connection: %w", err))
	}

	return combiner.ErrorOrNil()
}

func replaceHostPort(baseurl, newHost, newPort string) string {
	parsed, _ := url.Parse(baseurl)
	parsed.Host = net.JoinHostPort(newHost, newPort)
	return parsed.String()
}

func CombineError(base, err error) (updated error) {
	return fmt.Errorf("%v:%v", base, err)
}

type ErrorCombiner []error

func (ec *ErrorCombiner) Combine(err error) {
	*ec = append(*ec, err)
}

func (ec *ErrorCombiner) Error() string {
	err := ""
	for _, e := range *ec {
		err += e.Error()
	}
	return err
}

func (ec *ErrorCombiner) ErrorOrNil() error {
	if len(*ec) == 0 {
		return nil
	}
	return ec
}
