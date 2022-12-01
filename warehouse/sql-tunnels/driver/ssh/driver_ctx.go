package ssh

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/rudderlabs/rudder-server/warehouse/sql-tunnels/tunnel"
	stunnel "github.com/rudderlabs/rudder-server/warehouse/sql-tunnels/tunnel/ssh"
)

// sql -> Open(name)-> conn
// sql -> DriverCtx -> Connector -> OpenCtx(name) ->driver.Conn

// Issues:
// 1. The connection string would change, so we would need to lock it down to use postgres one ?
// 2. The next thing is getting the driver.Conn object from the sql.Open() or pq.Open(). This needs to be fixed,
// to allow for the streamlining of the fetch of driver.Conn

var (
	_ driver.DriverContext = &SqlSshDriver{}
	_ driver.Connector     = &SqlSshConnector{}
)

func init() {
	fmt.Println("registering the `ssh+sql` driver")
	sql.Register("sql+ssh", &SqlSshDriver{})
}

type SqlSshDriver struct {
}

func (driver *SqlSshDriver) Open(string) (driver.Conn, error) {
	return nil, fmt.Errorf("not implemented")
}

// OpenConnector creates a connector for the encoded dsn provided which contains
// the sshconfig
func (driver *SqlSshDriver) OpenConnector(name string) (driver.Connector, error) {

	// Decode the SSH Config from the encoded name.
	sshConfig := &SSHConfig{}
	remoteHostDSN, err := sshConfig.DecodeFromDSN(name)
	if err != nil {
		return nil, fmt.Errorf("decoding config from dsn: %w", err)
	}

	fmt.Println(remoteHostDSN)
	remoteParsed, err := url.Parse(remoteHostDSN)
	if err != nil {
		return nil, fmt.Errorf("parsing the remote host dsn: %w", err)
	}

	remotePort, _ := strconv.Atoi(remoteParsed.Port())
	// Setup the tunnel config from the ssh config.
	config := &stunnel.SSHTunnelConfig{
		SshUser:    sshConfig.SshUser,
		SshHost:    sshConfig.SshHost,
		SshPort:    sshConfig.SshPort,
		PrivateKey: sshConfig.PrivateKey,
		RemoteHost: remoteParsed.Hostname(),
		RemotePort: remotePort,
		LocalPort:  0,
	}

	fmt.Println()

	t, err := stunnel.NewSSHTunnel(config)
	if err != nil {
		return nil, fmt.Errorf("creating instance of tunnel: %w", err)
	}

	return &SqlSshConnector{
		Tunnel: t,
		dsn:    remoteHostDSN,
	}, nil
}

type SqlSshConnector struct {
	sync.Once
	dsn string
	db  *sql.DB
	tunnel.Tunnel
	localDSN string
}

func (c *SqlSshConnector) Connect(ctx context.Context) (driver.Conn, error) {

	var (
		err error
	)

	// Once we, need to open the tunnel and return the connection
	// to the underlying warehouse.
	c.Once.Do(func() {
		fmt.Println("opening the tunnel only once")

		err = c.Tunnel.Open(ctx)
		if err != nil {
			return
		}

		hostPort := strings.Split(c.Tunnel.LocalConnectionString(), ":")
		c.localDSN = replaceHostPort(
			c.dsn,
			hostPort[0],
			hostPort[1])

		parsed, _ := url.Parse(c.localDSN)

		// find the protocol from the url parsing of the dsn
		fmt.Printf("opening connection to postgres warehouse over: %s\n", c.localDSN)
		c.db, err = sql.Open(parsed.Scheme, c.localDSN)
		if err != nil {
			return
		}
	})

	if err != nil {
		return nil, fmt.Errorf("opening connection through tunnel: %w", err)
	}

	if c.db == nil {
		return nil, fmt.Errorf("no remote database connection available")
	}

	conn, err := c.db.Driver().Open(c.localDSN)
	if err != nil {
		return nil, fmt.Errorf("opening connection to warehouse: %w", err)
	}

	return conn, nil
}

func (c *SqlSshConnector) Driver() driver.Driver {
	return c.db.Driver()
}

// Close needs to run best effort protocol to try and close
// the underlying resources. In case it's unable to close the connection,
// we need to be able to send the combined errors to layers above.
func (c *SqlSshConnector) Close() (resultErr error) {
	fmt.Println("connector: closing the tunnel")

	var (
		combiner ErrorCombiner
	)

	if err := c.db.Close(); err != nil {
		combiner.Combine(fmt.Errorf("closing underlying db connection: %w", err))
	}

	if err := c.Tunnel.Close(context.Background()); err != nil {
		combiner.Combine(fmt.Errorf("closing underlying tunnel connection: %w", err))
	}

	return combiner.ErrorOrNil()
}

func replaceHostPort(baseurl, newHost, newPort string) string {
	parsed, _ := url.Parse(baseurl)
	parsed.Host = fmt.Sprintf("%s:%s", newHost, newPort)
	return parsed.String()
}

func CombineError(base error, err error) (updated error) {
	return fmt.Errorf("%v:%v", base, err)
}

type ErrorCombiner []error

func (ec *ErrorCombiner) Combine(err error) {
	*ec = append(*ec, err)
}

func (ec *ErrorCombiner) Error() string {
	err := ""
	for _, e := range *ec {
		err += fmt.Sprintf("%s", e.Error())
	}
	return err
}

func (ec *ErrorCombiner) ErrorOrNil() error {
	if len(*ec) == 0 {
		return nil
	}
	return ec
}
