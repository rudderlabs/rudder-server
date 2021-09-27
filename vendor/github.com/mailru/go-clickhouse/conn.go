package clickhouse

import (
	"compress/gzip"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type key int

const (
	// QueryID uses for setting query_id request param for request to Clickhouse
	QueryID key = iota
	// QuotaKey uses for setting quota_key request param for request to Clickhouse
	QuotaKey

	quotaKeyParamName = "quota_key"
	queryIDParamName  = "query_id"
)

// errors
var (
	errEmptyQueryID = errors.New("query id is empty")
)

var defaultKillQueryTimeout = time.Duration(time.Second)

// conn implements an interface sql.Conn
type conn struct {
	url                *url.URL
	user               *url.Userinfo
	location           *time.Location
	useDBLocation      bool
	useGzipCompression bool
	transport          *http.Transport
	cancel             context.CancelFunc
	txCtx              context.Context
	stmts              []*stmt
	logger             *log.Logger
	closed             int32
	killQueryOnErr     bool
	killQueryTimeout   time.Duration
}

func newConn(cfg *Config) *conn {
	var logger *log.Logger
	if cfg.Debug {
		logger = log.New(os.Stderr, "clickhouse: ", log.LstdFlags)
	}
	c := &conn{
		url:                cfg.url(map[string]string{"default_format": "TabSeparatedWithNamesAndTypes"}, false),
		location:           cfg.Location,
		useDBLocation:      cfg.UseDBLocation,
		useGzipCompression: cfg.GzipCompression,
		killQueryOnErr:     cfg.KillQueryOnErr,
		killQueryTimeout:   cfg.KillQueryTimeout,
		transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   cfg.Timeout,
				KeepAlive: cfg.IdleTimeout,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:          1,
			IdleConnTimeout:       cfg.IdleTimeout,
			ResponseHeaderTimeout: cfg.ReadTimeout,
			TLSClientConfig:       getTLSConfigClone(cfg.TLSConfig),
		},
		logger: logger,
	}
	// store userinfo in separate member, we will handle it manually
	c.user = c.url.User
	c.url.User = nil
	c.log("new connection", c.url.Scheme, c.url.Host, c.url.Path)
	return c
}

func (c *conn) log(msg ...interface{}) {
	if c.logger != nil {
		c.logger.Println(msg...)
	}
}

// Prepare returns a prepared statement, bound to this connection.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(query)
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
func (c *conn) Close() error {
	if atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		c.log("close connection", c.url.Scheme, c.url.Host, c.url.Path)
		cancel := c.cancel
		transport := c.transport
		c.transport = nil
		c.cancel = nil

		if cancel != nil {
			cancel()
		}
		if transport != nil {
			transport.CloseIdleConnections()
		}
	}
	return nil
}

// Begin starts and returns a new transaction.
func (c *conn) Begin() (driver.Tx, error) {
	return c.beginTx(context.Background())
}

// Commit applies prepared statement if it exists
func (c *conn) Commit() (err error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return driver.ErrBadConn
	}
	if c.txCtx == nil {
		return sql.ErrTxDone
	}
	ctx := c.txCtx
	stmts := c.stmts
	c.txCtx = nil
	c.stmts = stmts[:0]

	if len(stmts) == 0 {
		return nil
	}
	for _, stmt := range stmts {
		c.log("commit statement: ", stmt.prefix, stmt.pattern)
		if err = stmt.commit(ctx); err != nil {
			break
		}
	}
	return
}

// Rollback cleans prepared statement
func (c *conn) Rollback() error {
	if atomic.LoadInt32(&c.closed) != 0 {
		return driver.ErrBadConn
	}
	if c.txCtx == nil {
		return sql.ErrTxDone
	}
	c.txCtx = nil
	stmts := c.stmts
	c.stmts = stmts[:0]

	if len(stmts) == 0 {
		// there is no statements, so nothing to rollback
		return sql.ErrTxDone
	}
	// the statements will be closed by sql.Tx
	return nil
}

// Exec implements the driver.Execer
func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.exec(context.Background(), query, args)
}

// Query implements the driver.Queryer
func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.query(context.Background(), query, args)
}

func (c *conn) beginTx(ctx context.Context) (driver.Tx, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, driver.ErrBadConn
	}
	c.txCtx = ctx
	return c, nil
}

func (c *conn) killQuery(req *http.Request, args []driver.Value) error {
	if !c.killQueryOnErr {
		return nil
	}
	queryID := req.URL.Query().Get(queryIDParamName)
	if queryID == "" {
		return errEmptyQueryID
	}
	query := fmt.Sprintf("KILL QUERY WHERE query_id='%s'", queryID)
	timeout := c.killQueryTimeout
	if timeout == 0 {
		timeout = defaultKillQueryTimeout
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	req, err := c.buildRequest(ctx, query, args)
	if err != nil {
		return err
	}
	body, err := c.doRequest(ctx, req)
	if err != nil {
		return err
	}
	if body != nil {
		// Drain body to enable connection reuse
		_, _ = io.Copy(ioutil.Discard, body)
		body.Close()
	}
	return nil
}

func (c *conn) query(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, driver.ErrBadConn
	}
	req, err := c.buildRequest(ctx, query, args)
	if err != nil {
		return nil, err
	}
	body, err := c.doRequest(ctx, req)
	if err != nil {
		if _, ok := err.(*Error); !ok && err != driver.ErrBadConn {
			killErr := c.killQuery(req, args)
			if killErr != nil {
				c.log("error from killQuery", killErr)
			}
		}
		return nil, err
	}

	return newTextRows(c, body, c.location, c.useDBLocation)
}

func (c *conn) exec(ctx context.Context, query string, args []driver.Value) (driver.Result, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, driver.ErrBadConn
	}
	req, err := c.buildRequest(ctx, query, args)
	if err != nil {
		return nil, err
	}
	body, err := c.doRequest(ctx, req)
	if body != nil {
		// Drain body to enable connection reuse
		_, _ = io.Copy(ioutil.Discard, body)
		body.Close()
	}
	return emptyResult, err
}

func (c *conn) doRequest(ctx context.Context, req *http.Request) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(ctx)
	transport := c.transport
	c.cancel = cancel

	if transport == nil {
		c.cancel = nil
		return nil, driver.ErrBadConn
	}

	req = req.WithContext(ctx)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		c.cancel = nil
		return nil, fmt.Errorf("doRequest: transport failed to send a request to ClickHouse: %w", err)
	}
	if resp.StatusCode != 200 {
		msg, err := readResponse(resp)
		c.cancel = nil
		if err != nil {
			return nil, fmt.Errorf("doRequest: failed to read the response with the status code %d: %w", resp.StatusCode, err)
		}
		// we got non-200 response, which means ClickHouse send an error in the
		// response
		return nil, newError(string(msg))
	}
	return resp.Body, nil
}

func (c *conn) buildRequest(ctx context.Context, query string, params []driver.Value) (*http.Request, error) {
	var err error
	if len(params) > 0 {
		if query, err = interpolateParams(query, params); err != nil {
			return nil, fmt.Errorf("buildRequest: failed to interpolate params: %w", err)
		}
	}

	bodyReader, bodyWriter := io.Pipe()
	go func() {
		if c.useGzipCompression {
			gz := gzip.NewWriter(bodyWriter)
			_, _ = gz.Write([]byte(query))
			_ = gz.Close()
			_ = bodyWriter.Close()
		} else {
			_, _ = bodyWriter.Write([]byte(query))
			_ = bodyWriter.Close()
		}
	}()
	c.log("query: ", query)

	req, err := http.NewRequest(http.MethodPost, c.url.String(), bodyReader)
	if err != nil {
		return nil, fmt.Errorf("buildRequest: failed to create a request: %w", err)
	}

	// http.Transport ignores url.User argument, handle it here
	if c.user != nil {
		p, _ := c.user.Password()
		req.SetBasicAuth(c.user.Username(), p)
	}

	var reqQuery url.Values
	if ctx != nil {
		quotaKey, quotaOk := ctx.Value(QuotaKey).(string)
		if quotaOk && quotaKey != "" {
			if reqQuery == nil {
				reqQuery = req.URL.Query()
			}
			reqQuery.Add(quotaKeyParamName, quotaKey)
		}
		queryID, queryOk := ctx.Value(QueryID).(string)
		if c.killQueryOnErr && (!queryOk || queryID == "") {
			queryID = uuid.New().String()
		}
		if queryID != "" {
			if reqQuery == nil {
				reqQuery = req.URL.Query()
			}
			reqQuery.Add(queryIDParamName, queryID)
		}

	}
	if reqQuery != nil {
		req.URL.RawQuery = reqQuery.Encode()
	}

	if c.useGzipCompression {
		req.Header.Set("Content-Encoding", "gzip")
	}

	return req, nil
}

func (c *conn) prepare(query string) (*stmt, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, driver.ErrBadConn
	}
	c.log("new statement: ", query)
	s := newStmt(query)
	s.c = c
	if c.txCtx == nil {
		s.batchMode = false
	}
	if s.batchMode {
		c.stmts = append(c.stmts, s)
	}
	return s, nil
}
