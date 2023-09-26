// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file contains code that was borrowed from prestgo, mainly some
// data type definitions.
//
// See https://github.com/avct/prestgo for copyright information.
//
// The MIT License (MIT)
//
// Copyright (c) 2015 Avocet Systems Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

// Package trino provides a database/sql driver for Trino.
//
// The driver should be used via the database/sql package:
//
//	import "database/sql"
//	import _ "github.com/trinodb/trino-go-client/trino"
//
//	dsn := "http://user@localhost:8080?catalog=default&schema=test"
//	db, err := sql.Open("trino", dsn)
package trino

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"gopkg.in/jcmturner/gokrb5.v6/client"
	"gopkg.in/jcmturner/gokrb5.v6/config"
	"gopkg.in/jcmturner/gokrb5.v6/keytab"
)

func init() {
	sql.Register("trino", &Driver{})
}

var (
	// DefaultQueryTimeout is the default timeout for queries executed without a context.
	DefaultQueryTimeout = 60 * time.Second

	// DefaultCancelQueryTimeout is the timeout for the request to cancel queries in Trino.
	DefaultCancelQueryTimeout = 30 * time.Second

	// ErrOperationNotSupported indicates that a database operation is not supported.
	ErrOperationNotSupported = errors.New("trino: operation not supported")

	// ErrQueryCancelled indicates that a query has been cancelled.
	ErrQueryCancelled = errors.New("trino: query cancelled")

	// ErrUnsupportedHeader indicates that the server response contains an unsupported header.
	ErrUnsupportedHeader = errors.New("trino: server response contains an unsupported header")

	// ErrInvalidResponseType indicates that the server returned an invalid type definition.
	ErrInvalidResponseType = errors.New("trino: server response contains an invalid type")

	// ErrInvalidProgressCallbackHeader indicates that server did not get valid headers for progress callback
	ErrInvalidProgressCallbackHeader = errors.New("trino: both " + trinoProgressCallbackParam + " and " + trinoProgressCallbackPeriodParam + " must be set when using progress callback")
)

const (
	trinoHeaderPrefix = `X-Trino-`

	preparedStatementHeader = trinoHeaderPrefix + "Prepared-Statement"
	preparedStatementName   = "_trino_go"

	trinoUserHeader            = trinoHeaderPrefix + `User`
	trinoSourceHeader          = trinoHeaderPrefix + `Source`
	trinoCatalogHeader         = trinoHeaderPrefix + `Catalog`
	trinoSchemaHeader          = trinoHeaderPrefix + `Schema`
	trinoSessionHeader         = trinoHeaderPrefix + `Session`
	trinoSetCatalogHeader      = trinoHeaderPrefix + `Set-Catalog`
	trinoSetSchemaHeader       = trinoHeaderPrefix + `Set-Schema`
	trinoSetPathHeader         = trinoHeaderPrefix + `Set-Path`
	trinoSetSessionHeader      = trinoHeaderPrefix + `Set-Session`
	trinoClearSessionHeader    = trinoHeaderPrefix + `Clear-Session`
	trinoSetRoleHeader         = trinoHeaderPrefix + `Set-Role`
	trinoExtraCredentialHeader = trinoHeaderPrefix + `Extra-Credential`

	trinoProgressCallbackParam       = trinoHeaderPrefix + `Progress-Callback`
	trinoProgressCallbackPeriodParam = trinoHeaderPrefix + `Progress-Callback-Period`

	trinoAddedPrepareHeader       = trinoHeaderPrefix + `Added-Prepare`
	trinoDeallocatedPrepareHeader = trinoHeaderPrefix + `Deallocated-Prepare`

	KerberosEnabledConfig    = "KerberosEnabled"
	kerberosKeytabPathConfig = "KerberosKeytabPath"
	kerberosPrincipalConfig  = "KerberosPrincipal"
	kerberosRealmConfig      = "KerberosRealm"
	kerberosConfigPathConfig = "KerberosConfigPath"
	SSLCertPathConfig        = "SSLCertPath"
	SSLCertConfig            = "SSLCert"
)

var (
	responseToRequestHeaderMap = map[string]string{
		trinoSetSchemaHeader:  trinoSchemaHeader,
		trinoSetCatalogHeader: trinoCatalogHeader,
	}
	unsupportedResponseHeaders = []string{
		trinoSetPathHeader,
		trinoSetRoleHeader,
	}
)

type Driver struct{}

func (d *Driver) Open(name string) (driver.Conn, error) {
	return newConn(name)
}

var _ driver.Driver = &Driver{}

// Config is a configuration that can be encoded to a DSN string.
type Config struct {
	ServerURI          string            // URI of the Trino server, e.g. http://user@localhost:8080
	Source             string            // Source of the connection (optional)
	Catalog            string            // Catalog (optional)
	Schema             string            // Schema (optional)
	SessionProperties  map[string]string // Session properties (optional)
	ExtraCredentials   map[string]string // Extra credentials (optional)
	CustomClientName   string            // Custom client name (optional)
	KerberosEnabled    string            // KerberosEnabled (optional, default is false)
	KerberosKeytabPath string            // Kerberos Keytab Path (optional)
	KerberosPrincipal  string            // Kerberos Principal used to authenticate to KDC (optional)
	KerberosRealm      string            // The Kerberos Realm (optional)
	KerberosConfigPath string            // The krb5 config path (optional)
	SSLCertPath        string            // The SSL cert path for TLS verification (optional)
	SSLCert            string            // The SSL cert for TLS verification (optional)
}

// FormatDSN returns a DSN string from the configuration.
func (c *Config) FormatDSN() (string, error) {
	serverURL, err := url.Parse(c.ServerURI)
	if err != nil {
		return "", err
	}
	var sessionkv []string
	if c.SessionProperties != nil {
		for k, v := range c.SessionProperties {
			sessionkv = append(sessionkv, k+"="+v)
		}
	}
	var credkv []string
	if c.ExtraCredentials != nil {
		for k, v := range c.ExtraCredentials {
			credkv = append(credkv, k+"="+v)
		}
	}
	source := c.Source
	if source == "" {
		source = "trino-go-client"
	}
	query := make(url.Values)
	query.Add("source", source)

	KerberosEnabled, _ := strconv.ParseBool(c.KerberosEnabled)
	isSSL := serverURL.Scheme == "https"

	if c.CustomClientName != "" {
		if c.SSLCert != "" || c.SSLCertPath != "" {
			return "", fmt.Errorf("trino: client configuration error, a custom client cannot be specific together with a custom SSL certificate")
		}
	}
	if c.SSLCertPath != "" {
		if !isSSL {
			return "", fmt.Errorf("trino: client configuration error, SSL must be enabled to specify a custom SSL certificate file")
		}
		if c.SSLCert != "" {
			return "", fmt.Errorf("trino: client configuration error, a custom SSL certificate file cannot be specified together with a certificate string")
		}
		query.Add(SSLCertPathConfig, c.SSLCertPath)
	}

	if c.SSLCert != "" {
		if !isSSL {
			return "", fmt.Errorf("trino: client configuration error, SSL must be enabled to specify a custom SSL certificate")
		}
		if c.SSLCertPath != "" {
			return "", fmt.Errorf("trino: client configuration error, a custom SSL certificate string cannot be specified together with a certificate file")
		}
		query.Add(SSLCertConfig, c.SSLCert)
	}

	if KerberosEnabled {
		query.Add(KerberosEnabledConfig, "true")
		query.Add(kerberosKeytabPathConfig, c.KerberosKeytabPath)
		query.Add(kerberosPrincipalConfig, c.KerberosPrincipal)
		query.Add(kerberosRealmConfig, c.KerberosRealm)
		query.Add(kerberosConfigPathConfig, c.KerberosConfigPath)
		if !isSSL {
			return "", fmt.Errorf("trino: client configuration error, SSL must be enabled for secure env")
		}
	}

	// ensure consistent order of items
	sort.Strings(sessionkv)
	sort.Strings(credkv)

	for k, v := range map[string]string{
		"catalog":            c.Catalog,
		"schema":             c.Schema,
		"session_properties": strings.Join(sessionkv, ","),
		"extra_credentials":  strings.Join(credkv, ","),
		"custom_client":      c.CustomClientName,
	} {
		if v != "" {
			query[k] = []string{v}
		}
	}
	serverURL.RawQuery = query.Encode()
	return serverURL.String(), nil
}

// Conn is a Trino connection.
type Conn struct {
	baseURL               string
	auth                  *url.Userinfo
	httpClient            http.Client
	httpHeaders           http.Header
	kerberosClient        client.Client
	kerberosEnabled       bool
	progressUpdater       ProgressUpdater
	progressUpdaterPeriod queryProgressCallbackPeriod
}

var (
	_ driver.Conn               = &Conn{}
	_ driver.ConnPrepareContext = &Conn{}
)

func newConn(dsn string) (*Conn, error) {
	serverURL, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("trino: malformed dsn: %v", err)
	}

	query := serverURL.Query()

	kerberosEnabled, _ := strconv.ParseBool(query.Get(KerberosEnabledConfig))

	var kerberosClient client.Client

	if kerberosEnabled {
		kt, err := keytab.Load(query.Get(kerberosKeytabPathConfig))
		if err != nil {
			return nil, fmt.Errorf("trino: Error loading Keytab: %v", err)
		}

		kerberosClient = client.NewClientWithKeytab(query.Get(kerberosPrincipalConfig), query.Get(kerberosRealmConfig), kt)
		conf, err := config.Load(query.Get(kerberosConfigPathConfig))
		if err != nil {
			return nil, fmt.Errorf("trino: Error loading krb config: %v", err)
		}

		kerberosClient.WithConfig(conf)

		loginErr := kerberosClient.Login()
		if loginErr != nil {
			return nil, fmt.Errorf("trino: Error login to KDC: %v", loginErr)
		}
	}

	var httpClient = http.DefaultClient
	if clientKey := query.Get("custom_client"); clientKey != "" {
		httpClient = getCustomClient(clientKey)
		if httpClient == nil {
			return nil, fmt.Errorf("trino: custom client not registered: %q", clientKey)
		}
	} else if serverURL.Scheme == "https" {

		cert := []byte(query.Get(SSLCertConfig))

		if certPath := query.Get(SSLCertPathConfig); certPath != "" {
			cert, err = ioutil.ReadFile(certPath)
			if err != nil {
				return nil, fmt.Errorf("trino: Error loading SSL Cert File: %v", err)
			}
		}

		if len(cert) != 0 {
			certPool := x509.NewCertPool()
			certPool.AppendCertsFromPEM(cert)

			httpClient = &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						RootCAs: certPool,
					},
				},
			}
		}
	}

	c := &Conn{
		baseURL:         serverURL.Scheme + "://" + serverURL.Host,
		httpClient:      *httpClient,
		httpHeaders:     make(http.Header),
		kerberosClient:  kerberosClient,
		kerberosEnabled: kerberosEnabled,
	}

	var user string
	if serverURL.User != nil {
		user = serverURL.User.Username()
		pass, _ := serverURL.User.Password()
		if pass != "" && serverURL.Scheme == "https" {
			c.auth = serverURL.User
		}
	}

	for k, v := range map[string]string{
		trinoUserHeader:            user,
		trinoSourceHeader:          query.Get("source"),
		trinoCatalogHeader:         query.Get("catalog"),
		trinoSchemaHeader:          query.Get("schema"),
		trinoSessionHeader:         query.Get("session_properties"),
		trinoExtraCredentialHeader: query.Get("extra_credentials"),
	} {
		if v != "" {
			c.httpHeaders.Add(k, v)
		}
	}

	return c, nil
}

// registry for custom http clients
var customClientRegistry = struct {
	sync.RWMutex
	Index map[string]http.Client
}{
	Index: make(map[string]http.Client),
}

// RegisterCustomClient associates a client to a key in the driver's registry.
//
// Register your custom client in the driver, then refer to it by name in the DSN, on the call to sql.Open:
//
//	foobarClient := &http.Client{
//		Transport: &http.Transport{
//			Proxy: http.ProxyFromEnvironment,
//			DialContext: (&net.Dialer{
//				Timeout:   30 * time.Second,
//				KeepAlive: 30 * time.Second,
//				DualStack: true,
//			}).DialContext,
//			MaxIdleConns:          100,
//			IdleConnTimeout:       90 * time.Second,
//			TLSHandshakeTimeout:   10 * time.Second,
//			ExpectContinueTimeout: 1 * time.Second,
//			TLSClientConfig:       &tls.Config{
//			// your config here...
//			},
//		},
//	}
//	trino.RegisterCustomClient("foobar", foobarClient)
//	db, err := sql.Open("trino", "https://user@localhost:8080?custom_client=foobar")
func RegisterCustomClient(key string, client *http.Client) error {
	if _, err := strconv.ParseBool(key); err == nil {
		return fmt.Errorf("trino: custom client key %q is reserved", key)
	}
	customClientRegistry.Lock()
	customClientRegistry.Index[key] = *client
	customClientRegistry.Unlock()
	return nil
}

// DeregisterCustomClient removes the client associated to the key.
func DeregisterCustomClient(key string) {
	customClientRegistry.Lock()
	delete(customClientRegistry.Index, key)
	customClientRegistry.Unlock()
}

func getCustomClient(key string) *http.Client {
	customClientRegistry.RLock()
	defer customClientRegistry.RUnlock()
	if client, ok := customClientRegistry.Index[key]; ok {
		return &client
	}
	return nil
}

// Begin implements the driver.Conn interface.
func (c *Conn) Begin() (driver.Tx, error) {
	return nil, ErrOperationNotSupported
}

// Prepare implements the driver.Conn interface.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

// PrepareContext implements the driver.ConnPrepareContext interface.
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &driverStmt{conn: c, query: query}, nil
}

// Close implements the driver.Conn interface.
func (c *Conn) Close() error {
	return nil
}

func (c *Conn) newRequest(method, url string, body io.Reader, hs http.Header) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, fmt.Errorf("trino: %v", err)
	}

	if c.kerberosEnabled {
		err = c.kerberosClient.SetSPNEGOHeader(req, "trino/"+req.URL.Hostname())
		if err != nil {
			return nil, fmt.Errorf("error setting client SPNEGO header: %v", err)
		}
	}

	for k, v := range c.httpHeaders {
		req.Header[k] = v
	}
	for k, v := range hs {
		req.Header[k] = v
	}

	if c.auth != nil {
		pass, _ := c.auth.Password()
		req.SetBasicAuth(c.auth.Username(), pass)
	}
	return req, nil
}

func (c *Conn) roundTrip(ctx context.Context, req *http.Request) (*http.Response, error) {
	delay := 100 * time.Millisecond
	const maxDelayBetweenRequests = float64(15 * time.Second)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			timeout := DefaultQueryTimeout
			if deadline, ok := ctx.Deadline(); ok {
				timeout = time.Until(deadline)
			}
			client := c.httpClient
			client.Timeout = timeout
			req.Cancel = ctx.Done()
			resp, err := client.Do(req)
			if err != nil {
				return nil, &ErrQueryFailed{Reason: err}
			}
			switch resp.StatusCode {
			case http.StatusOK:
				for src, dst := range responseToRequestHeaderMap {
					if v := resp.Header.Get(src); v != "" {
						c.httpHeaders.Set(dst, v)
					}
				}
				if v := resp.Header.Get(trinoAddedPrepareHeader); v != "" {
					c.httpHeaders.Add(preparedStatementHeader, v)
				}
				if v := resp.Header.Get(trinoDeallocatedPrepareHeader); v != "" {
					values := c.httpHeaders.Values(preparedStatementHeader)
					c.httpHeaders.Del(preparedStatementHeader)
					for _, v2 := range values {
						if !strings.HasPrefix(v2, v+"=") {
							c.httpHeaders.Add(preparedStatementHeader, v2)
						}
					}
				}
				if v := resp.Header.Get(trinoSetSessionHeader); v != "" {
					c.httpHeaders.Add(trinoSessionHeader, v)
				}
				if v := resp.Header.Get(trinoClearSessionHeader); v != "" {
					values := c.httpHeaders.Values(trinoSessionHeader)
					c.httpHeaders.Del(trinoSessionHeader)
					for _, v2 := range values {
						if !strings.HasPrefix(v2, v+"=") {
							c.httpHeaders.Add(trinoSessionHeader, v2)
						}
					}
				}
				for _, name := range unsupportedResponseHeaders {
					if v := resp.Header.Get(name); v != "" {
						return nil, ErrUnsupportedHeader
					}
				}
				return resp, nil
			case http.StatusServiceUnavailable:
				resp.Body.Close()
				timer.Reset(delay)
				delay = time.Duration(math.Min(
					float64(delay)*math.Phi,
					maxDelayBetweenRequests,
				))
				continue
			default:
				return nil, newErrQueryFailedFromResponse(resp)
			}
		}
	}
}

// ErrQueryFailed indicates that a query to Trino failed.
type ErrQueryFailed struct {
	StatusCode int
	Reason     error
}

// Error implements the error interface.
func (e *ErrQueryFailed) Error() string {
	return fmt.Sprintf("trino: query failed (%d %s): %q",
		e.StatusCode, http.StatusText(e.StatusCode), e.Reason)
}

func newErrQueryFailedFromResponse(resp *http.Response) *ErrQueryFailed {
	const maxBytes = 8 * 1024
	defer resp.Body.Close()
	qf := &ErrQueryFailed{StatusCode: resp.StatusCode}
	b, err := ioutil.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		qf.Reason = err
		return qf
	}
	reason := string(b)
	if resp.ContentLength > maxBytes {
		reason += "..."
	}
	qf.Reason = errors.New(reason)
	return qf
}

type driverStmt struct {
	conn           *Conn
	query          string
	user           string
	nextURIs       chan string
	httpResponses  chan *http.Response
	queryResponses chan queryResponse
	statsCh        chan QueryProgressInfo
	errors         chan error
	doneCh         chan struct{}
}

var (
	_ driver.Stmt              = &driverStmt{}
	_ driver.StmtQueryContext  = &driverStmt{}
	_ driver.StmtExecContext   = &driverStmt{}
	_ driver.NamedValueChecker = &driverStmt{}
)

// Close closes statement just before releasing connection
func (st *driverStmt) Close() error {
	if st.doneCh == nil {
		return nil
	}
	close(st.doneCh)
	if st.statsCh != nil {
		<-st.statsCh
		st.statsCh = nil
	}
	go func() {
		// drain errors chan to allow goroutines to write to it
		for range st.errors {
		}
	}()
	for range st.queryResponses {
	}
	for range st.httpResponses {
	}
	close(st.nextURIs)
	close(st.errors)
	st.doneCh = nil
	return nil
}

func (st *driverStmt) NumInput() int {
	return -1
}

func (st *driverStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, driver.ErrSkip
}

func (st *driverStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	sr, err := st.exec(ctx, args)
	if err != nil {
		return nil, err
	}
	rows := &driverRows{
		ctx:          ctx,
		stmt:         st,
		queryID:      sr.ID,
		nextURI:      sr.NextURI,
		rowsAffected: sr.UpdateCount,
		statsCh:      st.statsCh,
		doneCh:       st.doneCh,
	}
	// consume all results, if there are any
	for err == nil {
		err = rows.fetch()
	}

	if err != nil && err != io.EOF {
		return nil, err
	}
	return rows, nil
}

func (st *driverStmt) CheckNamedValue(arg *driver.NamedValue) error {
	switch arg.Value.(type) {
	case Numeric, trinoDate, trinoTime, trinoTimeTz, trinoTimestamp:
		return nil
	default:
		{
			if reflect.TypeOf(arg.Value).Kind() == reflect.Slice {
				return nil
			}

			if arg.Name == trinoProgressCallbackParam {
				return nil
			}
			if arg.Name == trinoProgressCallbackPeriodParam {
				return nil
			}
		}
	}

	return driver.ErrSkip
}

type stmtResponse struct {
	ID          string    `json:"id"`
	InfoURI     string    `json:"infoUri"`
	NextURI     string    `json:"nextUri"`
	Stats       stmtStats `json:"stats"`
	Error       stmtError `json:"error"`
	UpdateType  string    `json:"updateType"`
	UpdateCount int64     `json:"updateCount"`
}

type stmtStats struct {
	State              string    `json:"state"`
	Scheduled          bool      `json:"scheduled"`
	Nodes              int       `json:"nodes"`
	TotalSplits        int       `json:"totalSplits"`
	QueuesSplits       int       `json:"queuedSplits"`
	RunningSplits      int       `json:"runningSplits"`
	CompletedSplits    int       `json:"completedSplits"`
	UserTimeMillis     int       `json:"userTimeMillis"`
	CPUTimeMillis      int       `json:"cpuTimeMillis"`
	WallTimeMillis     int       `json:"wallTimeMillis"`
	ProcessedRows      int       `json:"processedRows"`
	ProcessedBytes     int       `json:"processedBytes"`
	RootStage          stmtStage `json:"rootStage"`
	ProgressPercentage float32   `json:"progressPercentage"`
}

type stmtError struct {
	Message       string               `json:"message"`
	ErrorName     string               `json:"errorName"`
	ErrorCode     int                  `json:"errorCode"`
	ErrorLocation stmtErrorLocation    `json:"errorLocation"`
	FailureInfo   stmtErrorFailureInfo `json:"failureInfo"`
	// Other fields omitted
}

type stmtErrorLocation struct {
	LineNumber   int `json:"lineNumber"`
	ColumnNumber int `json:"columnNumber"`
}

type stmtErrorFailureInfo struct {
	Type string `json:"type"`
	// Other fields omitted
}

func (e stmtError) Error() string {
	return e.FailureInfo.Type + ": " + e.Message
}

type stmtStage struct {
	StageID         string      `json:"stageId"`
	State           string      `json:"state"`
	Done            bool        `json:"done"`
	Nodes           int         `json:"nodes"`
	TotalSplits     int         `json:"totalSplits"`
	QueuedSplits    int         `json:"queuedSplits"`
	RunningSplits   int         `json:"runningSplits"`
	CompletedSplits int         `json:"completedSplits"`
	UserTimeMillis  int         `json:"userTimeMillis"`
	CPUTimeMillis   int         `json:"cpuTimeMillis"`
	WallTimeMillis  int         `json:"wallTimeMillis"`
	ProcessedRows   int         `json:"processedRows"`
	ProcessedBytes  int         `json:"processedBytes"`
	SubStages       []stmtStage `json:"subStages"`
}

func (st *driverStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, driver.ErrSkip
}

func (st *driverStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	sr, err := st.exec(ctx, args)
	if err != nil {
		return nil, err
	}
	rows := &driverRows{
		ctx:     ctx,
		stmt:    st,
		queryID: sr.ID,
		nextURI: sr.NextURI,
		statsCh: st.statsCh,
		doneCh:  st.doneCh,
	}
	if err = rows.fetch(); err != nil && err != io.EOF {
		return nil, err
	}
	return rows, nil
}

func (st *driverStmt) exec(ctx context.Context, args []driver.NamedValue) (*stmtResponse, error) {
	query := st.query
	hs := make(http.Header)
	// Ensure the server returns timestamps preserving their precision, without truncating them to timestamp(3).
	hs.Add("X-Trino-Client-Capabilities", "PARAMETRIC_DATETIME")

	if len(args) > 0 {
		var ss []string
		for _, arg := range args {
			if arg.Name == trinoProgressCallbackParam {
				st.conn.progressUpdater = arg.Value.(ProgressUpdater)
				continue
			}
			if arg.Name == trinoProgressCallbackPeriodParam {
				st.conn.progressUpdaterPeriod.Period = arg.Value.(time.Duration)
				continue
			}

			s, err := Serial(arg.Value)
			if err != nil {
				return nil, err
			}

			if strings.HasPrefix(arg.Name, trinoHeaderPrefix) {
				headerValue := arg.Value.(string)

				if arg.Name == trinoUserHeader {
					st.user = headerValue
				}

				hs.Add(arg.Name, headerValue)
			} else {
				if hs.Get(preparedStatementHeader) == "" {
					for _, v := range st.conn.httpHeaders.Values(preparedStatementHeader) {
						hs.Add(preparedStatementHeader, v)
					}
					hs.Add(preparedStatementHeader, preparedStatementName+"="+url.QueryEscape(st.query))
				}
				ss = append(ss, s)
			}
		}
		if (st.conn.progressUpdater != nil && st.conn.progressUpdaterPeriod.Period == 0) || (st.conn.progressUpdater == nil && st.conn.progressUpdaterPeriod.Period > 0) {
			return nil, ErrInvalidProgressCallbackHeader
		}
		if len(ss) > 0 {
			query = "EXECUTE " + preparedStatementName + " USING " + strings.Join(ss, ", ")
		}
	}

	req, err := st.conn.newRequest("POST", st.conn.baseURL+"/v1/statement", strings.NewReader(query), hs)
	if err != nil {
		return nil, err
	}

	resp, err := st.conn.roundTrip(ctx, req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	var sr stmtResponse
	d := json.NewDecoder(resp.Body)
	d.UseNumber()
	err = d.Decode(&sr)
	if err != nil {
		return nil, fmt.Errorf("trino: %v", err)
	}

	st.doneCh = make(chan struct{})
	st.nextURIs = make(chan string)
	st.httpResponses = make(chan *http.Response)
	st.queryResponses = make(chan queryResponse)
	st.errors = make(chan error)
	go func() {
		defer close(st.httpResponses)
		for {
			select {
			case nextURI := <-st.nextURIs:
				if nextURI == "" {
					return
				}
				hs := make(http.Header)
				hs.Add(trinoUserHeader, st.user)
				req, err := st.conn.newRequest("GET", nextURI, nil, hs)
				if err != nil {
					st.errors <- err
					return
				}
				resp, err := st.conn.roundTrip(ctx, req)
				if err != nil {
					if ctx.Err() == context.Canceled {
						st.errors <- context.Canceled
						return
					}
					st.errors <- err
					return
				}
				select {
				case st.httpResponses <- resp:
				case <-st.doneCh:
					return
				}
			case <-st.doneCh:
				return
			}
		}
	}()
	go func() {
		defer close(st.queryResponses)
		for {
			select {
			case resp := <-st.httpResponses:
				if resp == nil {
					return
				}
				var qresp queryResponse
				d := json.NewDecoder(resp.Body)
				d.UseNumber()
				err = d.Decode(&qresp)
				if err != nil {
					st.errors <- fmt.Errorf("trino: %v", err)
					return
				}
				err = resp.Body.Close()
				if err != nil {
					st.errors <- err
					return
				}
				err = handleResponseError(resp.StatusCode, qresp.Error)
				if err != nil {
					st.errors <- err
					return
				}
				select {
				case st.nextURIs <- qresp.NextURI:
				case <-st.doneCh:
					return
				}
				select {
				case st.queryResponses <- qresp:
				case <-st.doneCh:
					return
				}
			case <-st.doneCh:
				return
			}
		}
	}()
	st.nextURIs <- sr.NextURI
	if st.conn.progressUpdater != nil {
		st.statsCh = make(chan QueryProgressInfo)

		// progress updater go func
		go func() {
			for {
				select {
				case stats := <-st.statsCh:
					st.conn.progressUpdater.Update(stats)
				case <-st.doneCh:
					close(st.statsCh)
					return
				}
			}
		}()

		// initial progress callback call
		srStats := QueryProgressInfo{
			QueryId:    sr.ID,
			QueryStats: sr.Stats,
		}
		select {
		case st.statsCh <- srStats:
		default:
			// ignore when can't send stats
		}
		st.conn.progressUpdaterPeriod.LastCallbackTime = time.Now()
		st.conn.progressUpdaterPeriod.LastQueryState = sr.Stats.State
	}
	return &sr, handleResponseError(resp.StatusCode, sr.Error)
}

type driverRows struct {
	ctx     context.Context
	stmt    *driverStmt
	queryID string
	nextURI string

	err          error
	rowindex     int
	columns      []string
	coltype      []*typeConverter
	data         []queryData
	rowsAffected int64

	statsCh chan QueryProgressInfo
	doneCh  chan struct{}
}

var _ driver.Rows = &driverRows{}
var _ driver.Result = &driverRows{}
var _ driver.RowsColumnTypeScanType = &driverRows{}
var _ driver.RowsColumnTypeDatabaseTypeName = &driverRows{}
var _ driver.RowsColumnTypeLength = &driverRows{}
var _ driver.RowsColumnTypePrecisionScale = &driverRows{}

// Close closes the rows iterator.
func (qr *driverRows) Close() error {
	if qr.err == sql.ErrNoRows || qr.err == io.EOF {
		return nil
	}
	qr.err = io.EOF
	hs := make(http.Header)
	if qr.stmt.user != "" {
		hs.Add(trinoUserHeader, qr.stmt.user)
	}
	req, err := qr.stmt.conn.newRequest("DELETE", qr.stmt.conn.baseURL+"/v1/query/"+url.PathEscape(qr.queryID), nil, hs)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), DefaultCancelQueryTimeout)
	defer cancel()
	resp, err := qr.stmt.conn.roundTrip(ctx, req)
	if err != nil {
		qferr, ok := err.(*ErrQueryFailed)
		if ok && qferr.StatusCode == http.StatusNoContent {
			qr.nextURI = ""
			return nil
		}
		return err
	}
	resp.Body.Close()
	return qr.err
}

// Columns returns the names of the columns.
func (qr *driverRows) Columns() []string {
	if qr.err != nil {
		return []string{}
	}
	if qr.columns == nil {
		if err := qr.fetch(); err != nil && err != io.EOF {
			qr.err = err
			return []string{}
		}
	}
	return qr.columns
}

func (qr *driverRows) ColumnTypeDatabaseTypeName(index int) string {
	typeName := qr.coltype[index].parsedType[0]
	if typeName == "map" || typeName == "array" || typeName == "row" {
		typeName = qr.coltype[index].typeName
	}
	return strings.ToUpper(typeName)
}

func (qr *driverRows) ColumnTypeScanType(index int) reflect.Type {
	return qr.coltype[index].scanType
}

func (qr *driverRows) ColumnTypeLength(index int) (int64, bool) {
	return qr.coltype[index].size.value, qr.coltype[index].size.hasValue
}

func (qr *driverRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	return qr.coltype[index].precision.value, qr.coltype[index].scale.value, qr.coltype[index].precision.hasValue
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
func (qr *driverRows) Next(dest []driver.Value) error {
	if qr.err != nil {
		return qr.err
	}
	if qr.columns == nil || qr.rowindex >= len(qr.data) {
		if qr.nextURI == "" {
			qr.err = io.EOF
			return qr.err
		}
		if err := qr.fetch(); err != nil {
			qr.err = err
			return err
		}
	}
	if len(qr.coltype) == 0 {
		qr.err = sql.ErrNoRows
		return qr.err
	}
	for i, v := range qr.coltype {
		if i > len(dest)-1 {
			break
		}
		vv, err := v.ConvertValue(qr.data[qr.rowindex][i])
		if err != nil {
			qr.err = err
			return err
		}
		dest[i] = vv
	}
	qr.rowindex++
	return nil
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (qr driverRows) LastInsertId() (int64, error) {
	return 0, ErrOperationNotSupported
}

// RowsAffected returns the number of rows affected by the query.
func (qr driverRows) RowsAffected() (int64, error) {
	return qr.rowsAffected, nil
}

type queryResponse struct {
	ID               string        `json:"id"`
	InfoURI          string        `json:"infoUri"`
	PartialCancelURI string        `json:"partialCancelUri"`
	NextURI          string        `json:"nextUri"`
	Columns          []queryColumn `json:"columns"`
	Data             []queryData   `json:"data"`
	Stats            stmtStats     `json:"stats"`
	Error            stmtError     `json:"error"`
	UpdateType       string        `json:"updateType"`
	UpdateCount      int64         `json:"updateCount"`
}

type queryColumn struct {
	Name          string        `json:"name"`
	Type          string        `json:"type"`
	TypeSignature typeSignature `json:"typeSignature"`
}

type queryData []interface{}

type namedTypeSignature struct {
	FieldName     rowFieldName  `json:"fieldName"`
	TypeSignature typeSignature `json:"typeSignature"`
}

type rowFieldName struct {
	Name string `json:"name"`
}

type typeSignature struct {
	RawType   string         `json:"rawType"`
	Arguments []typeArgument `json:"arguments"`
}

type typeKind string

const (
	KIND_TYPE       = typeKind("TYPE")
	KIND_NAMED_TYPE = typeKind("NAMED_TYPE")
	KIND_LONG       = typeKind("LONG")
	KIND_VARIABLE   = typeKind("VARIABLE")
)

type typeArgument struct {
	// Kind determines if the typeSignature, namedTypeSignature, or long field has a value
	Kind  typeKind        `json:"kind"`
	Value json.RawMessage `json:"value"`
	// typeSignature decoded from Value when Kind is TYPE
	typeSignature typeSignature
	// namedTypeSignature decoded from Value when Kind is NAMED_TYPE
	namedTypeSignature namedTypeSignature
	// long decoded from Value when Kind is LONG
	long int64
}

func handleResponseError(status int, respErr stmtError) error {
	switch respErr.ErrorName {
	case "":
		return nil
	case "USER_CANCELLED":
		return ErrQueryCancelled
	default:
		return &ErrQueryFailed{
			StatusCode: status,
			Reason:     &respErr,
		}
	}
}

func (qr *driverRows) fetch() error {
	var qresp queryResponse
	var err error
	for {
		select {
		case qresp = <-qr.stmt.queryResponses:
			if qresp.ID == "" {
				return io.EOF
			}
			err = qr.initColumns(&qresp)
			if err != nil {
				return err
			}
			qr.rowindex = 0
			qr.data = qresp.Data
			qr.rowsAffected = qresp.UpdateCount
			qr.scheduleProgressUpdate(qresp.ID, qresp.Stats)
			if len(qr.data) != 0 {
				return nil
			}
		case err = <-qr.stmt.errors:
			if err == nil {
				// Channel was closed, which means the statement
				// or rows were closed.
				err = io.EOF
			} else if err == context.Canceled {
				qr.Close()
			}
			qr.err = err
			return err
		}
	}
}

func unmarshalArguments(signature *typeSignature) error {
	for i, argument := range signature.Arguments {
		var payload interface{}
		switch argument.Kind {
		case KIND_TYPE:
			payload = &(signature.Arguments[i].typeSignature)
		case KIND_NAMED_TYPE:
			payload = &(signature.Arguments[i].namedTypeSignature)
		case KIND_LONG:
			payload = &(signature.Arguments[i].long)
		}
		err := json.Unmarshal(argument.Value, payload)
		if err != nil {
			return err
		}
		switch argument.Kind {
		case KIND_TYPE:
			err = unmarshalArguments(&(signature.Arguments[i].typeSignature))
		case KIND_NAMED_TYPE:
			err = unmarshalArguments(&(signature.Arguments[i].namedTypeSignature.TypeSignature))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (qr *driverRows) initColumns(qresp *queryResponse) error {
	if qr.columns != nil || len(qresp.Columns) == 0 {
		return nil
	}
	var err error
	for i := range qresp.Columns {
		err = unmarshalArguments(&(qresp.Columns[i].TypeSignature))
		if err != nil {
			return fmt.Errorf("error decoding column type signature: %w", err)
		}
	}
	qr.columns = make([]string, len(qresp.Columns))
	qr.coltype = make([]*typeConverter, len(qresp.Columns))
	for i, col := range qresp.Columns {
		err = unmarshalArguments(&(qresp.Columns[i].TypeSignature))
		if err != nil {
			return fmt.Errorf("error decoding column type signature: %w", err)
		}
		qr.columns[i] = col.Name
		qr.coltype[i], err = newTypeConverter(col.Type, col.TypeSignature)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qr *driverRows) scheduleProgressUpdate(id string, stats stmtStats) {
	if qr.stmt.conn.progressUpdater == nil {
		return
	}

	qrStats := QueryProgressInfo{
		QueryId:    id,
		QueryStats: stats,
	}
	currentTime := time.Now()
	diff := currentTime.Sub(qr.stmt.conn.progressUpdaterPeriod.LastCallbackTime)
	period := qr.stmt.conn.progressUpdaterPeriod.Period

	// Check if period has not passed yet AND if query state did not change
	if diff < period && qr.stmt.conn.progressUpdaterPeriod.LastQueryState == qrStats.QueryStats.State {
		return
	}

	select {
	case qr.statsCh <- qrStats:
	default:
		// ignore when can't send stats
	}
	qr.stmt.conn.progressUpdaterPeriod.LastCallbackTime = currentTime
	qr.stmt.conn.progressUpdaterPeriod.LastQueryState = qrStats.QueryStats.State
}

type typeConverter struct {
	typeName   string
	parsedType []string
	scanType   reflect.Type
	precision  optionalInt64
	scale      optionalInt64
	size       optionalInt64
}

type optionalInt64 struct {
	value    int64
	hasValue bool
}

func newOptionalInt64(value int64) optionalInt64 {
	return optionalInt64{value: value, hasValue: true}
}

func newTypeConverter(typeName string, signature typeSignature) (*typeConverter, error) {
	result := &typeConverter{
		typeName:   typeName,
		parsedType: getNestedTypes([]string{}, signature),
	}
	var err error
	result.scanType, err = getScanType(result.parsedType)
	if err != nil {
		return nil, err
	}
	switch signature.RawType {
	case "char", "varchar":
		if len(signature.Arguments) > 0 {
			if signature.Arguments[0].Kind != KIND_LONG {
				return nil, ErrInvalidResponseType
			}
			result.size = newOptionalInt64(signature.Arguments[0].long)
		}
	case "decimal":
		if len(signature.Arguments) > 0 {
			if signature.Arguments[0].Kind != KIND_LONG {
				return nil, ErrInvalidResponseType
			}
			result.precision = newOptionalInt64(signature.Arguments[0].long)
		}
		if len(signature.Arguments) > 1 {
			if signature.Arguments[1].Kind != KIND_LONG {
				return nil, ErrInvalidResponseType
			}
			result.scale = newOptionalInt64(signature.Arguments[1].long)
		}
	case "time", "time with time zone", "timestamp", "timestamp with time zone":
		if len(signature.Arguments) > 0 {
			if signature.Arguments[0].Kind != KIND_LONG {
				return nil, ErrInvalidResponseType
			}
			result.precision = newOptionalInt64(signature.Arguments[0].long)
		}
	}

	return result, nil
}

func getNestedTypes(types []string, signature typeSignature) []string {
	types = append(types, signature.RawType)
	if len(signature.Arguments) == 1 {
		switch signature.Arguments[0].Kind {
		case KIND_TYPE:
			types = getNestedTypes(types, signature.Arguments[0].typeSignature)
		case KIND_NAMED_TYPE:
			types = getNestedTypes(types, signature.Arguments[0].namedTypeSignature.TypeSignature)
		}
	}
	return types
}

func getScanType(typeNames []string) (reflect.Type, error) {
	var v interface{}
	switch typeNames[0] {
	case "boolean":
		v = sql.NullBool{}
	case "json", "char", "varchar", "varbinary", "interval year to month", "interval day to second", "decimal", "ipaddress", "uuid", "unknown":
		v = sql.NullString{}
	case "tinyint", "smallint":
		v = sql.NullInt32{}
	case "integer":
		v = sql.NullInt32{}
	case "bigint":
		v = sql.NullInt64{}
	case "real", "double":
		v = sql.NullFloat64{}
	case "date", "time", "time with time zone", "timestamp", "timestamp with time zone":
		v = sql.NullTime{}
	case "map":
		v = NullMap{}
	case "array":
		if len(typeNames) <= 1 {
			return nil, ErrInvalidResponseType
		}
		switch typeNames[1] {
		case "boolean":
			v = NullSliceBool{}
		case "json", "char", "varchar", "varbinary", "interval year to month", "interval day to second", "decimal", "ipaddress", "uuid", "unknown":
			v = NullSliceString{}
		case "tinyint", "smallint", "integer", "bigint":
			v = NullSliceInt64{}
		case "real", "double":
			v = NullSliceFloat64{}
		case "date", "time", "time with time zone", "timestamp", "timestamp with time zone":
			v = NullSliceTime{}
		case "map":
			v = NullSliceMap{}
		case "array":
			if len(typeNames) <= 2 {
				return nil, ErrInvalidResponseType
			}
			switch typeNames[2] {
			case "boolean":
				v = NullSlice2Bool{}
			case "json", "char", "varchar", "varbinary", "interval year to month", "interval day to second", "decimal", "ipaddress", "uuid", "unknown":
				v = NullSlice2String{}
			case "tinyint", "smallint", "integer", "bigint":
				v = NullSlice2Int64{}
			case "real", "double":
				v = NullSlice2Float64{}
			case "date", "time", "time with time zone", "timestamp", "timestamp with time zone":
				v = NullSlice2Time{}
			case "map":
				v = NullSlice2Map{}
			case "array":
				if len(typeNames) <= 3 {
					return nil, ErrInvalidResponseType
				}
				switch typeNames[3] {
				case "boolean":
					v = NullSlice3Bool{}
				case "json", "char", "varchar", "varbinary", "interval year to month", "interval day to second", "decimal", "ipaddress", "uuid", "unknown":
					v = NullSlice3String{}
				case "tinyint", "smallint", "integer", "bigint":
					v = NullSlice3Int64{}
				case "real", "double":
					v = NullSlice3Float64{}
				case "date", "time", "time with time zone", "timestamp", "timestamp with time zone":
					v = NullSlice3Time{}
				case "map":
					v = NullSlice3Map{}
				}
				// if this is a 4 or more dimensional array, scan type will be an empty interface
			}
		}
	}
	if v == nil {
		return reflect.TypeOf(new(interface{})).Elem(), nil
	}
	return reflect.TypeOf(v), nil
}

// ConvertValue implements the driver.ValueConverter interface.
func (c *typeConverter) ConvertValue(v interface{}) (driver.Value, error) {
	switch c.parsedType[0] {
	case "boolean":
		vv, err := scanNullBool(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.Bool, err
	case "json", "char", "varchar", "varbinary", "interval year to month", "interval day to second", "decimal", "ipaddress", "uuid", "unknown":
		vv, err := scanNullString(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.String, err
	case "tinyint", "smallint", "integer", "bigint":
		vv, err := scanNullInt64(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.Int64, err
	case "real", "double":
		vv, err := scanNullFloat64(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.Float64, err
	case "date", "time", "time with time zone", "timestamp", "timestamp with time zone":
		vv, err := scanNullTime(v)
		if !vv.Valid {
			return nil, err
		}
		return vv.Time, err
	case "map":
		if err := validateMap(v); err != nil {
			return nil, err
		}
		return v, nil
	case "array":
		if err := validateSlice(v); err != nil {
			return nil, err
		}
		return v, nil
	case "row":
		if err := validateSlice(v); err != nil {
			return nil, err
		}
		return v, nil
	default:
		return nil, fmt.Errorf("type not supported: %q", c.typeName)
	}
}

func validateMap(v interface{}) error {
	if v == nil {
		return nil
	}
	if _, ok := v.(map[string]interface{}); !ok {
		return fmt.Errorf("cannot convert %v (%T) to map", v, v)
	}
	return nil
}

func validateSlice(v interface{}) error {
	if v == nil {
		return nil
	}
	if _, ok := v.([]interface{}); !ok {
		return fmt.Errorf("cannot convert %v (%T) to slice", v, v)
	}
	return nil
}

func scanNullBool(v interface{}) (sql.NullBool, error) {
	if v == nil {
		return sql.NullBool{}, nil
	}
	vv, ok := v.(bool)
	if !ok {
		return sql.NullBool{},
			fmt.Errorf("cannot convert %v (%T) to bool", v, v)
	}
	return sql.NullBool{Valid: true, Bool: vv}, nil
}

// NullSliceBool represents a slice of bool that may be null.
type NullSliceBool struct {
	SliceBool []sql.NullBool
	Valid     bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceBool) Scan(value interface{}) error {
	if value == nil {
		s.SliceBool, s.Valid = []sql.NullBool{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to []bool", value, value)
	}
	slice := make([]sql.NullBool, len(vs))
	for i := range vs {
		v, err := scanNullBool(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceBool = slice
	s.Valid = true
	return nil
}

// NullSlice2Bool represents a two-dimensional slice of bool that may be null.
type NullSlice2Bool struct {
	Slice2Bool [][]sql.NullBool
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Bool) Scan(value interface{}) error {
	if value == nil {
		s.Slice2Bool, s.Valid = [][]sql.NullBool{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][]bool", value, value)
	}
	slice := make([][]sql.NullBool, len(vs))
	for i := range vs {
		var ss NullSliceBool
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceBool
	}
	s.Slice2Bool = slice
	s.Valid = true
	return nil
}

// NullSlice3Bool implements a three-dimensional slice of bool that may be null.
type NullSlice3Bool struct {
	Slice3Bool [][][]sql.NullBool
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Bool) Scan(value interface{}) error {
	if value == nil {
		s.Slice3Bool, s.Valid = [][][]sql.NullBool{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][][]bool", value, value)
	}
	slice := make([][][]sql.NullBool, len(vs))
	for i := range vs {
		var ss NullSlice2Bool
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Bool
	}
	s.Slice3Bool = slice
	s.Valid = true
	return nil
}

func scanNullString(v interface{}) (sql.NullString, error) {
	if v == nil {
		return sql.NullString{}, nil
	}
	vv, ok := v.(string)
	if !ok {
		return sql.NullString{},
			fmt.Errorf("cannot convert %v (%T) to string", v, v)
	}
	return sql.NullString{Valid: true, String: vv}, nil
}

// NullSliceString represents a slice of string that may be null.
type NullSliceString struct {
	SliceString []sql.NullString
	Valid       bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceString) Scan(value interface{}) error {
	if value == nil {
		s.SliceString, s.Valid = []sql.NullString{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to []string", value, value)
	}
	slice := make([]sql.NullString, len(vs))
	for i := range vs {
		v, err := scanNullString(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceString = slice
	s.Valid = true
	return nil
}

// NullSlice2String represents a two-dimensional slice of string that may be null.
type NullSlice2String struct {
	Slice2String [][]sql.NullString
	Valid        bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2String) Scan(value interface{}) error {
	if value == nil {
		s.Slice2String, s.Valid = [][]sql.NullString{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][]string", value, value)
	}
	slice := make([][]sql.NullString, len(vs))
	for i := range vs {
		var ss NullSliceString
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceString
	}
	s.Slice2String = slice
	s.Valid = true
	return nil
}

// NullSlice3String implements a three-dimensional slice of string that may be null.
type NullSlice3String struct {
	Slice3String [][][]sql.NullString
	Valid        bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3String) Scan(value interface{}) error {
	if value == nil {
		s.Slice3String, s.Valid = [][][]sql.NullString{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][][]string", value, value)
	}
	slice := make([][][]sql.NullString, len(vs))
	for i := range vs {
		var ss NullSlice2String
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2String
	}
	s.Slice3String = slice
	s.Valid = true
	return nil
}

func scanNullInt64(v interface{}) (sql.NullInt64, error) {
	if v == nil {
		return sql.NullInt64{}, nil
	}
	vNumber, ok := v.(json.Number)
	if !ok {
		return sql.NullInt64{},
			fmt.Errorf("cannot convert %v (%T) to int64", v, v)
	}
	vv, err := vNumber.Int64()
	if err != nil {
		return sql.NullInt64{},
			fmt.Errorf("cannot convert %v (%T) to int64", v, v)
	}
	return sql.NullInt64{Valid: true, Int64: vv}, nil
}

// NullSliceInt64 represents a slice of int64 that may be null.
type NullSliceInt64 struct {
	SliceInt64 []sql.NullInt64
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceInt64) Scan(value interface{}) error {
	if value == nil {
		s.SliceInt64, s.Valid = []sql.NullInt64{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to []int64", value, value)
	}
	slice := make([]sql.NullInt64, len(vs))
	for i := range vs {
		v, err := scanNullInt64(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceInt64 = slice
	s.Valid = true
	return nil
}

// NullSlice2Int64 represents a two-dimensional slice of int64 that may be null.
type NullSlice2Int64 struct {
	Slice2Int64 [][]sql.NullInt64
	Valid       bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Int64) Scan(value interface{}) error {
	if value == nil {
		s.Slice2Int64, s.Valid = [][]sql.NullInt64{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][]int64", value, value)
	}
	slice := make([][]sql.NullInt64, len(vs))
	for i := range vs {
		var ss NullSliceInt64
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceInt64
	}
	s.Slice2Int64 = slice
	s.Valid = true
	return nil
}

// NullSlice3Int64 implements a three-dimensional slice of int64 that may be null.
type NullSlice3Int64 struct {
	Slice3Int64 [][][]sql.NullInt64
	Valid       bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Int64) Scan(value interface{}) error {
	if value == nil {
		s.Slice3Int64, s.Valid = [][][]sql.NullInt64{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][][]int64", value, value)
	}
	slice := make([][][]sql.NullInt64, len(vs))
	for i := range vs {
		var ss NullSlice2Int64
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Int64
	}
	s.Slice3Int64 = slice
	s.Valid = true
	return nil
}

func scanNullFloat64(v interface{}) (sql.NullFloat64, error) {
	if v == nil {
		return sql.NullFloat64{}, nil
	}
	vNumber, ok := v.(json.Number)
	if ok {
		vFloat, err := vNumber.Float64()
		if err != nil {
			return sql.NullFloat64{}, fmt.Errorf("cannot convert %v (%T) to float64", vNumber, vNumber)
		}
		return sql.NullFloat64{Valid: true, Float64: vFloat}, nil
	}
	switch v {
	case "NaN":
		return sql.NullFloat64{Valid: true, Float64: math.NaN()}, nil
	case "Infinity":
		return sql.NullFloat64{Valid: true, Float64: math.Inf(+1)}, nil
	case "-Infinity":
		return sql.NullFloat64{Valid: true, Float64: math.Inf(-1)}, nil
	default:
		return sql.NullFloat64{}, fmt.Errorf("cannot convert %v (%T) to float64", v, v)
	}
}

// NullSliceFloat64 represents a slice of float64 that may be null.
type NullSliceFloat64 struct {
	SliceFloat64 []sql.NullFloat64
	Valid        bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceFloat64) Scan(value interface{}) error {
	if value == nil {
		s.SliceFloat64, s.Valid = []sql.NullFloat64{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to []float64", value, value)
	}
	slice := make([]sql.NullFloat64, len(vs))
	for i := range vs {
		v, err := scanNullFloat64(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceFloat64 = slice
	s.Valid = true
	return nil
}

// NullSlice2Float64 represents a two-dimensional slice of float64 that may be null.
type NullSlice2Float64 struct {
	Slice2Float64 [][]sql.NullFloat64
	Valid         bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Float64) Scan(value interface{}) error {
	if value == nil {
		s.Slice2Float64, s.Valid = [][]sql.NullFloat64{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][]float64", value, value)
	}
	slice := make([][]sql.NullFloat64, len(vs))
	for i := range vs {
		var ss NullSliceFloat64
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceFloat64
	}
	s.Slice2Float64 = slice
	s.Valid = true
	return nil
}

// NullSlice3Float64 represents a three-dimensional slice of float64 that may be null.
type NullSlice3Float64 struct {
	Slice3Float64 [][][]sql.NullFloat64
	Valid         bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Float64) Scan(value interface{}) error {
	if value == nil {
		s.Slice3Float64, s.Valid = [][][]sql.NullFloat64{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][][]float64", value, value)
	}
	slice := make([][][]sql.NullFloat64, len(vs))
	for i := range vs {
		var ss NullSlice2Float64
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Float64
	}
	s.Slice3Float64 = slice
	s.Valid = true
	return nil
}

// Layout for time and timestamp WITHOUT time zone.
// Trino can support up to 12 digits sub second precision, but Go only 9.
// (Requires X-Trino-Client-Capabilities: PARAMETRIC_DATETIME)
var timeLayouts = []string{
	"2006-01-02",
	"15:04:05.999999999",
	"2006-01-02 15:04:05.999999999",
}

// Layout for time and timestamp WITH time zone.
// Trino can support up to 12 digits sub second precision, but Go only 9.
// (Requires X-Trino-Client-Capabilities: PARAMETRIC_DATETIME)
var timeLayoutsTZ = []string{
	"15:04:05.999999999 -07:00",
	"2006-01-02 15:04:05.999999999 -07:00",
}

func scanNullTime(v interface{}) (NullTime, error) {
	if v == nil {
		return NullTime{}, nil
	}
	vv, ok := v.(string)
	if !ok {
		return NullTime{}, fmt.Errorf("cannot convert %v (%T) to time string", v, v)
	}
	vparts := strings.Split(vv, " ")
	if len(vparts) > 1 && !unicode.IsDigit(rune(vparts[len(vparts)-1][0])) {
		return parseNullTimeWithLocation(vv)
	}
	// Time literals may not have spaces before the timezone.
	if strings.ContainsRune(vv, '+') {
		return parseNullTimeWithLocation(strings.Replace(vv, "+", " +", 1))
	}
	hyphenCount := strings.Count(vv, "-")
	// We need to ensure we don't treat the hyphens in dates as the minus offset sign.
	// So if there's only one hyphen or more than 2, we have a negative offset.
	if hyphenCount == 1 || hyphenCount > 2 {
		// We add a space before the last hyphen to parse properly.
		i := strings.LastIndex(vv, "-")
		timestamp := vv[:i] + strings.Replace(vv[i:], "-", " -", 1)
		return parseNullTimeWithLocation(timestamp)
	}
	return parseNullTime(vv)
}

func parseNullTime(v string) (NullTime, error) {
	var t time.Time
	var err error
	for _, layout := range timeLayouts {
		t, err = time.ParseInLocation(layout, v, time.Local)
		if err == nil {
			return NullTime{Valid: true, Time: t}, nil
		}
	}
	return NullTime{}, err
}

func parseNullTimeWithLocation(v string) (NullTime, error) {
	idx := strings.LastIndex(v, " ")
	if idx == -1 {
		return NullTime{}, fmt.Errorf("cannot convert %v (%T) to time+zone", v, v)
	}
	stamp, location := v[:idx], v[idx+1:]
	var t time.Time
	var err error
	// Try offset timezones.
	if strings.HasPrefix(location, "+") || strings.HasPrefix(location, "-") {
		for _, layout := range timeLayoutsTZ {
			t, err = time.Parse(layout, v)
			if err == nil {
				return NullTime{Valid: true, Time: t}, nil
			}
		}
		return NullTime{}, err
	}
	loc, err := time.LoadLocation(location)
	// Not a named location.
	if err != nil {
		return NullTime{}, fmt.Errorf("cannot load timezone %q: %v", location, err)
	}

	for _, layout := range timeLayouts {
		t, err = time.ParseInLocation(layout, stamp, loc)
		if err == nil {
			return NullTime{Valid: true, Time: t}, nil
		}
	}
	return NullTime{}, err
}

// NullTime represents a time.Time value that can be null.
// The NullTime supports Trino's Date, Time and Timestamp data types,
// with or without time zone.
type NullTime struct {
	Time  time.Time
	Valid bool
}

// Scan implements the sql.Scanner interface.
func (s *NullTime) Scan(value interface{}) error {
	if value == nil {
		s.Time, s.Valid = time.Time{}, false
		return nil
	}
	switch t := value.(type) {
	case time.Time:
		s.Time, s.Valid = t, true
	case NullTime:
		*s = t
	}
	return nil
}

// NullSliceTime represents a slice of time.Time that may be null.
type NullSliceTime struct {
	SliceTime []NullTime
	Valid     bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceTime) Scan(value interface{}) error {
	if value == nil {
		s.SliceTime, s.Valid = []NullTime{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to []time.Time", value, value)
	}
	slice := make([]NullTime, len(vs))
	for i := range vs {
		v, err := scanNullTime(vs[i])
		if err != nil {
			return err
		}
		slice[i] = v
	}
	s.SliceTime = slice
	s.Valid = true
	return nil
}

// NullSlice2Time represents a two-dimensional slice of time.Time that may be null.
type NullSlice2Time struct {
	Slice2Time [][]NullTime
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Time) Scan(value interface{}) error {
	if value == nil {
		s.Slice2Time, s.Valid = [][]NullTime{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][]time.Time", value, value)
	}
	slice := make([][]NullTime, len(vs))
	for i := range vs {
		var ss NullSliceTime
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceTime
	}
	s.Slice2Time = slice
	s.Valid = true
	return nil
}

// NullSlice3Time represents a three-dimensional slice of time.Time that may be null.
type NullSlice3Time struct {
	Slice3Time [][][]NullTime
	Valid      bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Time) Scan(value interface{}) error {
	if value == nil {
		s.Slice3Time, s.Valid = [][][]NullTime{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][][]time.Time", value, value)
	}
	slice := make([][][]NullTime, len(vs))
	for i := range vs {
		var ss NullSlice2Time
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Time
	}
	s.Slice3Time = slice
	s.Valid = true
	return nil
}

// NullMap represents a map type that may be null.
type NullMap struct {
	Map   map[string]interface{}
	Valid bool
}

// Scan implements the sql.Scanner interface.
func (m *NullMap) Scan(v interface{}) error {
	if v == nil {
		m.Map, m.Valid = map[string]interface{}{}, false
		return nil
	}
	m.Map, m.Valid = v.(map[string]interface{})
	return nil
}

// NullSliceMap represents a slice of NullMap that may be null.
type NullSliceMap struct {
	SliceMap []NullMap
	Valid    bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSliceMap) Scan(value interface{}) error {
	if value == nil {
		s.SliceMap, s.Valid = []NullMap{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to []NullMap", value, value)
	}
	slice := make([]NullMap, len(vs))
	for i := range vs {
		if err := validateMap(vs[i]); err != nil {
			return fmt.Errorf("cannot convert %v (%T) to []NullMap", value, value)
		}
		m := NullMap{}
		// this scan can never fail
		_ = m.Scan(vs[i])
		slice[i] = m
	}
	s.SliceMap = slice
	s.Valid = true
	return nil
}

// NullSlice2Map represents a two-dimensional slice of NullMap that may be null.
type NullSlice2Map struct {
	Slice2Map [][]NullMap
	Valid     bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice2Map) Scan(value interface{}) error {
	if value == nil {
		s.Slice2Map, s.Valid = [][]NullMap{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][]NullMap", value, value)
	}
	slice := make([][]NullMap, len(vs))
	for i := range vs {
		var ss NullSliceMap
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.SliceMap
	}
	s.Slice2Map = slice
	s.Valid = true
	return nil
}

// NullSlice3Map represents a three-dimensional slice of NullMap that may be null.
type NullSlice3Map struct {
	Slice3Map [][][]NullMap
	Valid     bool
}

// Scan implements the sql.Scanner interface.
func (s *NullSlice3Map) Scan(value interface{}) error {
	if value == nil {
		s.Slice3Map, s.Valid = [][][]NullMap{}, false
		return nil
	}
	vs, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("trino: cannot convert %v (%T) to [][][]NullMap", value, value)
	}
	slice := make([][][]NullMap, len(vs))
	for i := range vs {
		var ss NullSlice2Map
		if err := ss.Scan(vs[i]); err != nil {
			return err
		}
		slice[i] = ss.Slice2Map
	}
	s.Slice3Map = slice
	s.Valid = true
	return nil
}

type QueryProgressInfo struct {
	QueryId    string
	QueryStats stmtStats
}

type queryProgressCallbackPeriod struct {
	Period           time.Duration
	LastCallbackTime time.Time
	LastQueryState   string
}

type ProgressUpdater interface {
	// Update the query progress, immediately when the query starts, when receiving data, and once when the query is finished.
	Update(QueryProgressInfo)
}
