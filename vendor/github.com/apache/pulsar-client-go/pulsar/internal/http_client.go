// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package internal

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/auth"

	"github.com/apache/pulsar-client-go/pulsar/log"

	"github.com/pkg/errors"
)

// httpClient is a base client that is used to make http httpRequest to the ServiceURL
type httpClient struct {
	ServiceNameResolver ServiceNameResolver
	HTTPClient          *http.Client
	requestTimeout      time.Duration
	log                 log.Logger
	metrics             *Metrics
}

func (c *httpClient) Close() {
	if c.HTTPClient != nil {
		c.HTTPClient.CloseIdleConnections()
	}
}

type HTTPClient interface {
	Get(endpoint string, obj interface{}, params map[string]string) error
	Closable
}

func NewHTTPClient(serviceURL *url.URL, serviceNameResolver ServiceNameResolver, tlsConfig *TLSOptions,
	requestTimeout time.Duration, logger log.Logger, metrics *Metrics,
	authProvider auth.Provider) (HTTPClient, error) {
	h := &httpClient{
		ServiceNameResolver: serviceNameResolver,
		requestTimeout:      requestTimeout,
		log:                 logger.SubLogger(log.Fields{"serviceURL": serviceURL}),
		metrics:             metrics,
	}
	c := &http.Client{Timeout: requestTimeout}
	transport, err := getDefaultTransport(tlsConfig)
	if err != nil {
		return nil, err
	}
	c.Transport = transport
	if authProvider.Name() != "" {
		err = authProvider.WithTransport(c.Transport)
		if err != nil {
			return nil, err
		}
		c.Transport = authProvider
	}
	h.HTTPClient = c
	return h, nil
}

func (c *httpClient) newRequest(method, path string) (*httpRequest, error) {
	base, err := c.ServiceNameResolver.ResolveHost()
	if err != nil {
		return nil, err
	}

	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	req := &httpRequest{
		method: method,
		url: &url.URL{
			Scheme: base.Scheme,
			User:   base.User,
			Host:   base.Host,
			Path:   endpoint(base.Path, u.Path),
		},
		params: make(url.Values),
	}
	return req, nil
}

func (c *httpClient) doRequest(r *httpRequest) (*http.Response, error) {
	req, err := r.toHTTP()
	if err != nil {
		return nil, err
	}

	if r.contentType != "" {
		req.Header.Set("Content-Type", r.contentType)
	} else if req.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.useragent())
	hc := c.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}

	return hc.Do(req)
}

// MakeRequest can make a simple httpRequest and handle the response by yourself
func (c *httpClient) MakeRequest(method, endpoint string) (*http.Response, error) {
	req, err := c.newRequest(method, endpoint)
	if err != nil {
		return nil, err
	}

	resp, err := checkSuccessful(c.doRequest(req))
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *httpClient) Get(endpoint string, obj interface{}, params map[string]string) error {
	_, err := c.GetWithQueryParams(endpoint, obj, params, true)
	if _, ok := err.(*url.Error); ok {
		// We can retry this kind of requests over a connection error because they're
		// not specific to a particular broker.
		backoff := DefaultBackoff{100 * time.Millisecond}
		startTime := time.Now()
		var retryTime time.Duration

		for time.Since(startTime) < c.requestTimeout {
			retryTime = backoff.Next()
			c.log.Debugf("Retrying httpRequest in {%v} with timeout in {%v}", retryTime, c.requestTimeout)
			time.Sleep(retryTime)
			_, err = c.GetWithQueryParams(endpoint, obj, params, true)
			if _, ok := err.(*url.Error); ok {
				continue
			} else {
				// We either succeeded or encountered a non connection error
				break
			}
		}
	}
	return err
}

func (c *httpClient) GetWithQueryParams(endpoint string, obj interface{}, params map[string]string,
	decode bool) ([]byte, error) {
	return c.GetWithOptions(endpoint, obj, params, decode, nil)
}

//nolint:bodyclose // false positive
func (c *httpClient) GetWithOptions(endpoint string, obj interface{}, params map[string]string,
	decode bool, file io.Writer) ([]byte, error) {

	req, err := c.newRequest(http.MethodGet, endpoint)
	if err != nil {
		return nil, err
	}

	if params != nil {
		query := req.url.Query()
		for k, v := range params {
			query.Add(k, v)
		}
		req.params = query
	}

	doRequest, err := c.doRequest(req)
	defer safeRespClose(doRequest)
	resp, err := checkSuccessful(doRequest, err)
	if err != nil {
		return nil, err
	}
	defer safeRespClose(resp)

	if obj != nil {
		if err := decodeJSONBody(resp, &obj); err != nil {
			if err == io.EOF {
				return nil, nil
			}
			return nil, err
		}
	} else if !decode {
		if file != nil {
			_, err := io.Copy(file, resp.Body)
			if err != nil {
				return nil, err
			}
		} else {
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}
			return body, err
		}
	}

	return nil, err
}

func (c *httpClient) useragent() string {
	return "Pulsar-httpClient-Go-v2"
}

type httpRequest struct {
	method      string
	contentType string
	url         *url.URL
	params      url.Values

	obj  interface{}
	body io.Reader
}

func (r *httpRequest) toHTTP() (*http.Request, error) {
	r.url.RawQuery = r.params.Encode()

	// add a httpRequest body if there is one
	if r.body == nil && r.obj != nil {
		body, err := encodeJSONBody(r.obj)
		if err != nil {
			return nil, err
		}
		r.body = body
	}

	req, err := http.NewRequest(r.method, r.url.RequestURI(), r.body)
	if err != nil {
		return nil, err
	}

	req.URL.Host = r.url.Host
	req.URL.Scheme = r.url.Scheme
	req.Host = r.url.Host
	return req, nil
}

// respIsOk is used to validate a successful http status code
func respIsOk(resp *http.Response) bool {
	return resp.StatusCode >= http.StatusOK && resp.StatusCode <= http.StatusNoContent
}

// checkSuccessful checks for a valid response and parses an error
func checkSuccessful(resp *http.Response, err error) (*http.Response, error) {
	if err != nil {
		safeRespClose(resp)
		return nil, err
	}

	if !respIsOk(resp) {
		defer safeRespClose(resp)
		return nil, responseError(resp)
	}

	return resp, nil
}

func endpoint(parts ...string) string {
	return path.Join(parts...)
}

// encodeJSONBody is used to JSON encode a body
func encodeJSONBody(obj interface{}) (io.Reader, error) {
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(obj); err != nil {
		return nil, err
	}
	return buf, nil
}

// decodeJSONBody is used to JSON decode a body
func decodeJSONBody(resp *http.Response, out interface{}) error {
	if resp.ContentLength == 0 {
		return nil
	}
	dec := json.NewDecoder(resp.Body)
	return dec.Decode(out)
}

// safeRespClose is used to close a response body
func safeRespClose(resp *http.Response) {
	if resp != nil {
		// ignore error since it is closing a response body
		_ = resp.Body.Close()
	}
}

// responseError is used to parse a response into a client error
func responseError(resp *http.Response) error {
	var e error
	body, err := io.ReadAll(resp.Body)
	reason := ""
	code := resp.StatusCode
	if err != nil {
		reason = err.Error()
		return errors.Errorf("Code: %d, Reason: %s", code, reason)
	}

	err = json.Unmarshal(body, &e)
	if err != nil {
		reason = string(body)
	}

	if reason == "" {
		reason = "Unknown error"
	}

	return errors.Errorf("Code: %d, Reason: %s", code, reason)
}

func getDefaultTransport(tlsConfig *TLSOptions) (http.RoundTripper, error) {
	transport := http.DefaultTransport.(*http.Transport)
	if tlsConfig != nil {
		cfg := &tls.Config{
			InsecureSkipVerify: tlsConfig.AllowInsecureConnection,
			CipherSuites:       tlsConfig.CipherSuites,
			MinVersion:         tlsConfig.MinVersion,
			MaxVersion:         tlsConfig.MaxVersion,
		}
		if len(tlsConfig.TrustCertsFilePath) > 0 {
			rootCA, err := os.ReadFile(tlsConfig.TrustCertsFilePath)
			if err != nil {
				return nil, err
			}
			cfg.RootCAs = x509.NewCertPool()
			cfg.RootCAs.AppendCertsFromPEM(rootCA)
		}

		if tlsConfig.CertFile != "" && tlsConfig.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(tlsConfig.CertFile, tlsConfig.KeyFile)
			if err != nil {
				return nil, errors.New(err.Error())
			}
			cfg.Certificates = []tls.Certificate{cert}
		}
		transport.TLSClientConfig = cfg
	}
	transport.MaxIdleConnsPerHost = 10
	return transport, nil
}
