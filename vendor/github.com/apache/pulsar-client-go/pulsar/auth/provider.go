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

package auth

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Provider is an interface of authentication providers.
type Provider interface {
	// Init the authentication provider.
	Init() error

	// Name func returns the identifier for this authentication method.
	Name() string

	// GetTLSCertificate returns a client certificate chain, or nil if the data are not available
	GetTLSCertificate() (*tls.Certificate, error)

	// GetData returns the authentication data identifying this client that will be sent to the broker.
	GetData() ([]byte, error)

	io.Closer

	HTTPAuthProvider
}

type HTTPAuthProvider interface {
	RoundTrip(req *http.Request) (*http.Response, error)
	Transport() http.RoundTripper
	WithTransport(tripper http.RoundTripper) error
}

type HTTPTransport struct {
	T http.RoundTripper
}

// NewProvider get/create an authentication data provider which provides the data
// that this client will be sent to the broker.
// Some authentication method need to auth between each client channel. So it need
// the broker, who it will talk to.
func NewProvider(name string, params string) (Provider, error) {
	m, err := parseParams(params)
	if err != nil {
		return nil, err
	}

	switch name {
	case "":
		return NewAuthDisabled(), nil

	case "tls", "org.apache.pulsar.client.impl.auth.AuthenticationTls":
		return NewAuthenticationTLSWithParams(m), nil

	case "token", "org.apache.pulsar.client.impl.auth.AuthenticationToken":
		return NewAuthenticationTokenWithParams(m)

	case "athenz", "org.apache.pulsar.client.impl.auth.AuthenticationAthenz":
		return NewAuthenticationAthenzWithParams(m)

	case "oauth2", "org.apache.pulsar.client.impl.auth.oauth2.AuthenticationOAuth2":
		return NewAuthenticationOAuth2WithParams(m)

	case "basic", "org.apache.pulsar.client.impl.auth.AuthenticationBasic":
		return NewAuthenticationBasicWithParams(m)

	default:
		return nil, fmt.Errorf("invalid auth provider '%s'", name)
	}
}

func parseParams(params string) (map[string]string, error) {
	var mapString map[string]string
	if err := json.Unmarshal([]byte(params), &mapString); err != nil {
		return nil, err
	}

	return mapString, nil
}
