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
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/pkg/errors"
)

type tokenAuthProvider struct {
	tokenSupplier func() (string, error)
	T             http.RoundTripper
}

// NewAuthenticationTokenWithParams return a interface of Provider with string map.
func NewAuthenticationTokenWithParams(params map[string]string) (Provider, error) {
	if params["token"] != "" {
		return NewAuthenticationToken(params["token"]), nil
	} else if params["file"] != "" {
		return NewAuthenticationTokenFromFile(params["file"]), nil
	} else {
		return nil, errors.New("missing configuration for token auth")
	}
}

// NewAuthenticationToken returns a token auth provider that will use the specified token to
// talk with Pulsar brokers
func NewAuthenticationToken(token string) Provider {
	return &tokenAuthProvider{
		tokenSupplier: func() (string, error) {
			if token == "" {
				return "", errors.New("empty token credentials")
			}
			return token, nil
		},
	}
}

// NewAuthenticationTokenFromSupplier returns a token auth provider that get
// the token data from a user supplied function. The function is invoked each
// time the client library needs to use a token in talking with Pulsar brokers
func NewAuthenticationTokenFromSupplier(tokenSupplier func() (string, error)) Provider {
	return &tokenAuthProvider{
		tokenSupplier: tokenSupplier,
	}
}

// NewAuthenticationTokenFromFile return a interface of a Provider with a string token file path.
func NewAuthenticationTokenFromFile(tokenFilePath string) Provider {
	return &tokenAuthProvider{
		tokenSupplier: func() (string, error) {
			data, err := os.ReadFile(tokenFilePath)
			if err != nil {
				return "", err
			}

			token := strings.Trim(string(data), " \n")
			if token == "" {
				return "", errors.New("empty token credentials")
			}
			return token, nil
		},
	}
}

func (p *tokenAuthProvider) Init() error {
	// Try to read certificates immediately to provide better error at startup
	_, err := p.GetData()
	return err
}

func (p *tokenAuthProvider) Name() string {
	return "token"
}

func (p *tokenAuthProvider) GetTLSCertificate() (*tls.Certificate, error) {
	return nil, nil
}

func (p *tokenAuthProvider) GetData() ([]byte, error) {
	t, err := p.tokenSupplier()
	if err != nil {
		return nil, err
	}
	return []byte(t), nil
}

func (p *tokenAuthProvider) Close() error {
	return nil
}

func (p *tokenAuthProvider) RoundTrip(req *http.Request) (*http.Response, error) {
	token, _ := p.tokenSupplier()
	req.Header.Add("Authorization", strings.TrimSpace(fmt.Sprintf("Bearer %s", token)))
	return p.T.RoundTrip(req)
}

func (p *tokenAuthProvider) Transport() http.RoundTripper {
	return p.T
}

func (p *tokenAuthProvider) WithTransport(tripper http.RoundTripper) error {
	p.T = tripper
	return nil
}
