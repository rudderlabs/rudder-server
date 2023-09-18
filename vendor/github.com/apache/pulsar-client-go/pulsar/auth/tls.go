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
	"net/http"
)

type tlsAuthProvider struct {
	certificatePath string
	privateKeyPath  string
	tlsCertSupplier func() (*tls.Certificate, error)
	T               http.RoundTripper
}

// NewAuthenticationTLSWithParams initialize the authentication provider with map param.
func NewAuthenticationTLSWithParams(params map[string]string) Provider {
	return NewAuthenticationTLS(
		params["tlsCertFile"],
		params["tlsKeyFile"],
	)
}

// NewAuthenticationTLS initialize the authentication provider
func NewAuthenticationTLS(certificatePath string, privateKeyPath string) Provider {
	return &tlsAuthProvider{
		certificatePath: certificatePath,
		privateKeyPath:  privateKeyPath,
	}
}

func NewAuthenticationFromTLSCertSupplier(tlsCertSupplier func() (*tls.Certificate, error)) Provider {
	return &tlsAuthProvider{
		tlsCertSupplier: tlsCertSupplier,
	}
}

func (p *tlsAuthProvider) Init() error {
	// Try to read certificates immediately to provide better error at startup
	_, err := p.GetTLSCertificate()
	return err
}

func (p *tlsAuthProvider) Name() string {
	return "tls"
}

func (p *tlsAuthProvider) GetTLSCertificate() (*tls.Certificate, error) {
	if p.tlsCertSupplier != nil {
		return p.tlsCertSupplier()
	}
	cert, err := tls.LoadX509KeyPair(p.certificatePath, p.privateKeyPath)
	return &cert, err
}

func (p *tlsAuthProvider) GetData() ([]byte, error) {
	return nil, nil
}

func (tlsAuthProvider) Close() error {
	return nil
}

func (p *tlsAuthProvider) RoundTrip(req *http.Request) (*http.Response, error) {
	return p.T.RoundTrip(req)
}

func (p *tlsAuthProvider) Transport() http.RoundTripper {
	return p.T
}

func (p *tlsAuthProvider) WithTransport(tripper http.RoundTripper) error {
	p.T = tripper
	return p.configTLS()
}

func (p *tlsAuthProvider) configTLS() error {
	cert, err := p.GetTLSCertificate()
	if err != nil {
		return err
	}
	transport := p.T.(*http.Transport)
	transport.TLSClientConfig.Certificates = []tls.Certificate{*cert}
	return nil
}
