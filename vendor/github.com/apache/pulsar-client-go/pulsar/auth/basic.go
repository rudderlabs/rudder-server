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
	"encoding/base64"
	"errors"
	"net/http"
)

type basicAuthProvider struct {
	rt               http.RoundTripper
	commandAuthToken []byte
	httpAuthToken    string
}

func NewAuthenticationBasic(username, password string) (Provider, error) {
	if username == "" {
		return nil, errors.New("username cannot be empty")
	}
	if password == "" {
		return nil, errors.New("password cannot be empty")
	}

	commandAuthToken := []byte(username + ":" + password)
	return &basicAuthProvider{
		commandAuthToken: commandAuthToken,
		httpAuthToken:    "Basic " + base64.StdEncoding.EncodeToString(commandAuthToken),
	}, nil
}

func NewAuthenticationBasicWithParams(params map[string]string) (Provider, error) {
	return NewAuthenticationBasic(params["username"], params["password"])
}

func (b *basicAuthProvider) Init() error {
	return nil
}

func (b *basicAuthProvider) Name() string {
	return "basic"
}

func (b *basicAuthProvider) GetTLSCertificate() (*tls.Certificate, error) {
	return nil, nil
}

func (b *basicAuthProvider) GetData() ([]byte, error) {
	return b.commandAuthToken, nil
}

func (b *basicAuthProvider) Close() error {
	return nil
}

func (b *basicAuthProvider) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Add("Authorization", b.httpAuthToken)
	return b.rt.RoundTrip(req)
}

func (b *basicAuthProvider) Transport() http.RoundTripper {
	return b.rt
}

func (b *basicAuthProvider) WithTransport(tr http.RoundTripper) error {
	b.rt = tr
	return nil
}
