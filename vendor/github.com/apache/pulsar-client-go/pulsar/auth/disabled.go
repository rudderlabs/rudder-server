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

type disabled struct{}

// NewAuthDisabled return a interface of Provider
func NewAuthDisabled() Provider {
	return &disabled{}
}

func (disabled) Init() error {
	return nil
}

func (disabled) GetData() ([]byte, error) {
	return nil, nil
}

func (disabled) Name() string {
	return ""
}

func (disabled) GetTLSCertificate() (*tls.Certificate, error) {
	return nil, nil
}

func (disabled) Close() error {
	return nil
}

func (d disabled) RoundTrip(req *http.Request) (*http.Response, error) {
	return nil, nil
}

func (d disabled) Transport() http.RoundTripper {
	return nil
}

func (d disabled) WithTransport(tripper http.RoundTripper) error {
	return nil
}
