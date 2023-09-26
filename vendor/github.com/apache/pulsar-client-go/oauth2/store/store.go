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

package store

import (
	"errors"

	"github.com/apache/pulsar-client-go/oauth2"
)

// ErrNoAuthenticationData indicates that stored authentication data is not available
var ErrNoAuthenticationData = errors.New("authentication data is not available")

// ErrUnsupportedAuthData ndicates that stored authentication data is unusable
var ErrUnsupportedAuthData = errors.New("authentication data is not usable")

// Store is responsible for persisting authorization grants
type Store interface {
	// SaveGrant stores an authorization grant for a given audience
	SaveGrant(audience string, grant oauth2.AuthorizationGrant) error

	// LoadGrant loads an authorization grant for a given audience
	LoadGrant(audience string) (*oauth2.AuthorizationGrant, error)

	// WhoAmI returns the current user name (or an error if nobody is logged in)
	WhoAmI(audience string) (string, error)

	// Logout deletes all stored credentials
	Logout() error
}
