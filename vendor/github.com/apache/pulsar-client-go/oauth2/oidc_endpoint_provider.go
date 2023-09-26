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

package oauth2

import (
	"encoding/json"
	"net/http"
	"net/url"
	"path"

	"github.com/pkg/errors"
)

// OIDCWellKnownEndpoints holds the well known OIDC endpoints
type OIDCWellKnownEndpoints struct {
	AuthorizationEndpoint       string `json:"authorization_endpoint"`
	TokenEndpoint               string `json:"token_endpoint"`
	DeviceAuthorizationEndpoint string `json:"device_authorization_endpoint"`
}

// GetOIDCWellKnownEndpointsFromIssuerURL gets the well known endpoints for the
// passed in issuer url
func GetOIDCWellKnownEndpointsFromIssuerURL(issuerURL string) (*OIDCWellKnownEndpoints, error) {
	u, err := url.Parse(issuerURL)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse issuer url to build well known endpoints")
	}
	u.Path = path.Join(u.Path, ".well-known/openid-configuration")

	r, err := http.Get(u.String())
	if err != nil {
		return nil, errors.Wrapf(err, "could not get well known endpoints from url %s", u.String())
	}
	defer r.Body.Close()

	var wkEndpoints OIDCWellKnownEndpoints
	err = json.NewDecoder(r.Body).Decode(&wkEndpoints)
	if err != nil {
		return nil, errors.Wrap(err, "could not decode json body when getting well known endpoints")
	}

	return &wkEndpoints, nil
}
