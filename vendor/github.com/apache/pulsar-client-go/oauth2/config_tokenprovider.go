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

import "fmt"

type configProvider interface {
	GetTokens(identifier string) (string, string)
	SaveTokens(identifier, accessToken, refreshToken string)
}

// ConfigBackedCachingProvider wraps a configProvider in order to conform to
// the cachingProvider interface
type ConfigBackedCachingProvider struct {
	identifier string
	config     configProvider
}

// NewConfigBackedCachingProvider builds and returns a CachingTokenProvider
// that utilizes a configProvider to cache tokens
func NewConfigBackedCachingProvider(clientID, audience string, config configProvider) *ConfigBackedCachingProvider {
	return &ConfigBackedCachingProvider{
		identifier: fmt.Sprintf("%s-%s", clientID, audience),
		config:     config,
	}
}

// GetTokens gets the tokens from the cache and returns them as a TokenResult
func (c *ConfigBackedCachingProvider) GetTokens() (*TokenResult, error) {
	accessToken, refreshToken := c.config.GetTokens(c.identifier)
	return &TokenResult{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
	}, nil
}

// CacheTokens caches the id and refresh token from TokenResult in the
// configProvider
func (c *ConfigBackedCachingProvider) CacheTokens(toCache *TokenResult) error {
	c.config.SaveTokens(c.identifier, toCache.AccessToken, toCache.RefreshToken)
	return nil
}
