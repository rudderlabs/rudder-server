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
	"sync"

	"github.com/apache/pulsar-client-go/oauth2"
	"github.com/apache/pulsar-client-go/oauth2/clock"
)

type MemoryStore struct {
	clock  clock.Clock
	lock   sync.Mutex
	grants map[string]*oauth2.AuthorizationGrant
}

func NewMemoryStore() Store {
	return &MemoryStore{
		clock:  clock.RealClock{},
		grants: make(map[string]*oauth2.AuthorizationGrant),
	}
}

var _ Store = &MemoryStore{}

func (f *MemoryStore) SaveGrant(audience string, grant oauth2.AuthorizationGrant) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.grants[audience] = &grant
	return nil
}

func (f *MemoryStore) LoadGrant(audience string) (*oauth2.AuthorizationGrant, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	grant, ok := f.grants[audience]
	if !ok {
		return nil, ErrNoAuthenticationData
	}
	return grant, nil
}

func (f *MemoryStore) WhoAmI(audience string) (string, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	grant, ok := f.grants[audience]
	if !ok {
		return "", ErrNoAuthenticationData
	}
	switch grant.Type {
	case oauth2.GrantTypeClientCredentials:
		if grant.ClientCredentials == nil {
			return "", ErrUnsupportedAuthData
		}
		return grant.ClientCredentials.ClientEmail, nil
	case oauth2.GrantTypeDeviceCode:
		if grant.Token == nil {
			return "", ErrUnsupportedAuthData
		}
		return oauth2.ExtractUserName(*grant.Token)
	default:
		return "", ErrUnsupportedAuthData
	}
}

func (f *MemoryStore) Logout() error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.grants = map[string]*oauth2.AuthorizationGrant{}
	return nil
}
