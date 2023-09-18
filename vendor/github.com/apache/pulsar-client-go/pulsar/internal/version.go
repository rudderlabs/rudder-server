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
	"runtime/debug"

	"golang.org/x/mod/semver"
)

const (
	pulsarClientGoModulePath = "github.com/apache/pulsar-client-go"
)

var (
	Version             string
	ClientVersionString string
)

// init Initializes the module version information by reading
// the built in golang build info.  If the application was not built
// using go modules then the version string will not be available.
func init() {
	if buildInfo, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range buildInfo.Deps {
			if dep.Path == pulsarClientGoModulePath {
				Version = semver.Canonical(dep.Version)
				ClientVersionString = "Pulsar Go " + Version
				return
			}
		}
	}
	Version = "unknown"
	ClientVersionString = "Pulsar Go version unknown"
}
