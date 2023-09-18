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
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// BackoffPolicy parameterize the following options in the reconnection logic to
// allow users to customize the reconnection logic (minBackoff, maxBackoff and jitterPercentage)
type BackoffPolicy interface {
	Next() time.Duration
}

// DefaultBackoff computes the delay before retrying an action.
// It uses an exponential backoff with jitter. The jitter represents up to 20 percents of the delay.
type DefaultBackoff struct {
	backoff time.Duration
}

const maxBackoff = 60 * time.Second

// Next returns the delay to wait before next retry
func (b *DefaultBackoff) Next() time.Duration {
	minBackoff := 100 * time.Millisecond
	jitterPercentage := 0.2

	// Double the delay each time
	b.backoff += b.backoff
	if b.backoff.Nanoseconds() < minBackoff.Nanoseconds() {
		b.backoff = minBackoff
	} else if b.backoff.Nanoseconds() > maxBackoff.Nanoseconds() {
		b.backoff = maxBackoff
	}
	jitter := rand.Float64() * float64(b.backoff) * jitterPercentage

	return b.backoff + time.Duration(jitter)
}

// IsMaxBackoffReached evaluates if the max number of retries is reached
func (b *DefaultBackoff) IsMaxBackoffReached() bool {
	return b.backoff >= maxBackoff
}
