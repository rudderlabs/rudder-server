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

package pulsar

import (
	"math"
	"time"
)

// NackBackoffPolicy is a interface for custom message negativeAcked policy, users can specify a NackBackoffPolicy
// for a consumer.
//
// > Notice: the consumer crashes will trigger the redelivery of the unacked message, this case will not respect the
// > NackBackoffPolicy, which means the message might get redelivered earlier than the delay time
// > from the backoff.
type NackBackoffPolicy interface {
	// Next param redeliveryCount indicates the number of times the message was redelivered.
	// We can get the redeliveryCount from the CommandMessage.
	Next(redeliveryCount uint32) time.Duration
}

// defaultNackBackoffPolicy is default impl for NackBackoffPolicy.
type defaultNackBackoffPolicy struct{}

func (nbp *defaultNackBackoffPolicy) Next(redeliveryCount uint32) time.Duration {
	minNackTime := 1 * time.Second  // 1sec
	maxNackTime := 10 * time.Minute // 10min

	if redeliveryCount < 0 {
		return minNackTime
	}

	return time.Duration(math.Min(math.Abs(float64(minNackTime<<redeliveryCount)), float64(maxNackTime)))
}
