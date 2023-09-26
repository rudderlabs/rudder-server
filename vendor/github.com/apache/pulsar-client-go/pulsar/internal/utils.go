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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/pkg/errors"
)

// TimestampMillis return a time unix nano.
func TimestampMillis(t time.Time) uint64 {
	// calling UnixNano on the zero Time is undefined
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano()) / uint64(time.Millisecond)
}

// GetAndAdd perform atomic read and update
func GetAndAdd(n *uint64, diff uint64) uint64 {
	for {
		v := *n
		if atomic.CompareAndSwapUint64(n, v, v+diff) {
			return v
		}
	}
}

func ParseRelativeTimeInSeconds(relativeTime string) (time.Duration, error) {
	if relativeTime == "" {
		return -1, errors.New("time can not be empty")
	}

	unitTime := relativeTime[len(relativeTime)-1:]
	t := relativeTime[:len(relativeTime)-1]
	timeValue, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return -1, errors.Errorf("invalid time '%s'", t)
	}

	switch strings.ToLower(unitTime) {
	case "s":
		return time.Duration(timeValue) * time.Second, nil
	case "m":
		return time.Duration(timeValue) * time.Minute, nil
	case "h":
		return time.Duration(timeValue) * time.Hour, nil
	case "d":
		return time.Duration(timeValue) * time.Hour * 24, nil
	case "w":
		return time.Duration(timeValue) * time.Hour * 24 * 7, nil
	case "y":
		return time.Duration(timeValue) * time.Hour * 24 * 7 * 365, nil
	default:
		return -1, errors.Errorf("invalid time unit '%s'", unitTime)
	}
}

func MarshalToSizedBuffer(m proto.Message, out []byte) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	copy(out, b[:len(out)])
	return nil
}
