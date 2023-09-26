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
	"fmt"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
)

type KeySharedPolicyMode int

const (
	// KeySharedPolicyModeAutoSplit Auto split hash range key shared policy.
	KeySharedPolicyModeAutoSplit KeySharedPolicyMode = iota
	// KeySharedPolicyModeSticky is Sticky attach topic with fixed hash range.
	KeySharedPolicyModeSticky
)

// KeySharedPolicy for KeyShared subscription
type KeySharedPolicy struct {
	//KeySharedPolicyMode
	Mode KeySharedPolicyMode
	//HashRanges value pair list
	HashRanges []int
	// If enabled, it will relax the ordering requirement, allowing the broker to send out-of-order messages in case of
	// failures. This will make it faster for new consumers to join without being stalled by an existing slow consumer.
	AllowOutOfOrderDelivery bool
}

// NewKeySharedPolicySticky construct KeySharedPolicy in Sticky mode with
// hashRanges formed in value pair list: [x1, x2, y1, y2, z1, z2], and must not overlap with each others
func NewKeySharedPolicySticky(hashRanges []int) (*KeySharedPolicy, error) {
	err := validateHashRanges(hashRanges)
	if err != nil {
		return nil, err
	}
	return &KeySharedPolicy{
		Mode:       KeySharedPolicyModeSticky,
		HashRanges: hashRanges,
	}, nil
}

func toProtoKeySharedMeta(ksp *KeySharedPolicy) *pb.KeySharedMeta {
	if ksp == nil {
		return nil
	}

	mode := pb.KeySharedMode(ksp.Mode)
	meta := &pb.KeySharedMeta{
		KeySharedMode:           &mode,
		AllowOutOfOrderDelivery: &ksp.AllowOutOfOrderDelivery,
	}

	if ksp.Mode == KeySharedPolicyModeSticky {
		for i := 0; i < len(ksp.HashRanges); i += 2 {
			start, end := int32(ksp.HashRanges[i]), int32(ksp.HashRanges[i+1])
			meta.HashRanges = append(meta.HashRanges, &pb.IntRange{Start: &start, End: &end})
		}
	}

	return meta
}

func validateHashRanges(hashRanges []int) error {
	sz := len(hashRanges)
	if sz == 0 || sz%2 != 0 {
		return fmt.Errorf("ranges must not be empty or not in value pairs")
	}
	var x1, x2, y1, y2 int
	//check that the ranges are well-formed
	for i := 0; i < sz; i += 2 {
		x1, x2 = hashRanges[i], hashRanges[i+1]
		if x1 >= x2 || x1 < 0 || x2 > 65535 {
			return fmt.Errorf("ranges must be in [0, 65535], but provided range is, %d - %d", x1, x2)
		}
	}
	//loop again for checking range overlap
	for i := 0; i < sz; i += 2 {
		x1, x2 = hashRanges[i], hashRanges[i+1]
		for j := 0; j < sz; j += 2 {
			if j == i {
				continue
			}
			y1, y2 = hashRanges[j], hashRanges[j+1]
			if x1 <= y2 && y1 <= x2 {
				return fmt.Errorf("ranges with overlap between, %d - %d, and %d - %d", x1, x2, y1, y2)
			}
		}
	}
	return nil
}
