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

import "github.com/spaolacci/murmur3"

// JavaStringHash and Java String.hashCode() equivalent
func JavaStringHash(s string) uint32 {
	var h uint32
	for i, size := 0, len(s); i < size; i++ {
		h = 31*h + uint32(s[i])
	}

	return h
}

// Murmur3_32Hash use Murmur3 hashing function
func Murmur3_32Hash(s string) uint32 {
	h := murmur3.New32()
	_, err := h.Write([]byte(s))
	if err != nil {
		return 0
	}
	// Maintain compatibility with values used in Java client
	return h.Sum32() & 0x7fffffff
}
