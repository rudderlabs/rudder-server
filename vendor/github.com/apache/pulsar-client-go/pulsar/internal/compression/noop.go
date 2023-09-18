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

package compression

import (
	"bytes"
)

type noopProvider struct{}

// NewNoopProvider returns a Provider interface that does not compress the data
func NewNoopProvider() Provider {
	return &noopProvider{}
}

func (noopProvider) CompressMaxSize(originalSize int) int {
	return originalSize
}

func (noopProvider) Compress(dst, src []byte) []byte {
	if dst == nil {
		dst = make([]byte, len(src))
	}

	b := bytes.NewBuffer(dst[:0])
	b.Write(src)
	return dst[:len(src)]
}

func (noopProvider) Decompress(dst, src []byte, originalSize int) ([]byte, error) {
	if dst == nil {
		dst = make([]byte, len(src))
	}

	b := bytes.NewBuffer(dst[:0])
	b.Write(src)
	return dst[:len(src)], nil
}

func (noopProvider) Close() error {
	return nil
}

func (noopProvider) Clone() Provider {
	return NewNoopProvider()
}
