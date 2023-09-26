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
	"compress/zlib"
	"io"
)

type zlibProvider struct{}

// NewZLibProvider returns a Provider interface
func NewZLibProvider() Provider {
	return &zlibProvider{}
}

func (zlibProvider) CompressMaxSize(originalSize int) int {
	// Use formula from ZLib: https://github.com/madler/zlib/blob/cacf7f1d4e3d44d871b605da3b647f07d718623f/deflate.c#L659
	return originalSize +
		((originalSize + 7) >> 3) + ((originalSize + 63) >> 6) + 11
}

func (zlibProvider) Compress(dst, src []byte) []byte {
	var b = bytes.NewBuffer(dst[:0])
	w := zlib.NewWriter(b)

	if _, err := w.Write(src); err != nil {
		return nil
	}
	if err := w.Close(); err != nil {
		return nil
	}

	return b.Bytes()
}

func (zlibProvider) Decompress(dst, src []byte, originalSize int) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, err
	}

	if cap(dst) >= originalSize {
		dst = dst[0:originalSize] // Reuse dst buffer
	} else {
		dst = make([]byte, originalSize)
	}
	if _, err = io.ReadFull(r, dst); err != nil {
		return nil, err
	}

	if err = r.Close(); err != nil {
		return nil, err
	}

	return dst, nil
}

func (zlibProvider) Clone() Provider {
	return NewZLibProvider()
}

func (zlibProvider) Close() error {
	return nil
}
