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
	"github.com/pierrec/lz4"
)

const (
	minLz4DestinationBufferSize = 1024 * 1024
)

type lz4Provider struct {
	hashTable []int
}

// NewLz4Provider return a interface of Provider.
func NewLz4Provider() Provider {
	const tableSize = 1 << 16

	return &lz4Provider{
		hashTable: make([]int, tableSize),
	}
}

func (l *lz4Provider) CompressMaxSize(originalSize int) int {
	s := lz4.CompressBlockBound(originalSize)
	if s < minLz4DestinationBufferSize {
		return minLz4DestinationBufferSize
	}

	return s
}

func (l *lz4Provider) Compress(dst, data []byte) []byte {
	maxSize := lz4.CompressBlockBound(len(data))
	if cap(dst) >= maxSize {
		dst = dst[0:maxSize] // Reuse dst buffer
	} else {
		dst = make([]byte, maxSize)
	}
	size, err := lz4.CompressBlock(data, dst, l.hashTable)
	if err != nil {
		panic("Failed to compress")
	}

	if size == 0 {
		// The data block was not compressed. Just repeat it with
		// the block header flag to signal it's not compressed
		headerSize := writeSize(len(data), dst)
		copy(dst[headerSize:], data)
		return dst[:len(data)+headerSize]
	}
	return dst[:size]
}

// Write the encoded size for the uncompressed payload
func writeSize(size int, dst []byte) int {
	if size < 0xF {
		dst[0] |= byte(size << 4)
		return 1
	}
	dst[0] |= 0xF0
	l := size - 0xF
	i := 1
	for ; l >= 0xFF; l -= 0xFF {
		dst[i] = 0xFF
		i++
	}
	dst[i] = byte(l)
	return i + 1
}

func (lz4Provider) Decompress(dst, src []byte, originalSize int) ([]byte, error) {
	if cap(dst) >= originalSize {
		dst = dst[0:originalSize] // Reuse dst buffer
	} else {
		dst = make([]byte, originalSize)
	}
	_, err := lz4.UncompressBlock(src, dst)
	return dst, err
}

func (lz4Provider) Close() error {
	return nil
}

func (lz4Provider) Clone() Provider {
	return NewLz4Provider()
}
