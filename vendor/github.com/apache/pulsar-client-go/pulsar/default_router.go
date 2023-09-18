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
	"math/rand"
	"sync/atomic"
	"time"
)

type defaultRouter struct {
	currentPartitionCursor uint32

	lastBatchTimestamp  int64
	msgCounter          uint32
	cumulativeBatchSize uint32
}

// NewDefaultRouter set the message routing mode for the partitioned producer.
// Default routing mode is round-robin routing if no partition key is specified.
// If the batching is enabled, it honors the different thresholds for batching i.e. maximum batch size,
// maximum number of messages, maximum delay to publish a batch. When one of the threshold is reached the next partition
// is used.
func NewDefaultRouter(
	hashFunc func(string) uint32,
	maxBatchingMessages uint,
	maxBatchingSize uint,
	maxBatchingDelay time.Duration,
	disableBatching bool) func(*ProducerMessage, uint32) int {
	state := &defaultRouter{
		currentPartitionCursor: rand.Uint32(),
		lastBatchTimestamp:     time.Now().UnixNano(),
	}

	readClockAfterNumMessages := uint32(maxBatchingMessages / 10)
	return func(message *ProducerMessage, numPartitions uint32) int {
		if numPartitions == 1 {
			// When there are no partitions, don't even bother
			return 0
		}

		if len(message.OrderingKey) != 0 {
			// When an OrderingKey is specified, use the hash of that key
			return int(hashFunc(message.OrderingKey) % numPartitions)
		}

		if len(message.Key) != 0 {
			// When a key is specified, use the hash of that key
			return int(hashFunc(message.Key) % numPartitions)
		}

		// If there's no key, we do round-robin across partition. If no batching go to next partition.
		if disableBatching {
			p := int(state.currentPartitionCursor % numPartitions)
			atomic.AddUint32(&state.currentPartitionCursor, 1)
			return p
		}

		// If there's no key, we do round-robin across partition, sticking with a given
		// partition for a certain amount of messages or volume buffered or the max delay to batch is reached so that
		// we ensure having a decent amount of batching of the messages.
		var now int64
		size := uint32(len(message.Payload))
		partitionCursor := atomic.LoadUint32(&state.currentPartitionCursor)
		messageCount := atomic.AddUint32(&state.msgCounter, 1)
		batchSize := atomic.AddUint32(&state.cumulativeBatchSize, size)

		// Note: use greater-than for the threshold check so that we don't route this message to a new partition
		//       before a batch is complete.
		messageCountReached := messageCount > uint32(maxBatchingMessages)
		sizeReached := batchSize > uint32(maxBatchingSize)
		durationReached := false
		if readClockAfterNumMessages == 0 || messageCount%readClockAfterNumMessages == 0 {
			now = time.Now().UnixNano()
			lastBatchTime := atomic.LoadInt64(&state.lastBatchTimestamp)
			durationReached = now-lastBatchTime > maxBatchingDelay.Nanoseconds()
		}
		if messageCountReached || sizeReached || durationReached {
			// Note: CAS to ensure that concurrent go-routines can only move the cursor forward by one so that
			//       partitions are not skipped.
			newCursor := partitionCursor + 1
			if atomic.CompareAndSwapUint32(&state.currentPartitionCursor, partitionCursor, newCursor) {
				atomic.StoreUint32(&state.msgCounter, 0)
				atomic.StoreUint32(&state.cumulativeBatchSize, 0)
				if now == 0 {
					now = time.Now().UnixNano()
				}
				atomic.StoreInt64(&state.lastBatchTimestamp, now)
			}

			return int(newCursor % numPartitions)
		}

		return int(partitionCursor % numPartitions)
	}
}
