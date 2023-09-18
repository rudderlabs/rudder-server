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

import "sync"

func NewSinglePartitionRouter() func(*ProducerMessage, TopicMetadata) int {
	var (
		singlePartition *int
		once            sync.Once
	)
	return func(message *ProducerMessage, metadata TopicMetadata) int {
		numPartitions := metadata.NumPartitions()
		if len(message.Key) != 0 {
			// When a key is specified, use the hash of that key
			return int(getHashingFunction(JavaStringHash)(message.Key) % numPartitions)
		}
		once.Do(func() {
			partition := r.R.Intn(int(numPartitions))
			singlePartition = &partition
		})

		return *singlePartition

	}

}
