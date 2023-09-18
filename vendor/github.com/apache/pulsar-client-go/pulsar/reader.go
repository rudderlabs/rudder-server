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
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar/internal"
)

// ReaderMessage packages Reader and Message as a struct to use.
type ReaderMessage struct {
	Reader
	Message
}

// ReaderOptions represents Reader options to use.
type ReaderOptions struct {
	// Topic specifies the topic this consumer will subscribe on.
	// This argument is required when constructing the reader.
	Topic string

	// Name set the reader name.
	Name string

	// Properties represents a set of application defined properties for the reader.
	// Those properties will be visible in the topic stats.
	Properties map[string]string

	// StartMessageID initial reader positioning is done by specifying a message id. The options are:
	//  * `pulsar.EarliestMessage` : Start reading from the earliest message available in the topic
	//  * `pulsar.LatestMessage` : Start reading from the end topic, only getting messages published after the
	//                           reader was created
	//  * `MessageID` : Start reading from a particular message id, the reader will position itself on that
	//                  specific position. The first message to be read will be the message next to the specified
	//                  messageID
	StartMessageID MessageID

	// StartMessageIDInclusive, if true, the reader will start at the `StartMessageID`, included.
	// Default is `false` and the reader will start from the "next" message
	StartMessageIDInclusive bool

	// MessageChannel sets a `MessageChannel` for the consumer
	// When a message is received, it will be pushed to the channel for consumption
	MessageChannel chan ReaderMessage

	// ReceiverQueueSize sets the size of the consumer receive queue.
	// The consumer receive queue controls how many messages can be accumulated by the Reader before the
	// application calls Reader.readNext(). Using a higher value could potentially increase the consumer
	// throughput at the expense of bigger memory utilization.
	// Default value is {@code 1000} messages and should be good for most use cases.
	ReceiverQueueSize int

	// SubscriptionRolePrefix sets the subscription role prefix. The default prefix is "reader".
	SubscriptionRolePrefix string

	// SubscriptionName sets the subscription name.
	// If subscriptionRolePrefix is set at the same time, this configuration will prevail
	SubscriptionName string

	// ReadCompacted, if enabled, the reader will read messages from the compacted topic rather than reading the
	// full message backlog of the topic. This means that, if the topic has been compacted, the reader will only
	// see the latest value for each key in the topic, up until the point in the topic message backlog that has
	// been compacted. Beyond that point, the messages will be sent as normal.
	//
	// ReadCompacted can only be enabled when reading from a persistent topic. Attempting to enable it on non-persistent
	// topics will lead to the reader create call throwing a PulsarClientException.
	ReadCompacted bool

	// Decryption represents the encryption related fields required by the reader to decrypt a message.
	Decryption *MessageDecryptionInfo

	// Schema represents the schema implementation.
	Schema Schema

	// BackoffPolicy parameterize the following options in the reconnection logic to
	// allow users to customize the reconnection logic (minBackoff, maxBackoff and jitterPercentage)
	BackoffPolicy internal.BackoffPolicy

	// MaxPendingChunkedMessage sets the maximum pending chunked messages. (default: 100)
	MaxPendingChunkedMessage int

	// ExpireTimeOfIncompleteChunk sets the expiry time of discarding incomplete chunked message. (default: 60 seconds)
	ExpireTimeOfIncompleteChunk time.Duration

	// AutoAckIncompleteChunk sets whether reader auto acknowledges incomplete chunked message when it should
	// be removed (e.g.the chunked message pending queue is full). (default: false)
	AutoAckIncompleteChunk bool
}

// Reader can be used to scan through all the messages currently available in a topic.
type Reader interface {
	// Topic from which this reader is reading from
	Topic() string

	// Next reads the next message in the topic, blocking until a message is available
	Next(context.Context) (Message, error)

	// HasNext checks if there is any message available to read from the current position
	HasNext() bool

	// Close the reader and stop the broker to push more messages
	Close()

	// Seek resets the subscription associated with this reader to a specific message id.
	// The message id can either be a specific message or represent the first or last messages in the topic.
	//
	// Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the
	//       seek() on the individual partitions.
	Seek(MessageID) error

	// SeekByTime resets the subscription associated with this reader to a specific message publish time.
	//
	// Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the seek() on
	// the individual partitions.
	//
	// @param timestamp
	//            the message publish time where to reposition the subscription
	//
	SeekByTime(time time.Time) error
}
