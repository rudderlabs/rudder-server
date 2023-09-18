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

type HashingScheme int

const (
	// JavaStringHash and Java String.hashCode() equivalent
	JavaStringHash HashingScheme = iota
	// Murmur3_32Hash use Murmur3 hashing function
	Murmur3_32Hash
)

type CompressionType int

const (
	NoCompression CompressionType = iota
	LZ4
	ZLib
	ZSTD
)

type CompressionLevel int

const (
	// Default compression level
	Default CompressionLevel = iota

	// Faster compression, with lower compression ratio
	Faster

	// Higher compression rate, but slower
	Better
)

type ProducerAccessMode int

const (
	// ProducerAccessModeShared is default multiple producers can publish on a topic.
	ProducerAccessModeShared ProducerAccessMode = iota

	// ProducerAccessModeExclusive is required exclusive access for producer.
	// Fail immediately if there's already a producer connected.
	ProducerAccessModeExclusive

	// ProducerAccessModeWaitForExclusive is pending until producer can acquire exclusive access.
	ProducerAccessModeWaitForExclusive
)

// TopicMetadata represents a topic metadata.
type TopicMetadata interface {
	// NumPartitions returns the number of partitions for a particular topic.
	NumPartitions() uint32
}

type ProducerOptions struct {
	// Topic specifies the topic this producer will be publishing on.
	// This argument is required when constructing the producer.
	Topic string

	// Name specifies a name for the producer.
	// If not assigned, the system will generate a globally unique name which can be access with
	// Producer.ProducerName().
	// When specifying a name, it is up to the user to ensure that, for a given topic, the producer name is unique
	// across all Pulsar's clusters. Brokers will enforce that only a single producer a given name can be publishing on
	// a topic.
	Name string

	// Properties specifies a set of application defined properties for the producer.
	// This properties will be visible in the topic stats
	Properties map[string]string

	// SendTimeout specifies the timeout for a message that has not been acknowledged by the server since sent.
	// Send and SendAsync returns an error after timeout.
	// Default is 30 seconds, negative such as -1 to disable.
	SendTimeout time.Duration

	// DisableBlockIfQueueFull controls whether Send and SendAsync block if producer's message queue is full.
	// Default is false, if set to true then Send and SendAsync return error when queue is full.
	DisableBlockIfQueueFull bool

	// MaxPendingMessages specifies the max size of the queue holding the messages pending to receive an
	// acknowledgment from the broker.
	MaxPendingMessages int

	// HashingScheme is used to define the partition on where to publish a particular message.
	// Standard hashing functions available are:
	//
	//  - `JavaStringHash` : Java String.hashCode() equivalent
	//  - `Murmur3_32Hash` : Use Murmur3 hashing function.
	// 		https://en.wikipedia.org/wiki/MurmurHash">https://en.wikipedia.org/wiki/MurmurHash
	//
	// Default is `JavaStringHash`.
	HashingScheme

	// CompressionType specifies the compression type for the producer.
	// By default, message payloads are not compressed. Supported compression types are:
	//  - LZ4
	//  - ZLIB
	//  - ZSTD
	//
	// Note: ZSTD is supported since Pulsar 2.3. Consumers will need to be at least at that
	// release in order to be able to receive messages compressed with ZSTD.
	CompressionType

	// CompressionLevel defines the desired compression level. Options:
	// - Default
	// - Faster
	// - Better
	CompressionLevel

	// MessageRouter represents a custom message routing policy by passing an implementation of MessageRouter
	// The router is a function that given a particular message and the topic metadata, returns the
	// partition index where the message should be routed to
	MessageRouter func(*ProducerMessage, TopicMetadata) int

	// DisableBatching controls whether automatic batching of messages is enabled for the producer. By default batching
	// is enabled.
	// When batching is enabled, multiple calls to Producer.sendAsync can result in a single batch to be sent to the
	// broker, leading to better throughput, especially when publishing small messages. If compression is enabled,
	// messages will be compressed at the batch level, leading to a much better compression ratio for similar headers or
	// contents.
	// When enabled default batch delay is set to 1 ms and default batch size is 1000 messages
	// Setting `DisableBatching: true` will make the producer to send messages individually
	DisableBatching bool

	// BatchingMaxPublishDelay specifies the time period within which the messages sent will be batched (default: 10ms)
	// if batch messages are enabled. If set to a non zero value, messages will be queued until this time
	// interval or until
	BatchingMaxPublishDelay time.Duration

	// BatchingMaxMessages specifies the maximum number of messages permitted in a batch. (default: 1000)
	// If set to a value greater than 1, messages will be queued until this threshold is reached or
	// BatchingMaxSize (see below) has been reached or the batch interval has elapsed.
	BatchingMaxMessages uint

	// BatchingMaxSize specifies the maximum number of bytes permitted in a batch. (default 128 KB)
	// If set to a value greater than 1, messages will be queued until this threshold is reached or
	// BatchingMaxMessages (see above) has been reached or the batch interval has elapsed.
	BatchingMaxSize uint

	// Interceptors is a chain of interceptors, These interceptors will be called at some points defined
	// in ProducerInterceptor interface
	Interceptors ProducerInterceptors

	// Schema represents the schema implementation.
	Schema Schema

	// MaxReconnectToBroker specifies the maximum retry number of reconnectToBroker. (default: ultimate)
	MaxReconnectToBroker *uint

	// BackoffPolicy parameterize the following options in the reconnection logic to
	// allow users to customize the reconnection logic (minBackoff, maxBackoff and jitterPercentage)
	BackoffPolicy internal.BackoffPolicy

	// BatcherBuilderType sets the batch builder type (default DefaultBatchBuilder)
	// This will be used to create batch container when batching is enabled.
	// Options:
	// - DefaultBatchBuilder
	// - KeyBasedBatchBuilder
	BatcherBuilderType

	// PartitionsAutoDiscoveryInterval is the time interval for the background process to discover new partitions
	// Default is 1 minute
	PartitionsAutoDiscoveryInterval time.Duration

	// Disable multiple Schame Version
	// Default false
	DisableMultiSchema bool

	// Encryption specifies the fields required to encrypt a message
	Encryption *ProducerEncryptionInfo

	// EnableChunking controls whether automatic chunking of messages is enabled for the producer. By default, chunking
	// is disabled.
	// Chunking can not be enabled when batching is enabled.
	EnableChunking bool

	// ChunkMaxMessageSize is the max size of single chunk payload.
	// It will actually only take effect if it is smaller than the maxMessageSize from the broker.
	ChunkMaxMessageSize uint

	// The type of access to the topic that the producer requires. (default ProducerAccessModeShared)
	// Options:
	// - ProducerAccessModeShared
	// - ProducerAccessModeExclusive
	ProducerAccessMode
}

// Producer is used to publish messages on a topic
type Producer interface {
	// Topic return the topic to which producer is publishing to
	Topic() string

	// Name return the producer name which could have been assigned by the system or specified by the client
	Name() string

	// Send a message
	// This call will be blocking until is successfully acknowledged by the Pulsar broker.
	// Example:
	// producer.Send(ctx, pulsar.ProducerMessage{ Payload: myPayload })
	Send(context.Context, *ProducerMessage) (MessageID, error)

	// SendAsync a message in asynchronous mode
	// This call is blocked when the `event channel` becomes full (default: 10) or the
	// `maxPendingMessages` becomes full (default: 1000)
	// The callback will report back the message being published and
	// the eventual error in publishing
	SendAsync(context.Context, *ProducerMessage, func(MessageID, *ProducerMessage, error))

	// LastSequenceID get the last sequence id that was published by this producer.
	// This represent either the automatically assigned or custom sequence id (set on the ProducerMessage) that
	// was published and acknowledged by the broker.
	// After recreating a producer with the same producer name, this will return the last message that was
	// published in the previous producer session, or -1 if there no message was ever published.
	// return the last sequence id published by this producer.
	LastSequenceID() int64

	// Flush all the messages buffered in the client and wait until all messages have been successfully
	// persisted.
	Flush() error

	// Close the producer and releases resources allocated
	// No more writes will be accepted from this producer. Waits until all pending write request are persisted. In case
	// of errors, pending writes will not be retried.
	Close()
}
