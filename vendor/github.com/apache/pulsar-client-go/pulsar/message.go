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
	"time"
)

// ProducerMessage abstraction used in Pulsar producer
type ProducerMessage struct {
	// Payload for the message
	Payload []byte

	// Value and payload is mutually exclusive, `Value interface{}` for schema message.
	Value interface{}

	// Key sets the key of the message for routing policy
	Key string

	// OrderingKey sets the ordering key of the message
	OrderingKey string

	// Properties attach application defined properties on the message
	Properties map[string]string

	// EventTime set the event time for a given message
	// By default, messages don't have an event time associated, while the publish
	// time will be be always present.
	// Set the event time to a non-zero timestamp to explicitly declare the time
	// that the event "happened", as opposed to when the message is being published.
	EventTime time.Time

	// ReplicationClusters override the replication clusters for this message.
	ReplicationClusters []string

	// DisableReplication disables the replication for this message
	DisableReplication bool

	// SequenceID sets the sequence id to assign to the current message
	SequenceID *int64

	// DeliverAfter requests to deliver the message only after the specified relative delay.
	// Note: messages are only delivered with delay when a consumer is consuming
	//     through a `SubscriptionType=Shared` subscription. With other subscription
	//     types, the messages will still be delivered immediately.
	DeliverAfter time.Duration

	// DeliverAt delivers the message only at or after the specified absolute timestamp.
	// Note: messages are only delivered with delay when a consumer is consuming
	//     through a `SubscriptionType=Shared` subscription. With other subscription
	//     types, the messages will still be delivered immediately.
	DeliverAt time.Time

	//Schema assign to the current message
	//Note: messages may have a different schema from producer schema, use it instead of producer schema when assigned
	Schema Schema

	//Transaction assign to the current message
	//Note: The message is not visible before the transaction is committed.
	Transaction Transaction
}

// Message abstraction used in Pulsar
type Message interface {
	// Topic returns the topic from which this message originated from.
	Topic() string

	// ProducerName returns the name of the producer that has published the message.
	ProducerName() string

	// Properties are application defined key/value pairs that will be attached to the message.
	// Returns the properties attached to the message.
	Properties() map[string]string

	// Payload returns the payload of the message
	Payload() []byte

	// ID returns the unique message ID associated with this message.
	// The message id can be used to univocally refer to a message without having the keep the entire payload in memory.
	ID() MessageID

	// PublishTime returns the publish time of this message. The publish time is the timestamp that a client
	// publish the message.
	PublishTime() time.Time

	// EventTime returns the event time associated with this message. It is typically set by the applications via
	// `ProducerMessage.EventTime`.
	// If EventTime is 0, it means there isn't any event time associated with this message.
	EventTime() time.Time

	// Key returns the key of the message, if any
	Key() string

	// OrderingKey returns the ordering key of the message, if any
	OrderingKey() string

	// RedeliveryCount returns message redelivery count, redelivery count maintain in pulsar broker.
	// When client nack acknowledge messages,
	// broker will dispatch message again with message redelivery count in CommandMessage defined.
	//
	// Message redelivery increases monotonically in a broker, when topic switch ownership to a another broker
	// redelivery count will be recalculated.
	RedeliveryCount() uint32

	// IsReplicated determines whether the message is replicated from another cluster.
	IsReplicated() bool

	// GetReplicatedFrom returns the name of the cluster, from which the message is replicated.
	GetReplicatedFrom() string

	// GetSchemaValue returns the de-serialized value of the message, according to the configuration.
	GetSchemaValue(v interface{}) error

	//SchemaVersion get the schema version of the message, if any
	SchemaVersion() []byte

	// GetEncryptionContext returns the ecryption context of the message.
	// It will be used by the application to parse the undecrypted message.
	GetEncryptionContext() *EncryptionContext

	// Index returns index from broker entry metadata,
	// or empty if the feature is not enabled in the broker.
	Index() *uint64

	// BrokerPublishTime returns broker publish time from broker entry metadata,
	// or empty if the feature is not enabled in the broker.
	BrokerPublishTime() *time.Time
}

// MessageID identifier for a particular message
type MessageID interface {
	// Serialize the message id into a sequence of bytes that can be stored somewhere else
	Serialize() []byte

	// LedgerID returns the message ledgerID
	LedgerID() int64

	// EntryID returns the message entryID
	EntryID() int64

	// BatchIdx returns the message batchIdx
	BatchIdx() int32

	// PartitionIdx returns the message partitionIdx
	PartitionIdx() int32

	// BatchSize returns 0 or the batch size, which must be greater than BatchIdx()
	BatchSize() int32

	// String returns message id in string format
	String() string
}

// DeserializeMessageID reconstruct a MessageID object from its serialized representation
func DeserializeMessageID(data []byte) (MessageID, error) {
	return deserializeMessageID(data)
}

// NewMessageID Custom Create MessageID
func NewMessageID(ledgerID int64, entryID int64, batchIdx int32, partitionIdx int32) MessageID {
	return newMessageID(ledgerID, entryID, batchIdx, partitionIdx, 0)
}

// EarliestMessageID returns a messageID that points to the earliest message available in a topic
func EarliestMessageID() MessageID {
	return earliestMessageID
}

// LatestMessageID returns a messageID that points to the latest message
func LatestMessageID() MessageID {
	return latestMessageID
}

func messageIDCompare(lhs MessageID, rhs MessageID) int {
	if lhs.LedgerID() < rhs.LedgerID() {
		return -1
	} else if lhs.LedgerID() > rhs.LedgerID() {
		return 1
	}
	if lhs.EntryID() < rhs.EntryID() {
		return -1
	} else if lhs.EntryID() > rhs.EntryID() {
		return 1
	}
	// When performing batch index ACK on a batched message whose batch size is N,
	// the ACK order should be:
	//   (ledger, entry, 0) -> (ledger, entry, 1) -> ... -> (ledger, entry, N-1) -> (ledger, entry)
	// So we have to treat any MessageID with the batch index precedes the MessageID without the batch index
	// if they are in the same entry.
	if lhs.BatchIdx() < 0 && rhs.BatchIdx() < 0 {
		return 0
	} else if lhs.BatchIdx() >= 0 && rhs.BatchIdx() < 0 {
		return -1
	} else if lhs.BatchIdx() < 0 && rhs.BatchIdx() >= 0 {
		return 1
	}
	if lhs.BatchIdx() < rhs.BatchIdx() {
		return -1
	} else if lhs.BatchIdx() > rhs.BatchIdx() {
		return 1
	}
	return 0
}
