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

	proto "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
)

// Result used to represent pulsar processing is an alias of type int.
type Result int

const (
	// Ok means no errors
	Ok Result = iota
	// UnknownError means unknown error happened on broker
	UnknownError
	// InvalidConfiguration means invalid configuration
	InvalidConfiguration
	// TimeoutError means operation timed out
	TimeoutError
	//LookupError means broker lookup failed
	LookupError
	// ConnectError means failed to connect to broker
	ConnectError
	// ReadError means failed to read from socket
	ReadError
	// AuthenticationError means authentication failed on broker
	AuthenticationError
	// AuthorizationError client is not authorized to create producer/consumer
	AuthorizationError
	// ErrorGettingAuthenticationData client cannot find authorization data
	ErrorGettingAuthenticationData
	// BrokerMetadataError broker failed in updating metadata
	BrokerMetadataError
	// BrokerPersistenceError broker failed to persist entry
	BrokerPersistenceError
	// ChecksumError corrupt message checksum failure
	ChecksumError
	// ConsumerBusy means Exclusive consumer is already connected
	ConsumerBusy
	// NotConnectedError producer/consumer is not currently connected to broker
	NotConnectedError
	// AlreadyClosedError producer/consumer is already closed and not accepting any operation
	AlreadyClosedError
	// InvalidMessage error in publishing an already used message
	InvalidMessage
	// ConsumerNotInitialized consumer is not initialized
	ConsumerNotInitialized
	// ProducerNotInitialized producer is not initialized
	ProducerNotInitialized
	// TooManyLookupRequestException too many concurrent LookupRequest
	TooManyLookupRequestException
	// InvalidTopicName means invalid topic name
	InvalidTopicName
	// InvalidURL means Client Initialized with Invalid Broker Url (VIP Url passed to Client Constructor)
	InvalidURL
	// ServiceUnitNotReady unloaded between client did lookup and producer/consumer got created
	ServiceUnitNotReady
	// OperationNotSupported operation not supported
	OperationNotSupported
	// ProducerBlockedQuotaExceededError producer is blocked
	ProducerBlockedQuotaExceededError
	// ProducerBlockedQuotaExceededException producer is getting exception
	ProducerBlockedQuotaExceededException
	// ProducerQueueIsFull producer queue is full
	ProducerQueueIsFull
	// MessageTooBig trying to send a messages exceeding the max size
	MessageTooBig
	// TopicNotFound topic not found
	TopicNotFound
	// SubscriptionNotFound subscription not found
	SubscriptionNotFound
	// ConsumerNotFound consumer not found
	ConsumerNotFound
	// UnsupportedVersionError when an older client/version doesn't support a required feature
	UnsupportedVersionError
	// TopicTerminated topic was already terminated
	TopicTerminated
	// CryptoError error when crypto operation fails
	CryptoError
	// ConsumerClosed means consumer already been closed
	ConsumerClosed
	// InvalidBatchBuilderType invalid batch builder type
	InvalidBatchBuilderType
	// AddToBatchFailed failed to add sendRequest to batchBuilder
	AddToBatchFailed
	// SeekFailed seek failed
	SeekFailed
	// ProducerClosed means producer already been closed
	ProducerClosed
	// SchemaFailure means the payload could not be encoded using the Schema
	SchemaFailure
	// InvalidStatus means the component status is not as expected.
	InvalidStatus
	// TransactionNoFoundError The transaction is not exist in the transaction coordinator, It may be an error txn
	// or already ended.
	TransactionNoFoundError
	// ClientMemoryBufferIsFull client limit buffer is full
	ClientMemoryBufferIsFull
)

// Error implement error interface, composed of two parts: msg and result.
type Error struct {
	msg    string
	result Result
}

// Result get error's original result.
func (e *Error) Result() Result {
	return e.result
}

func (e *Error) Error() string {
	return e.msg
}

func newError(result Result, msg string) error {
	return &Error{
		msg:    fmt.Sprintf("%s: %s", msg, getResultStr(result)),
		result: result,
	}
}

func getResultStr(r Result) string {
	switch r {
	case Ok:
		return "OK"
	case UnknownError:
		return "UnknownError"
	case InvalidConfiguration:
		return "InvalidConfiguration"
	case TimeoutError:
		return "TimeoutError"
	case LookupError:
		return "LookupError"
	case ConnectError:
		return "ConnectError"
	case ReadError:
		return "ReadError"
	case AuthenticationError:
		return "AuthenticationError"
	case AuthorizationError:
		return "AuthorizationError"
	case ErrorGettingAuthenticationData:
		return "ErrorGettingAuthenticationData"
	case BrokerMetadataError:
		return "BrokerMetadataError"
	case BrokerPersistenceError:
		return "BrokerPersistenceError"
	case ChecksumError:
		return "ChecksumError"
	case ConsumerBusy:
		return "ConsumerBusy"
	case NotConnectedError:
		return "NotConnectedError"
	case AlreadyClosedError:
		return "AlreadyClosedError"
	case InvalidMessage:
		return "InvalidMessage"
	case ConsumerNotInitialized:
		return "ConsumerNotInitialized"
	case ProducerNotInitialized:
		return "ProducerNotInitialized"
	case TooManyLookupRequestException:
		return "TooManyLookupRequestException"
	case InvalidTopicName:
		return "InvalidTopicName"
	case InvalidURL:
		return "InvalidURL"
	case ServiceUnitNotReady:
		return "ServiceUnitNotReady"
	case OperationNotSupported:
		return "OperationNotSupported"
	case ProducerBlockedQuotaExceededError:
		return "ProducerBlockedQuotaExceededError"
	case ProducerBlockedQuotaExceededException:
		return "ProducerBlockedQuotaExceededException"
	case ProducerQueueIsFull:
		return "ProducerQueueIsFull"
	case MessageTooBig:
		return "MessageTooBig"
	case TopicNotFound:
		return "TopicNotFound"
	case SubscriptionNotFound:
		return "SubscriptionNotFound"
	case ConsumerNotFound:
		return "ConsumerNotFound"
	case UnsupportedVersionError:
		return "UnsupportedVersionError"
	case TopicTerminated:
		return "TopicTerminated"
	case CryptoError:
		return "CryptoError"
	case ConsumerClosed:
		return "ConsumerClosed"
	case InvalidBatchBuilderType:
		return "InvalidBatchBuilderType"
	case AddToBatchFailed:
		return "AddToBatchFailed"
	case SeekFailed:
		return "SeekFailed"
	case ProducerClosed:
		return "ProducerClosed"
	case SchemaFailure:
		return "SchemaFailure"
	case ClientMemoryBufferIsFull:
		return "ClientMemoryBufferIsFull"
	case TransactionNoFoundError:
		return "TransactionNoFoundError"
	default:
		return fmt.Sprintf("Result(%d)", r)
	}
}

func getErrorFromServerError(serverError *proto.ServerError) error {
	switch *serverError {
	case proto.ServerError_TransactionNotFound:
		return newError(TransactionNoFoundError, serverError.String())
	case proto.ServerError_InvalidTxnStatus:
		return newError(InvalidStatus, serverError.String())
	default:
		return newError(UnknownError, serverError.String())
	}
}
