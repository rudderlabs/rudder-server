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

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar/internal/compression"
	"github.com/apache/pulsar-client-go/pulsar/internal/crypto"
	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"google.golang.org/protobuf/proto"
)

const (
	// MaxMessageSize limit message size for transfer
	MaxMessageSize = 5 * 1024 * 1024
	// MessageFramePadding is for metadata and other frame headers
	MessageFramePadding = 10 * 1024
	// MaxFrameSize limit the maximum size that pulsar allows for messages to be sent.
	MaxFrameSize                    = MaxMessageSize + MessageFramePadding
	magicCrc32c              uint16 = 0x0e01
	magicBrokerEntryMetadata uint16 = 0x0e02
)

// ErrCorruptedMessage is the error returned by ReadMessageData when it has detected corrupted data.
// The data is considered corrupted if it's missing a header, a checksum mismatch or there
// was an error when unmarshalling the message metadata.
var ErrCorruptedMessage = errors.New("corrupted message")

// ErrEOM is the error returned by ReadMessage when no more input is available.
var ErrEOM = errors.New("EOF")

var ErrConnectionClosed = errors.New("connection closed")

var ErrExceedMaxMessageSize = errors.New("encryptedPayload exceeds MaxMessageSize")

func NewMessageReader(headersAndPayload Buffer) *MessageReader {
	return &MessageReader{
		buffer: headersAndPayload,
	}
}

func NewMessageReaderFromArray(headersAndPayload []byte) *MessageReader {
	return NewMessageReader(NewBufferWrapper(headersAndPayload))
}

// MessageReader provides helper methods to parse
// the metadata and messages from the binary format
// Wire format for a messages
//
// Old format (single message)
// [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]
//
// Batch format
// [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [METADATA_SIZE][METADATA][PAYLOAD]
// [METADATA_SIZE][METADATA][PAYLOAD]
type MessageReader struct {
	buffer Buffer
	// true if we are parsing a batched message - set after parsing the message metadata
	batched bool
}

// ReadChecksum
func (r *MessageReader) readChecksum() (uint32, error) {
	if r.buffer.ReadableBytes() < 6 {
		return 0, errors.New("missing message header")
	}
	// reader magic number
	magicNumber := r.buffer.ReadUint16()
	if magicNumber != magicCrc32c {
		return 0, ErrCorruptedMessage
	}
	checksum := r.buffer.ReadUint32()
	return checksum, nil
}

func (r *MessageReader) ReadMessageMetadata() (*pb.MessageMetadata, error) {
	// Wire format
	// [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA]

	// read checksum
	checksum, err := r.readChecksum()
	if err != nil {
		return nil, err
	}

	// validate checksum
	computedChecksum := Crc32cCheckSum(r.buffer.ReadableSlice())
	if checksum != computedChecksum {
		return nil, fmt.Errorf("checksum mismatch received: 0x%x computed: 0x%x", checksum, computedChecksum)
	}

	size := r.buffer.ReadUint32()
	data := r.buffer.Read(size)
	var meta pb.MessageMetadata
	if err := proto.Unmarshal(data, &meta); err != nil {
		return nil, ErrCorruptedMessage
	}

	if meta.NumMessagesInBatch != nil {
		r.batched = true
	}

	return &meta, nil
}

func (r *MessageReader) ReadBrokerMetadata() (*pb.BrokerEntryMetadata, error) {
	magicNumber := binary.BigEndian.Uint16(r.buffer.Get(r.buffer.ReaderIndex(), 2))
	if magicNumber != magicBrokerEntryMetadata {
		return nil, nil
	}
	r.buffer.Skip(2)
	size := r.buffer.ReadUint32()
	var brokerEntryMetadata pb.BrokerEntryMetadata
	if err := proto.Unmarshal(r.buffer.Read(size), &brokerEntryMetadata); err != nil {
		return nil, err
	}
	return &brokerEntryMetadata, nil
}

func (r *MessageReader) ReadMessage() (*pb.SingleMessageMetadata, []byte, error) {
	if r.buffer.ReadableBytes() == 0 && r.buffer.Capacity() > 0 {
		return nil, nil, ErrEOM
	}
	if !r.batched {
		return r.readMessage()
	}

	return r.readSingleMessage()
}

func (r *MessageReader) readMessage() (*pb.SingleMessageMetadata, []byte, error) {
	// Wire format
	// [PAYLOAD]

	return nil, r.buffer.Read(r.buffer.ReadableBytes()), nil
}

func (r *MessageReader) readSingleMessage() (*pb.SingleMessageMetadata, []byte, error) {
	// Wire format
	// [METADATA_SIZE][METADATA][PAYLOAD]

	size := r.buffer.ReadUint32()
	var meta pb.SingleMessageMetadata
	if err := proto.Unmarshal(r.buffer.Read(size), &meta); err != nil {
		return nil, nil, err
	}

	return &meta, r.buffer.Read(uint32(meta.GetPayloadSize())), nil
}

func (r *MessageReader) ResetBuffer(buffer Buffer) {
	r.buffer = buffer
}

func baseCommand(cmdType pb.BaseCommand_Type, msg proto.Message) *pb.BaseCommand {
	cmd := &pb.BaseCommand{
		Type: &cmdType,
	}
	switch cmdType {
	case pb.BaseCommand_CONNECT:
		cmd.Connect = msg.(*pb.CommandConnect)
	case pb.BaseCommand_LOOKUP:
		cmd.LookupTopic = msg.(*pb.CommandLookupTopic)
	case pb.BaseCommand_PARTITIONED_METADATA:
		cmd.PartitionMetadata = msg.(*pb.CommandPartitionedTopicMetadata)
	case pb.BaseCommand_PRODUCER:
		cmd.Producer = msg.(*pb.CommandProducer)
	case pb.BaseCommand_SUBSCRIBE:
		cmd.Subscribe = msg.(*pb.CommandSubscribe)
	case pb.BaseCommand_FLOW:
		cmd.Flow = msg.(*pb.CommandFlow)
	case pb.BaseCommand_PING:
		cmd.Ping = msg.(*pb.CommandPing)
	case pb.BaseCommand_PONG:
		cmd.Pong = msg.(*pb.CommandPong)
	case pb.BaseCommand_SEND:
		cmd.Send = msg.(*pb.CommandSend)
	case pb.BaseCommand_SEND_ERROR:
		cmd.SendError = msg.(*pb.CommandSendError)
	case pb.BaseCommand_CLOSE_PRODUCER:
		cmd.CloseProducer = msg.(*pb.CommandCloseProducer)
	case pb.BaseCommand_CLOSE_CONSUMER:
		cmd.CloseConsumer = msg.(*pb.CommandCloseConsumer)
	case pb.BaseCommand_ACK:
		cmd.Ack = msg.(*pb.CommandAck)
	case pb.BaseCommand_SEEK:
		cmd.Seek = msg.(*pb.CommandSeek)
	case pb.BaseCommand_UNSUBSCRIBE:
		cmd.Unsubscribe = msg.(*pb.CommandUnsubscribe)
	case pb.BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES:
		cmd.RedeliverUnacknowledgedMessages = msg.(*pb.CommandRedeliverUnacknowledgedMessages)
	case pb.BaseCommand_GET_TOPICS_OF_NAMESPACE:
		cmd.GetTopicsOfNamespace = msg.(*pb.CommandGetTopicsOfNamespace)
	case pb.BaseCommand_GET_LAST_MESSAGE_ID:
		cmd.GetLastMessageId = msg.(*pb.CommandGetLastMessageId)
	case pb.BaseCommand_AUTH_RESPONSE:
		cmd.AuthResponse = msg.(*pb.CommandAuthResponse)
	case pb.BaseCommand_GET_OR_CREATE_SCHEMA:
		cmd.GetOrCreateSchema = msg.(*pb.CommandGetOrCreateSchema)
	case pb.BaseCommand_GET_SCHEMA:
		cmd.GetSchema = msg.(*pb.CommandGetSchema)
	case pb.BaseCommand_TC_CLIENT_CONNECT_REQUEST:
		cmd.TcClientConnectRequest = msg.(*pb.CommandTcClientConnectRequest)
	case pb.BaseCommand_NEW_TXN:
		cmd.NewTxn = msg.(*pb.CommandNewTxn)
	case pb.BaseCommand_ADD_PARTITION_TO_TXN:
		cmd.AddPartitionToTxn = msg.(*pb.CommandAddPartitionToTxn)
	case pb.BaseCommand_ADD_SUBSCRIPTION_TO_TXN:
		cmd.AddSubscriptionToTxn = msg.(*pb.CommandAddSubscriptionToTxn)
	case pb.BaseCommand_END_TXN:
		cmd.EndTxn = msg.(*pb.CommandEndTxn)

	default:
		panic(fmt.Sprintf("Missing command type: %v", cmdType))
	}

	return cmd
}

func addSingleMessageToBatch(wb Buffer, smm *pb.SingleMessageMetadata, payload []byte) {
	metadataSize := uint32(proto.Size(smm))
	wb.WriteUint32(metadataSize)

	wb.ResizeIfNeeded(metadataSize)
	err := MarshalToSizedBuffer(smm, wb.WritableSlice()[:metadataSize])
	if err != nil {
		panic(fmt.Sprintf("Protobuf serialization error: %v", err))
	}

	wb.WrittenBytes(metadataSize)
	wb.Write(payload)
}

func serializeMessage(wb Buffer,
	cmdSend *pb.BaseCommand,
	msgMetadata *pb.MessageMetadata,
	payload Buffer,
	compressionProvider compression.Provider,
	encryptor crypto.Encryptor,
	maxMessageSize uint32,
	doCompress bool) error {
	// Wire format
	// [TOTAL_SIZE] [CMD_SIZE][CMD] [MAGIC_NUMBER][CHECKSUM] [METADATA_SIZE][METADATA] [PAYLOAD]

	// compress the payload
	var compressedPayload []byte
	if doCompress {
		compressedPayload = compressionProvider.Compress(nil, payload.ReadableSlice())
	} else {
		compressedPayload = payload.ReadableSlice()
	}

	// encrypt the compressed payload
	encryptedPayload, err := encryptor.Encrypt(compressedPayload, msgMetadata)
	if err != nil {
		// error occurred while encrypting the payload, ProducerCryptoFailureAction is set to Fail
		return fmt.Errorf("encryption of message failed, ProducerCryptoFailureAction is set to Fail. Error :%v", err)
	}

	cmdSize := uint32(proto.Size(cmdSend))
	msgMetadataSize := uint32(proto.Size(msgMetadata))
	msgSize := len(encryptedPayload) + int(msgMetadataSize)

	// the maxMessageSize check of batching message is in here
	if !(msgMetadata.GetTotalChunkMsgSize() != 0) && msgSize > int(maxMessageSize) {
		return fmt.Errorf("%w, size: %d, MaxMessageSize: %d",
			ErrExceedMaxMessageSize, msgSize, maxMessageSize)
	}

	frameSizeIdx := wb.WriterIndex()
	wb.WriteUint32(0) // Skip frame size until we now the size
	frameStartIdx := wb.WriterIndex()

	// Write cmd
	wb.WriteUint32(cmdSize)
	wb.ResizeIfNeeded(cmdSize)
	err = MarshalToSizedBuffer(cmdSend, wb.WritableSlice()[:cmdSize])
	if err != nil {
		panic(fmt.Sprintf("Protobuf error when serializing cmdSend: %v", err))
	}
	wb.WrittenBytes(cmdSize)

	// Create checksum placeholder
	wb.WriteUint16(magicCrc32c)
	checksumIdx := wb.WriterIndex()
	wb.WriteUint32(0) // skip 4 bytes of checksum

	// Write metadata
	metadataStartIdx := wb.WriterIndex()
	wb.WriteUint32(msgMetadataSize)
	wb.ResizeIfNeeded(msgMetadataSize)
	err = MarshalToSizedBuffer(msgMetadata, wb.WritableSlice()[:msgMetadataSize])
	if err != nil {
		panic(fmt.Sprintf("Protobuf error when serializing msgMetadata: %v", err))
	}
	wb.WrittenBytes(msgMetadataSize)

	// add payload to the buffer
	wb.Write(encryptedPayload)

	// Write checksum at created checksum-placeholder
	frameEndIdx := wb.WriterIndex()
	checksum := Crc32cCheckSum(wb.Get(metadataStartIdx, frameEndIdx-metadataStartIdx))

	// Set Sizes and checksum in the fixed-size header
	wb.PutUint32(frameEndIdx-frameStartIdx, frameSizeIdx) // External frame
	wb.PutUint32(checksum, checksumIdx)
	return nil
}

func SingleSend(wb Buffer,
	producerID, sequenceID uint64,
	msgMetadata *pb.MessageMetadata,
	compressedPayload Buffer,
	encryptor crypto.Encryptor,
	maxMassageSize uint32,
	useTxn bool,
	mostSigBits uint64,
	leastSigBits uint64) error {
	cmdSend := baseCommand(
		pb.BaseCommand_SEND,
		&pb.CommandSend{
			ProducerId: &producerID,
		},
	)
	cmdSend.Send.SequenceId = &sequenceID
	if msgMetadata.GetTotalChunkMsgSize() > 1 {
		isChunk := true
		cmdSend.Send.IsChunk = &isChunk
	}
	if useTxn {
		cmdSend.Send.TxnidMostBits = proto.Uint64(mostSigBits)
		cmdSend.Send.TxnidLeastBits = proto.Uint64(leastSigBits)
	}
	// payload has been compressed so compressionProvider can be nil
	return serializeMessage(wb, cmdSend, msgMetadata, compressedPayload,
		nil, encryptor, maxMassageSize, false)
}

// ConvertFromStringMap convert a string map to a KeyValue []byte
func ConvertFromStringMap(m map[string]string) []*pb.KeyValue {
	list := make([]*pb.KeyValue, len(m))

	i := 0
	for k, v := range m {
		list[i] = &pb.KeyValue{
			Key:   proto.String(k),
			Value: proto.String(v),
		}

		i++
	}

	return list
}

// ConvertToStringMap convert a KeyValue []byte to string map
func ConvertToStringMap(pbb []*pb.KeyValue) map[string]string {
	m := make(map[string]string)

	for _, kv := range pbb {
		m[*kv.Key] = *kv.Value
	}

	return m
}
