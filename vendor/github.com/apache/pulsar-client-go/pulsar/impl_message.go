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
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"github.com/bits-and-blooms/bitset"
)

type messageID struct {
	ledgerID     int64
	entryID      int64
	batchIdx     int32
	partitionIdx int32
	batchSize    int32
}

var latestMessageID = &messageID{
	ledgerID:     math.MaxInt64,
	entryID:      math.MaxInt64,
	batchIdx:     -1,
	partitionIdx: -1,
	batchSize:    0,
}

var earliestMessageID = &messageID{
	ledgerID:     -1,
	entryID:      -1,
	batchIdx:     -1,
	partitionIdx: -1,
	batchSize:    0,
}

type trackingMessageID struct {
	*messageID

	tracker      *ackTracker
	consumer     acker
	receivedTime time.Time
}

func (id *trackingMessageID) Ack() error {
	if id.consumer == nil {
		return errors.New("consumer is nil in trackingMessageID")
	}
	if id.ack() {
		return id.consumer.AckID(id)
	}

	return nil
}

func (id *trackingMessageID) AckWithResponse() error {
	if id.consumer == nil {
		return errors.New("consumer is nil in trackingMessageID")
	}
	if id.ack() {
		return id.consumer.AckIDWithResponse(id)
	}

	return nil
}

func (id *trackingMessageID) Nack() {
	if id.consumer == nil {
		return
	}
	id.consumer.NackID(id)
}

func (id *trackingMessageID) NackByMsg(msg Message) {
	if id.consumer == nil {
		return
	}
	id.consumer.NackMsg(msg)
}

func (id *trackingMessageID) ack() bool {
	if id.tracker != nil && id.batchIdx > -1 {
		return id.tracker.ack(int(id.batchIdx))
	}
	return true
}

func (id *trackingMessageID) ackCumulative() bool {
	if id.tracker != nil && id.batchIdx > -1 {
		return id.tracker.ackCumulative(int(id.batchIdx))
	}
	return true
}

func (id *trackingMessageID) prev() *trackingMessageID {
	return &trackingMessageID{
		messageID: &messageID{
			ledgerID:     id.ledgerID,
			entryID:      id.entryID - 1,
			partitionIdx: id.partitionIdx,
		},
		tracker:  id.tracker,
		consumer: id.consumer,
	}
}

func (id *messageID) isEntryIDValid() bool {
	return id.entryID >= 0
}

func (id *messageID) greater(other *messageID) bool {
	if id.ledgerID != other.ledgerID {
		return id.ledgerID > other.ledgerID
	}

	if id.entryID != other.entryID {
		return id.entryID > other.entryID
	}

	return id.batchIdx > other.batchIdx
}

func (id *messageID) equal(other *messageID) bool {
	return id.ledgerID == other.ledgerID &&
		id.entryID == other.entryID &&
		id.batchIdx == other.batchIdx
}

func (id *messageID) greaterEqual(other *messageID) bool {
	return id.equal(other) || id.greater(other)
}

func (id *messageID) Serialize() []byte {
	msgID := &pb.MessageIdData{
		LedgerId:   proto.Uint64(uint64(id.ledgerID)),
		EntryId:    proto.Uint64(uint64(id.entryID)),
		BatchIndex: proto.Int32(id.batchIdx),
		Partition:  proto.Int32(id.partitionIdx),
		BatchSize:  proto.Int32(id.batchSize),
	}
	data, _ := proto.Marshal(msgID)
	return data
}

func (id *messageID) LedgerID() int64 {
	return id.ledgerID
}

func (id *messageID) EntryID() int64 {
	return id.entryID
}

func (id *messageID) BatchIdx() int32 {
	return id.batchIdx
}

func (id *messageID) PartitionIdx() int32 {
	return id.partitionIdx
}

func (id *messageID) BatchSize() int32 {
	return id.batchSize
}

func (id *messageID) String() string {
	return fmt.Sprintf("%d:%d:%d", id.ledgerID, id.entryID, id.partitionIdx)
}

func deserializeMessageID(data []byte) (MessageID, error) {
	msgID := &pb.MessageIdData{}
	err := proto.Unmarshal(data, msgID)
	if err != nil {
		return nil, err
	}
	id := newMessageID(
		int64(msgID.GetLedgerId()),
		int64(msgID.GetEntryId()),
		msgID.GetBatchIndex(),
		msgID.GetPartition(),
		msgID.GetBatchSize(),
	)
	return id, nil
}

func newMessageID(ledgerID int64, entryID int64, batchIdx int32, partitionIdx int32, batchSize int32) MessageID {
	return &messageID{
		ledgerID:     ledgerID,
		entryID:      entryID,
		batchIdx:     batchIdx,
		partitionIdx: partitionIdx,
		batchSize:    batchSize,
	}
}

func fromMessageID(msgID MessageID) *messageID {
	return &messageID{
		ledgerID:     msgID.LedgerID(),
		entryID:      msgID.EntryID(),
		batchIdx:     msgID.BatchIdx(),
		partitionIdx: msgID.PartitionIdx(),
		batchSize:    msgID.BatchSize(),
	}
}

func newTrackingMessageID(ledgerID int64, entryID int64, batchIdx int32, partitionIdx int32, batchSize int32,
	tracker *ackTracker) *trackingMessageID {
	return &trackingMessageID{
		messageID: &messageID{
			ledgerID:     ledgerID,
			entryID:      entryID,
			batchIdx:     batchIdx,
			partitionIdx: partitionIdx,
			batchSize:    batchSize,
		},
		tracker:      tracker,
		receivedTime: time.Now(),
	}
}

// checkMessageIDType checks if the MessageID is user-defined
func checkMessageIDType(msgID MessageID) (valid bool) {
	switch msgID.(type) {
	case *trackingMessageID:
		return true
	case *chunkMessageID:
		return true
	case *messageID:
		return true
	default:
		return false
	}
}

func toTrackingMessageID(msgID MessageID) (trackingMsgID *trackingMessageID) {
	if mid, ok := msgID.(*trackingMessageID); ok {
		return mid
	}
	return &trackingMessageID{
		messageID: fromMessageID(msgID),
	}
}

func timeFromUnixTimestampMillis(timestamp uint64) time.Time {
	ts := int64(timestamp) * int64(time.Millisecond)
	seconds := ts / int64(time.Second)
	nanos := ts - (seconds * int64(time.Second))
	return time.Unix(seconds, nanos)
}

// EncryptionContext
// It will be used to decrypt message outside of this client
type EncryptionContext struct {
	Keys             map[string]EncryptionKey
	Param            []byte
	Algorithm        string
	CompressionType  CompressionType
	UncompressedSize int
	BatchSize        int
}

// EncryptionKey
// Encryption key used to encrypt the message payload
type EncryptionKey struct {
	KeyValue []byte
	Metadata map[string]string
}

type message struct {
	publishTime         time.Time
	eventTime           time.Time
	key                 string
	orderingKey         string
	producerName        string
	payLoad             []byte
	msgID               MessageID
	properties          map[string]string
	topic               string
	replicationClusters []string
	replicatedFrom      string
	redeliveryCount     uint32
	schema              Schema
	schemaVersion       []byte
	schemaInfoCache     *schemaInfoCache
	encryptionContext   *EncryptionContext
	index               *uint64
	brokerPublishTime   *time.Time
}

func (msg *message) Topic() string {
	return msg.topic
}

func (msg *message) Properties() map[string]string {
	return msg.properties
}

func (msg *message) Payload() []byte {
	return msg.payLoad
}

func (msg *message) ID() MessageID {
	return msg.msgID
}

func (msg *message) PublishTime() time.Time {
	return msg.publishTime
}

func (msg *message) EventTime() time.Time {
	return msg.eventTime
}

func (msg *message) Key() string {
	return msg.key
}

func (msg *message) OrderingKey() string {
	return msg.orderingKey
}

func (msg *message) RedeliveryCount() uint32 {
	return msg.redeliveryCount
}

func (msg *message) IsReplicated() bool {
	return msg.replicatedFrom != ""
}

func (msg *message) GetReplicatedFrom() string {
	return msg.replicatedFrom
}

func (msg *message) GetSchemaValue(v interface{}) error {
	if msg.schemaVersion != nil {
		schema, err := msg.schemaInfoCache.Get(msg.schemaVersion)
		if err != nil {
			return err
		}
		return schema.Decode(msg.payLoad, v)
	}
	return msg.schema.Decode(msg.payLoad, v)
}

func (msg *message) SchemaVersion() []byte {
	return msg.schemaVersion
}

func (msg *message) ProducerName() string {
	return msg.producerName
}

func (msg *message) GetEncryptionContext() *EncryptionContext {
	return msg.encryptionContext
}

func (msg *message) Index() *uint64 {
	return msg.index
}

func (msg *message) BrokerPublishTime() *time.Time {
	return msg.brokerPublishTime
}

func (msg *message) size() int {
	return len(msg.payLoad)
}

func newAckTracker(size uint) *ackTracker {
	batchIDs := bitset.New(size)
	for i := uint(0); i < size; i++ {
		batchIDs.Set(i)
	}
	return &ackTracker{
		size:     size,
		batchIDs: batchIDs,
	}
}

type ackTracker struct {
	sync.Mutex
	size           uint
	batchIDs       *bitset.BitSet
	prevBatchAcked uint32
}

func (t *ackTracker) ack(batchID int) bool {
	if batchID < 0 {
		return true
	}
	t.Lock()
	defer t.Unlock()
	t.batchIDs.Clear(uint(batchID))
	return t.batchIDs.None()
}

func (t *ackTracker) ackCumulative(batchID int) bool {
	if batchID < 0 {
		return true
	}
	t.Lock()
	defer t.Unlock()
	for i := 0; i <= batchID; i++ {
		t.batchIDs.Clear(uint(i))
	}
	return t.batchIDs.None()
}

func (t *ackTracker) hasPrevBatchAcked() bool {
	return atomic.LoadUint32(&t.prevBatchAcked) == 1
}

func (t *ackTracker) setPrevBatchAcked() {
	atomic.StoreUint32(&t.prevBatchAcked, 1)
}

func (t *ackTracker) completed() bool {
	t.Lock()
	defer t.Unlock()
	return t.batchIDs.None()
}

func (t *ackTracker) toAckSet() []int64 {
	t.Lock()
	defer t.Unlock()
	if t.batchIDs.None() {
		return nil
	}
	bytes := t.batchIDs.Bytes()
	ackSet := make([]int64, len(bytes))
	for i := 0; i < len(bytes); i++ {
		ackSet[i] = int64(bytes[i])
	}
	return ackSet
}

type chunkMessageID struct {
	*messageID

	firstChunkID *messageID
	receivedTime time.Time

	consumer acker
}

func newChunkMessageID(firstChunkID *messageID, lastChunkID *messageID) *chunkMessageID {
	return &chunkMessageID{
		messageID:    lastChunkID,
		firstChunkID: firstChunkID,
		receivedTime: time.Now(),
	}
}

func (id chunkMessageID) String() string {
	return fmt.Sprintf("%s;%s", id.firstChunkID.String(), id.messageID.String())
}

func (id chunkMessageID) Serialize() []byte {
	msgID := &pb.MessageIdData{
		LedgerId:   proto.Uint64(uint64(id.ledgerID)),
		EntryId:    proto.Uint64(uint64(id.entryID)),
		BatchIndex: proto.Int32(id.batchIdx),
		Partition:  proto.Int32(id.partitionIdx),
		FirstChunkMessageId: &pb.MessageIdData{
			LedgerId:   proto.Uint64(uint64(id.firstChunkID.ledgerID)),
			EntryId:    proto.Uint64(uint64(id.firstChunkID.entryID)),
			BatchIndex: proto.Int32(id.firstChunkID.batchIdx),
			Partition:  proto.Int32(id.firstChunkID.partitionIdx),
		},
	}
	data, _ := proto.Marshal(msgID)
	return data
}
