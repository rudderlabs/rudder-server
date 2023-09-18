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
	"bufio"
	"fmt"
	"io"

	pb "github.com/apache/pulsar-client-go/pulsar/internal/pulsar_proto"
	"google.golang.org/protobuf/proto"
)

type connectionReader struct {
	cnx    *connection
	buffer Buffer
	reader *bufio.Reader
}

func newConnectionReader(cnx *connection) *connectionReader {
	return &connectionReader{
		cnx:    cnx,
		reader: bufio.NewReader(cnx.cnx),
		buffer: NewBuffer(4096),
	}
}

func (r *connectionReader) readFromConnection() {
	for {
		cmd, headersAndPayload, err := r.readSingleCommand()
		if err != nil {
			if !r.cnx.closed() {
				r.cnx.log.WithError(err).Infof("Error reading from connection")
				r.cnx.Close()
			}
			break
		}

		// Process
		var payloadLen uint32
		if headersAndPayload != nil {
			payloadLen = headersAndPayload.ReadableBytes()
		}
		r.cnx.log.Debug("Got command! ", cmd, " with payload size: ", payloadLen, " maxMsgSize: ", r.cnx.maxMessageSize)
		r.cnx.receivedCommand(cmd, headersAndPayload)
	}
}

func (r *connectionReader) readSingleCommand() (cmd *pb.BaseCommand, headersAndPayload Buffer, err error) {
	// First, we need to read the frame size
	if r.buffer.ReadableBytes() < 4 {
		if r.buffer.ReadableBytes() == 0 {
			// If the buffer is empty, just go back to write at the beginning
			r.buffer.Clear()
		}
		if err := r.readAtLeast(4); err != nil {
			return nil, nil, fmt.Errorf("unable to read frame size: %+v", err)
		}
	}

	// We have enough to read frame size
	frameSize := r.buffer.ReadUint32()
	maxFrameSize := r.cnx.maxMessageSize + MessageFramePadding
	if r.cnx.maxMessageSize != 0 && int32(frameSize) > maxFrameSize {
		frameSizeError := fmt.Errorf("received too big frame size=%d maxFrameSize=%d", frameSize, maxFrameSize)
		r.cnx.log.Error(frameSizeError)
		r.cnx.Close()
		return nil, nil, frameSizeError
	}

	// Next, we read the rest of the frame
	if r.buffer.ReadableBytes() < frameSize {
		remainingBytes := frameSize - r.buffer.ReadableBytes()
		if err := r.readAtLeast(remainingBytes); err != nil {
			return nil, nil,
				fmt.Errorf("unable to read frame: %+v", err)
		}
	}

	// We have now the complete frame
	cmdSize := r.buffer.ReadUint32()
	cmd, err = r.deserializeCmd(r.buffer.Read(cmdSize))
	if err != nil {
		return nil, nil, err
	}

	// Also read the eventual payload
	headersAndPayloadSize := frameSize - (cmdSize + 4)
	if cmdSize+4 < frameSize {
		headersAndPayload = NewBuffer(int(headersAndPayloadSize))
		headersAndPayload.Write(r.buffer.Read(headersAndPayloadSize))
	}
	return cmd, headersAndPayload, nil
}

func (r *connectionReader) readAtLeast(size uint32) error {
	if r.buffer.WritableBytes() < size {
		// There's not enough room in the current buffer to read the requested amount of data
		totalFrameSize := r.buffer.ReadableBytes() + size
		if r.buffer.ReadableBytes()+size > r.buffer.Capacity() {
			// Resize to a bigger buffer to avoid continuous resizing
			r.buffer.Resize(totalFrameSize * 2)
		} else {
			// Compact the buffer by moving the partial data to the beginning.
			// This will have enough room for reading the remainder of the data
			r.buffer.MoveToFront()
		}
	}

	n, err := io.ReadAtLeast(r.cnx.cnx, r.buffer.WritableSlice(), int(size))
	if err != nil {
		// has the connection been closed?
		if r.cnx.closed() {
			return errConnectionClosed
		}
		r.cnx.Close()
		return err
	}

	r.buffer.WrittenBytes(uint32(n))
	return nil
}

func (r *connectionReader) deserializeCmd(data []byte) (*pb.BaseCommand, error) {
	cmd := &pb.BaseCommand{}
	err := proto.Unmarshal(data, cmd)
	if err != nil {
		r.cnx.log.WithError(err).Warn("Failed to parse protobuf command")
		r.cnx.Close()
		return nil, err
	}
	return cmd, nil
}
