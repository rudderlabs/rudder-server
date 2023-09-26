// Copyright (c) 2020-2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"bytes"
	"encoding/base64"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type arrowResultChunk struct {
	reader    *ipc.Reader
	rowCount  int
	loc       *time.Location
	allocator memory.Allocator
}

func (arc *arrowResultChunk) decodeArrowChunk(rowType []execResponseRowType, highPrec bool) ([]chunkRowType, error) {
	logger.Debug("Arrow Decoder")
	var chunkRows []chunkRowType

	for arc.reader.Next() {
		record := arc.reader.Record()

		start := len(chunkRows)
		numRows := int(record.NumRows())
		columns := record.Columns()
		chunkRows = append(chunkRows, make([]chunkRowType, numRows)...)
		for i := start; i < start+numRows; i++ {
			chunkRows[i].ArrowRow = make([]snowflakeValue, len(columns))
		}

		for colIdx, col := range columns {
			values := make([]snowflakeValue, numRows)
			if err := arrowToValue(values, rowType[colIdx], col, arc.loc, highPrec); err != nil {
				return nil, err
			}

			for i := range values {
				chunkRows[start+i].ArrowRow[colIdx] = values[i]
			}
		}
		arc.rowCount += numRows
	}

	return chunkRows, arc.reader.Err()
}

func (arc *arrowResultChunk) decodeArrowBatch(scd *snowflakeChunkDownloader) (*[]arrow.Record, error) {
	var records []arrow.Record
	defer arc.reader.Release()

	for arc.reader.Next() {
		rawRecord := arc.reader.Record()

		record, err := arrowToRecord(rawRecord, arc.allocator, scd.RowSet.RowType, arc.loc)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}

	return &records, arc.reader.Err()
}

// Build arrow chunk based on RowSet of base64
func buildFirstArrowChunk(rowsetBase64 string, loc *time.Location, alloc memory.Allocator) arrowResultChunk {
	rowSetBytes, err := base64.StdEncoding.DecodeString(rowsetBase64)
	if err != nil {
		return arrowResultChunk{}
	}
	rr, err := ipc.NewReader(bytes.NewReader(rowSetBytes), ipc.WithAllocator(alloc))
	if err != nil {
		return arrowResultChunk{}
	}

	return arrowResultChunk{rr, 0, loc, alloc}
}
