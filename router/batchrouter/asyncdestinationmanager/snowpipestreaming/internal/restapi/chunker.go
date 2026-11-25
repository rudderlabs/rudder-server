package restapi

import (
	"bytes"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/compress"
)

// compressor is shared: it is safe for concurrent use.
var compressor = mustCompressor()

func mustCompressor() *compress.Compressor {
	c, err := compress.New(compress.CompressionAlgoZstd, compress.CompressionLevelZstdDefault)
	if err != nil {
		panic(fmt.Errorf("creating zstd compressor: %w", err))
	}
	return c
}

// Chunk is a zstd-compressed NDJSON body holding the rows First..Last (inclusive) of the input.
type Chunk struct {
	Body  []byte
	First int
	Last  int
}

// Chunker splits rows into request bodies that stay under the 4 MiB compressed limit, at row boundaries and in order.
type Chunker struct {
	// maxRawBytes bounds the uncompressed size of a chunk before compressing it.
	// Chunks that still compress above the request limit are halved until they fit.
	maxRawBytes int
	// maxBodyBytes is the compressed size limit of a request body.
	maxBodyBytes int
}

// NewChunker returns a chunker that compresses at most maxRawBytes of NDJSON per attempt.
func NewChunker(maxRawBytes int) *Chunker {
	return &Chunker{maxRawBytes: maxRawBytes, maxBodyBytes: maxRequestBodyBytes}
}

// Chunk returns the chunks for the given JSON-encoded rows, in order. Each row must be a single line of JSON.
// Rows that don't fit in a request even on their own are left out of every chunk, and their indexes returned as oversized.
func (c *Chunker) Chunk(rows [][]byte) (chunks []Chunk, oversized []int, err error) {
	for first := 0; first < len(rows); {
		last, rawSize := first, len(rows[first])+1
		for last+1 < len(rows) && rawSize+len(rows[last+1])+1 <= c.maxRawBytes {
			last++
			rawSize += len(rows[last]) + 1
		}
		if chunks, oversized, err = c.fit(rows, first, last, chunks, oversized); err != nil {
			return nil, nil, err
		}
		first = last + 1
	}
	return chunks, oversized, nil
}

// fit compresses rows first..last, halving the range until every part fits in a request.
func (c *Chunker) fit(rows [][]byte, first, last int, chunks []Chunk, oversized []int) ([]Chunk, []int, error) {
	var raw bytes.Buffer
	for _, row := range rows[first : last+1] {
		raw.Write(row)
		raw.WriteByte('\n')
	}
	body, err := compressor.Compress(raw.Bytes())
	if err != nil {
		return nil, nil, fmt.Errorf("compressing rows: %w", err)
	}
	if len(body) <= c.maxBodyBytes {
		return append(chunks, Chunk{Body: body, First: first, Last: last}), oversized, nil
	}
	if first == last {
		return chunks, append(oversized, first), nil
	}
	mid := first + (last-first)/2
	if chunks, oversized, err = c.fit(rows, first, mid, chunks, oversized); err != nil {
		return nil, nil, err
	}
	return c.fit(rows, mid+1, last, chunks, oversized)
}
