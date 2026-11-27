package restapi

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// encoder is shared: EncodeAll is safe for concurrent use. It is klauspost's pure-Go zstd, as rudder-go-kit/compress
// uses for its non-cgo mode; rudder-go-kit/compress itself can't be used, since it also links DataDog/zstd, which needs
// cgo, and rudder-server is built with cgo disabled.
var encoder = mustEncoder()

func mustEncoder() *zstd.Encoder {
	e, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		panic(fmt.Errorf("creating zstd encoder: %w", err))
	}
	return e
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
func (c *Chunker) Chunk(rows [][]byte) (chunks []Chunk, oversized []int) {
	for first := 0; first < len(rows); {
		last, rawSize := first, len(rows[first])+1
		for last+1 < len(rows) && rawSize+len(rows[last+1])+1 <= c.maxRawBytes {
			last++
			rawSize += len(rows[last]) + 1
		}
		chunks, oversized = c.fit(rows, first, last, chunks, oversized)
		first = last + 1
	}
	return chunks, oversized
}

// fit compresses rows first..last, halving the range until every part fits in a request.
func (c *Chunker) fit(rows [][]byte, first, last int, chunks []Chunk, oversized []int) ([]Chunk, []int) {
	var raw bytes.Buffer
	for _, row := range rows[first : last+1] {
		raw.Write(row)
		raw.WriteByte('\n')
	}
	body := encoder.EncodeAll(raw.Bytes(), nil)
	if len(body) <= c.maxBodyBytes {
		return append(chunks, Chunk{Body: body, First: first, Last: last}), oversized
	}
	if first == last {
		return chunks, append(oversized, first)
	}
	mid := first + (last-first)/2
	chunks, oversized = c.fit(rows, first, mid, chunks, oversized)
	return c.fit(rows, mid+1, last, chunks, oversized)
}
