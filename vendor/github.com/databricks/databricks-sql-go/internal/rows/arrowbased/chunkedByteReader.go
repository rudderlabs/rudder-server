package arrowbased

import (
	"fmt"
	"io"

	dbsqlerr "github.com/databricks/databricks-sql-go/internal/errors"
	"github.com/pkg/errors"
)

var errChunkedByteReaderInvalidState1 = "databricks: chunkedByteReader invalid state chunkIndex:%d, byteIndex:%d"
var errChunkedByteReaderOverreadOfNonterminalChunk = "databricks: chunkedByteReader invalid state chunks:%d chunkIndex:%d len:%d byteIndex%d"

// chunkedByteReader implements the io.Reader interface on a collection
// of byte arrays.
// The TSparkArrowBatch instances returned in TFetchResultsResp contain
// a byte array containing an ipc formatted MessageRecordBatch message.
// The ipc reader expects a bytestream containing a MessageSchema message
// followed by a MessageRecordBatch message.
// chunkedByteReader is used to avoid allocating new byte arrays for each
// TSparkRecordBatch and copying the schema and record bytes.
type chunkedByteReader struct {
	// byte slices to be read as a single slice
	chunks [][]byte
	// index of the chunk being read from
	chunkIndex int
	// index in the current chunk
	byteIndex int
}

// Read reads up to len(p) bytes into p. It returns the number of bytes
// read (0 <= n <= len(p)) and any error encountered.
//
// When Read encounters an error or end-of-file condition after
// successfully reading n > 0 bytes, it returns the number of
// bytes read and a non-nil error.  If len(p) is zero Read will
// return 0, nil
//
// Callers should always process the n > 0 bytes returned before
// considering the error err. Doing so correctly handles I/O errors
// that happen after reading some bytes.
func (c *chunkedByteReader) Read(p []byte) (bytesRead int, err error) {
	err = c.isValid()

	for err == nil && bytesRead < len(p) && !c.isEOF() {
		chunk := c.chunks[c.chunkIndex]
		chunkLen := len(chunk)
		source := chunk[c.byteIndex:]

		n := copy(p[bytesRead:], source)
		bytesRead += n

		c.byteIndex += n

		if c.byteIndex >= chunkLen {
			c.byteIndex = 0
			c.chunkIndex += 1
		}
	}

	if err != nil {
		err = dbsqlerr.WrapErr(err, "datbricks: read failure in chunked byte reader")
	} else if c.isEOF() {
		err = io.EOF
	}

	return bytesRead, err
}

// isEOF returns true if the chunkedByteReader is in
// an end-of-file condition.
func (c *chunkedByteReader) isEOF() bool {
	return c.chunkIndex >= len(c.chunks)
}

// reset returns the chunkedByteReader to its initial state
func (c *chunkedByteReader) reset() {
	c.byteIndex = 0
	c.chunkIndex = 0
}

// verify that the chunkedByteReader is in a valid state
func (c *chunkedByteReader) isValid() error {
	if c == nil {
		return errors.New("call to Read on nil chunkedByteReader")
	}
	if c.byteIndex < 0 || c.chunkIndex < 0 {
		return errors.New(fmt.Sprintf(errChunkedByteReaderInvalidState1, c.chunkIndex, c.byteIndex))
	}

	if c.chunkIndex < len(c.chunks)-1 {
		chunkLen := len(c.chunks[c.chunkIndex])
		if 0 < chunkLen && c.byteIndex >= chunkLen {
			return errors.New(fmt.Sprintf(errChunkedByteReaderOverreadOfNonterminalChunk, len(c.chunks), c.chunkIndex, len(c.chunks[c.chunkIndex]), c.byteIndex))
		}
	}
	return nil
}
