package gzip

import (
	"compress/gzip"
	"io"
	"sync"
)

// GzipPool manages a pool of gzip.Writer.
// The pool uses sync.Pool internally.
type GzipPool struct {
	readers sync.Pool
	writers sync.Pool
}

// GetReader returns gzip.Reader from the pool, or creates a new one
// if the pool is empty.
func (pool *GzipPool) GetReader(src io.Reader) (reader *gzip.Reader, err error) {
	if r := pool.readers.Get(); r != nil {
		reader = r.(*gzip.Reader)
		if err := reader.Reset(src); err != nil {
			pool.PutReader(reader)
			return nil, err
		}
		return reader, nil
	} else {
		return gzip.NewReader(src)
	}
}

// PutReader closes and returns a gzip.Reader to the pool
// so that it can be reused via GetReader.
func (pool *GzipPool) PutReader(reader *gzip.Reader) {
	reader.Close()
	pool.readers.Put(reader)
}

// GetWriter returns gzip.Writer from the pool, or creates a new one
// with gzip.BestCompression if the pool is empty.
func (pool *GzipPool) GetWriter(dst io.Writer) (writer *gzip.Writer, err error) {
	if w := pool.writers.Get(); w != nil {
		writer = w.(*gzip.Writer)
		writer.Reset(dst)
		return writer, nil
	} else {
		return gzip.NewWriterLevel(dst, gzip.BestSpeed)
	}
}

// PutWriter closes and returns a gzip.Writer to the pool
// so that it can be reused via GetWriter.
func (pool *GzipPool) PutWriter(writer *gzip.Writer) {
	_ = writer.Close()
	pool.writers.Put(writer)
}
