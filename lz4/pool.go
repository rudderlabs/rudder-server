package lz4

import (
	"io"
	"sync"

	"github.com/pierrec/lz4/v4"
)

// Lz4Pool manages a pool of gzip.Writer.
// The pool uses sync.Pool internally.
type Lz4Pool struct {
	readers sync.Pool
	writers sync.Pool
}

// GetReader returns gzip.Reader from the pool, or creates a new one
// if the pool is empty.
func (pool *Lz4Pool) GetReader(src io.Reader, opts ...lz4.Option) (reader *lz4.Reader, err error) {
	if r := pool.readers.Get(); r != nil {
		reader = r.(*lz4.Reader)
		reader.Reset(src)
		return reader, nil
	} else {
		r := lz4.NewReader(src)
		_ = r.Apply(opts...)
		return r, nil
	}
}

// PutReader closes and returns a gzip.Reader to the pool
// so that it can be reused via GetReader.
func (pool *Lz4Pool) PutReader(reader *lz4.Reader) {
	pool.readers.Put(reader)
}

// GetWriter returns gzip.Writer from the pool, or creates a new one
// with gzip.BestCompression if the pool is empty.
func (pool *Lz4Pool) GetWriter(dst io.Writer, opts ...lz4.Option) (writer *lz4.Writer, err error) {
	if w := pool.writers.Get(); w != nil {
		writer = w.(*lz4.Writer)
		writer.Reset(dst)
		return writer, nil
	} else {
		w := lz4.NewWriter(dst)
		_ = w.Apply(opts...)
		return w, nil
	}
}

// PutWriter closes and returns a gzip.Writer to the pool
// so that it can be reused via GetWriter.
func (pool *Lz4Pool) PutWriter(writer *lz4.Writer) {
	writer.Close()
	pool.writers.Put(writer)
}
