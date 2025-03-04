package gzip

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

func NewCompressor() Compressor {
	return &compressor{
		pool: &GzipPool{},
	}
}

type compressor struct {
	pool *GzipPool
}

func (c *compressor) Compress(data []byte) ([]byte, error) {
	var bb bytes.Buffer
	w, err := c.pool.GetWriter(&bb)
	if err != nil {
		return nil, err
	}
	defer c.pool.PutWriter(w)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return bb.Bytes(), nil
}

func (c *compressor) Decompress(data []byte) ([]byte, error) {
	r, err := c.pool.GetReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer c.pool.PutReader(r)
	res, err := io.ReadAll(r)
	if err != nil && !errors.Is(err, io.EOF) {
		fmt.Println(string(res))
		return res, err
	}
	return res, nil
}
