package lz4

import (
	"bytes"
	"errors"
	"io"

	"github.com/pierrec/lz4/v4"
)

type Compressor interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

func NewCompressor(concurrency int, compressionLevel string) (Compressor, error) {
	var options []lz4.Option
	options = append(options, lz4.ConcurrencyOption(concurrency))
	switch compressionLevel {
	case "fastest":
		options = append(options, lz4.CompressionLevelOption(lz4.Fast))
	case "level1":
		options = append(options, lz4.CompressionLevelOption(lz4.Level1))
	case "level2":
		options = append(options, lz4.CompressionLevelOption(lz4.Level3))
	case "level3":
		options = append(options, lz4.CompressionLevelOption(lz4.Level6))
	default:
		return nil, errors.New("invalid compression level: " + compressionLevel)
	}

	return &compressor{
		options: options,
		pool:    &Lz4Pool{},
	}, nil
}

type compressor struct {
	options []lz4.Option
	pool    *Lz4Pool
}

func (c *compressor) Compress(data []byte) ([]byte, error) {
	var bb bytes.Buffer
	w, err := c.pool.GetWriter(&bb, c.options...)
	if err != nil {
		return nil, err
	}
	defer c.pool.PutWriter(w)
	defer w.Close()
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
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
		return res, err
	}
	return res, nil
}
