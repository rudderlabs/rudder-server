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
		options = append(options, lz4.CompressionLevelOption(lz4.Level2))
	case "level3":
		options = append(options, lz4.CompressionLevelOption(lz4.Level3))
	case "level4":
		options = append(options, lz4.CompressionLevelOption(lz4.Level4))
	case "level5":
		options = append(options, lz4.CompressionLevelOption(lz4.Level5))
	case "level6":
		options = append(options, lz4.CompressionLevelOption(lz4.Level6))
	case "level7":
		options = append(options, lz4.CompressionLevelOption(lz4.Level7))
	case "level8":
		options = append(options, lz4.CompressionLevelOption(lz4.Level8))
	case "level9":
		options = append(options, lz4.CompressionLevelOption(lz4.Level9))
	default:
		return nil, errors.New("invalid compression level: " + compressionLevel)
	}

	return &compressor{
		compressOpts:   options,
		decompressOpts: []lz4.Option{lz4.ConcurrencyOption(concurrency)},
		pool:           &Lz4Pool{},
	}, nil
}

type compressor struct {
	compressOpts   []lz4.Option
	decompressOpts []lz4.Option
	pool           *Lz4Pool
}

func (c *compressor) Compress(data []byte) ([]byte, error) {
	var bb bytes.Buffer
	w, err := c.pool.GetWriter(&bb, c.compressOpts...)
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
	r, err := c.pool.GetReader(bytes.NewReader(data), c.decompressOpts...)
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
