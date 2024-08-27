package warehouseutils

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

func Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	gzipWriter := gzip.NewWriter(&buf)
	_, err := gzipWriter.Write(data)
	if err != nil {
		return nil, fmt.Errorf("writing data: %w", err)
	}

	err = gzipWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("closing gzip writer: %w", err)
	}

	return buf.Bytes(), nil
}

func Decompress(data []byte) ([]byte, error) {
	gzipReader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("creating gzip reader: %w", err)
	}
	defer func() {
		_ = gzipReader.Close()
	}()

	var out bytes.Buffer
	_, err = io.Copy(&out, gzipReader)
	if err != nil {
		return nil, fmt.Errorf("copying data: %w", err)
	}
	return out.Bytes(), nil
}
