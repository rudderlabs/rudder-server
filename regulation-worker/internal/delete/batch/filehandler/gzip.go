package filehandler

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/rudderlabs/rudder-go-kit/jsonparser"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type GZIPLocalFileHandler struct {
	records     []byte
	idFieldPath []string
}

func NewGZIPLocalFileHandler(idFieldPath []string) *GZIPLocalFileHandler {
	return &GZIPLocalFileHandler{
		idFieldPath: idFieldPath,
	}
}

func (h *GZIPLocalFileHandler) Read(_ context.Context, path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("error while opening compressed file, %w", err)
	}

	defer func() {
		_ = f.Close()
	}()

	gzipReader, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("error while reading compressed file: %w", err)
	}

	byt, err := io.ReadAll(gzipReader)
	if err != nil {
		return fmt.Errorf("unable to read contents of local file: %w", err)
	}

	h.records = byt
	return nil
}

func (h *GZIPLocalFileHandler) Write(_ context.Context, path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("error while opening file, %w", err)
	}

	defer func() {
		_ = f.Close()
	}()

	gw := gzip.NewWriter(f)
	defer gw.Close()

	_, err = gw.Write(h.records)
	if err != nil {
		return fmt.Errorf("error while writing cleaned & compressed data:%w", err)
	}

	return nil
}

func (h *GZIPLocalFileHandler) RemoveIdentity(_ context.Context, attributes []model.User) error {
	if len(h.idFieldPath) == 0 {
		return fmt.Errorf("id field path cannot be empty for native deletion")
	}

	suppress := make(map[string]struct{}, len(attributes))
	for _, a := range attributes {
		suppress[a.ID] = struct{}{}
	}

	w := 0
	data := h.records
	for len(data) > 0 {
		n := bytes.IndexByte(data, '\n')
		var line []byte
		if n < 0 {
			line = data
			data = nil
		} else {
			line = data[:n+1]
			data = data[n+1:]
		}

		id := jsonparser.GetStringOrEmpty(line, h.idFieldPath...)
		if _, found := suppress[id]; found {
			continue
		}
		copy(h.records[w:], line)
		w += len(line)
	}
	h.records = h.records[:w]
	return nil
}
