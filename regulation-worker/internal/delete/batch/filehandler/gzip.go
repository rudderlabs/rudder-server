package filehandler

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

type GZIPLocalFileHandler struct {
	records []byte
	casing  Case
}

func NewGZIPLocalFileHandler(casing Case) *GZIPLocalFileHandler {
	return &GZIPLocalFileHandler{
		casing: casing,
	}
}

func (h *GZIPLocalFileHandler) Read(_ context.Context, path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("error while opening compressed file, %w", err)
	}
	defer f.Close()

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

	defer f.Close()

	gw := gzip.NewWriter(f)
	defer gw.Close()

	_, err = gw.Write(h.records)
	if err != nil {
		return fmt.Errorf("error while writing cleaned & compressed data:%w", err)
	}

	return nil
}

func (h *GZIPLocalFileHandler) RemoveIdentity(ctx context.Context, attributes []model.User) error {
	var filteredContent []byte

	for _, attribute := range attributes {
		pattern, err := h.getDeletePattern(attribute)
		if err != nil {
			return fmt.Errorf("failed to get delete pattern: %s", err.Error())
		}

		cmd := exec.CommandContext(ctx, "bash", "-c", fmt.Sprintf("sed -e %s", pattern))
		cmd.Stdin = bytes.NewBuffer(h.records)

		filteredContent, err = cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("filtering the content: %w", err)
		}

		h.records = filteredContent
	}

	return nil
}

func (h *GZIPLocalFileHandler) getDeletePattern(attribute model.User) (string, error) {
	switch h.casing {

	case SnakeCase:
		return fmt.Sprintf("'/\"user_id\": *\"%s\"/d'", attribute.ID), nil
	case CamelCase:
		return fmt.Sprintf("'/\"userId\": *\"%s\"/d'", attribute.ID), nil
	case UpperCase:
		return fmt.Sprintf("'/\"USER_ID\": *\"%s\"/d'", attribute.ID), nil
	default:
		return "", fmt.Errorf("casing value: %v supplied not in list of supported cases", h.casing)
	}
}
