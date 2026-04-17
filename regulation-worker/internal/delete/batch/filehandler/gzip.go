package filehandler

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/jsonparser"

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

func (h *GZIPLocalFileHandler) RemoveIdentity(ctx context.Context, attributes []model.User) error {
	var filteredContent []byte

	patterns := make([]string, len(attributes))
	for idx, attribute := range attributes {
		pattern, err := h.getDeletePattern(attribute)
		if err != nil {
			return fmt.Errorf("creating delete pattern for userID: %s, %s", attribute.ID, err.Error())
		}
		patterns[idx] = pattern
	}

	cmd := exec.CommandContext(ctx, "bash", "-c", fmt.Sprintf("sed -r -e %s", strings.Join(patterns, " -e ")))
	cmd.Stdin = bytes.NewBuffer(h.records)
	filteredContent, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("filtering the content: %w", err)
	}

	h.records = filteredContent
	return nil
}

func (h *GZIPLocalFileHandler) getDeletePattern(attribute model.User) (string, error) {
	normalized := regexp.QuoteMeta(attribute.ID)
	switch h.casing {

	case SnakeCase:
		return fmt.Sprintf("'/\"user_id\": *\"%s\"/d'", normalized), nil
	case CamelCase:
		return fmt.Sprintf("'/\"userId\": *\"%s\"/d'", normalized), nil
	case UpperCase:
		return fmt.Sprintf("'/\"USER_ID\": *\"%s\"/d'", normalized), nil
	default:
		return "", fmt.Errorf("casing value: %v supplied not in list of supported cases", h.casing)
	}
}

func (h *GZIPLocalFileHandler) RemoveIdentityRE(_ context.Context, attributes []model.User) error {
	key, err := h.idFieldName()
	if err != nil {
		return err
	}
	re, err := regexp.Compile(fmt.Sprintf(`"%s": *"([^"]*)"`, key))
	if err != nil {
		return fmt.Errorf("compiling id extractor regex: %w", err)
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

		drop := false
		rest := line
		for {
			loc := re.FindSubmatchIndex(rest)
			if loc == nil {
				break
			}
			if _, found := suppress[string(rest[loc[2]:loc[3]])]; found {
				drop = true
				break
			}
			rest = rest[loc[1]:]
		}
		if !drop {
			copy(h.records[w:], line)
			w += len(line)
		}
	}
	h.records = h.records[:w]
	return nil
}

func (h *GZIPLocalFileHandler) idFieldName() (string, error) {
	switch h.casing {
	case SnakeCase:
		return "user_id", nil
	case CamelCase:
		return "userId", nil
	case UpperCase:
		return "USER_ID", nil
	default:
		return "", fmt.Errorf("casing value: %v supplied not in list of supported cases", h.casing)
	}
}

// RemoveIdentityPureGo is the proposed shell-injection-free replacement for
// GZIPLocalFileHandler.RemoveIdentity. It decodes each NDJSON line and drops
// records whose configured id field matches any entry in attributes.
func (h *GZIPLocalFileHandler) RemoveIdentityPureGo(_ context.Context, attributes []model.User) error {
	fieldName, err := h.idFieldName()
	if err != nil {
		return err
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

		id := jsonparser.GetStringOrEmpty(line, fieldName)
		if _, found := suppress[id]; found {
			continue
		}
		copy(h.records[w:], line)
		w += len(line)
	}
	h.records = h.records[:w]
	return nil
}
