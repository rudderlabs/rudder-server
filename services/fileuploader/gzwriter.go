package fileuploader

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// MultiFileWriter can write to multiple paths at the same time.
type MultiFileWriter interface {
	// Write writes the given data to the file at the given path.
	Write(path string, data []byte) (int, error)
	// Close closes all open files.
	Close() error

	// Count returns the number of open files.
	Count() int
}

type gzFileHandler struct {
	// gzWriters is a map of path to GZipWriter.
	gzWriters map[string]misc.GZipWriter
}

// NewGzMultiFileWriter creates a new MultiFileWriter that writes to multiple gz-compressed files.
func NewGzMultiFileWriter() MultiFileWriter {
	return &gzFileHandler{
		gzWriters: make(map[string]misc.GZipWriter),
	}
}

func (g *gzFileHandler) Write(path string, data []byte) (count int, err error) {
	if _, ok := g.gzWriters[path]; !ok {

		err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
		if err != nil {
			return 0, fmt.Errorf("creating gz file %q: mkdir error: %w", path, err)
		}

		g.gzWriters[path], err = misc.CreateGZ(path)
		if err != nil {
			return 0, err
		}
	}
	return g.gzWriters[path].Write(data)
}

func (g *gzFileHandler) Close() error {
	for path, writer := range g.gzWriters {
		if err := writer.CloseGZ(); err != nil {
			return fmt.Errorf("closing gz file %q: %w", path, err)
		}
	}
	g.gzWriters = make(map[string]misc.GZipWriter)
	return nil
}

func (g *gzFileHandler) Count() int {
	return len(g.gzWriters)
}
