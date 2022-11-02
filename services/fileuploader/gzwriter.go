package fileuploader

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

type MultiFileWriter interface {
	// Write writes the given data to the file at the given path.
	Write(path string, data []byte) (int, error)
	// Close closes all the files.
	Close() error
}

type GZFileHandler struct {
	// gzWriters is a map of path to GZipWriter.
	gzWriters map[string]misc.GZipWriter
}

func NewGzWriter() *GZFileHandler {
	return &GZFileHandler{
		gzWriters: make(map[string]misc.GZipWriter),
	}
}

func (g *GZFileHandler) Write(path string, data []byte) (count int, err error) {
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

func (g *GZFileHandler) Close() error {
	for path, writer := range g.gzWriters {
		if err := writer.CloseGZ(); err != nil {
			return fmt.Errorf("closing gz file %q: %w", path, err)
		}
	}
	return nil
}
