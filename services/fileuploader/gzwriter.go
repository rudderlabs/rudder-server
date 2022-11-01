package fileuploader

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

type FileHandler interface {
	Write(workspaceID, path string, data []byte) (int, error)
	Close() (map[string]string, error)
}

type GZFileHandler struct {
	gzWriters map[string]misc.GZipWriter
	dumps     map[string]string
}

func NewGzWriter() *GZFileHandler {
	return &GZFileHandler{
		gzWriters: make(map[string]misc.GZipWriter),
	}
}

func (g *GZFileHandler) Write(workspaceID, path string, data []byte) (count int, err error) {
	if _, ok := g.gzWriters[workspaceID]; !ok {

		err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
		if err != nil {
			return 0, fmt.Errorf("creating gz file %q: %w", path, err)
		}

		g.gzWriters[workspaceID], err = misc.CreateGZ(path)
		g.dumps[workspaceID] = path
		if err != nil {
			return 0, err
		}
	}
	return g.gzWriters[workspaceID].Write(data)
}

func (g *GZFileHandler) Close() (map[string]string, error) {
	for workspaceID, path := range g.dumps {
		if err := g.gzWriters[workspaceID].CloseGZ(); err != nil {
			return g.dumps, fmt.Errorf("closing gz file %q: %w", path, err)
		}
	}
	return g.dumps, nil
}
