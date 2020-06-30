package crash

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

// ReportFileHandler is a function responsible for producing contents of a report file.
type ReportFileHandler func(file *os.File) error

// Report contains information about a crash report
type Report struct {
	// base directory in which report files, and temporary directory, will be created.
	BaseDir string

	// generic metadata about the application state during a crash.
	Metadata map[string]interface{}

	// PanicInformation related to this report
	PanicInformation PanicInformation

	// registered ReportFileHandlers that will produce report files on Save()
	// the map's key corresponds to the filename
	handlers map[string]ReportFileHandler

	startedAt time.Time // holds when Save() was called
}

// RegisterFileHandler registers a new ReportFileHandler on this Report,
// that will run during report's Save(), and is responsible for create contents of file with provided name.
func (r *Report) RegisterFileHandler(name string, handler ReportFileHandler) {
	if r.handlers == nil {
		r.handlers = make(map[string]ReportFileHandler)
	}
	r.handlers[name] = handler
}

func (r *Report) ensureDir(dir string) error {
	fileInfo, err := os.Stat(dir)

	if os.IsNotExist(err) {
		err = os.MkdirAll(dir, os.FileMode(0700))
		if err != nil {
			return fmt.Errorf("Could not create crash report dir '%v': '%w'", dir, err)
		}
	} else if err != nil {
		return err
	} else if !fileInfo.IsDir() {
		return fmt.Errorf("Crash report base dir '%v' already exists and is not a directory", r.BaseDir)
	}

	if err != nil {
		return fmt.Errorf("Could not open crash report's base dir: '%v': %w", r.BaseDir, err)
	}

	return nil
}

// EnsureTempDir creates a temporary directory in BaseDir, if it does not exist.
// Any report files will be stored there, and the directory will be compressed to .tar.gz on Save()
func (r *Report) EnsureTempDir() {
	tempDir := r.TempDir()
	err := r.ensureDir(tempDir)
	if err != nil {
		panic(err)
	}
}

// TempDir returns the path of the directory that will be used to store all report files.
// It should be removed after it is compressed to .tar.gz successfully on Save().
func (r *Report) TempDir() string {
	if r.startedAt.IsZero() {
		r.startedAt = time.Now()
	}

	return path.Join(r.BaseDir, fmt.Sprintf("crash-report-%d", r.startedAt.UnixNano()))
}

func (r *Report) filePath(file string) string {
	return path.Join(r.TempDir(), file)
}

// Save stores the report file.
// TODO: Explain
func (r *Report) Save() error {
	r.startedAt = time.Now()

	r.EnsureTempDir()

	if err := r.generateReportFiles(); err != nil {
		return fmt.Errorf("Could not generate report files: %w", err)
	}

	if err := r.packageTempDir(); err != nil {
		return fmt.Errorf("Could not package report: %w", err)
	}

	return nil
}

func (r *Report) generateReportFiles() (err error) {
	// store report's metadata
	_, err = r.SaveMetadata()
	if err != nil {
		logger.Errorf("Could not store crash report metadata")
	}

	// run any registered report handlers, for custom report files
	for name, handler := range r.handlers {
		filePath := r.filePath(name)
		logger.Infof("Crash Report for: %s", path.Dir(filePath))
		err := r.ensureDir(path.Dir(filePath))
		if err != nil {
			logger.Errorf("Could not read or create base directory for file '%v': '%v'", name, err)
		}

		file, err := os.Create(filePath)
		if err != nil {
			logger.Errorf("Could not create report file '%v': '%v'", name, err)
		}

		defer file.Close()

		err = handler(file)
		if err != nil {
			logger.Errorf("Could not generate contents of file '%v': '%v'", name, err)
		}
	}

	return
}

func (r *Report) SaveMetadata() (metadataPath string, err error) {
	metadataPath = r.filePath("report.json")
	file, err := os.Create(metadataPath)
	if err != nil {
		err = fmt.Errorf("Could not create report metadata file '%v': %w", metadataPath, err)
		return
	}

	defer file.Close()

	metadata := r.metadata()
	err = WriteMapToFile(metadata, file)
	if err != nil {
		err = fmt.Errorf("Could not write report metadata file '%v': %w", metadataPath, err)
		return
	}

	return
}

// Compress will compress the report's BaseDir and contents, and remove the original BaseDir
// The compressed file's path will be returned, if no error occurred.
// If an error occurred, the compress file is not generated, and the report directory is not removed.
func (r *Report) Compress() {

}

func (r *Report) metadata() map[string]interface{} {
	metadata := r.Metadata

	metadata["report"] = r.reportMetadata()
	metadata["panic"] = r.panicMetadata()

	return metadata
}

func (r *Report) reportMetadata() map[string]interface{} {
	metadata := make(map[string]interface{})

	metadata["started_at"] = r.startedAt

	return metadata
}

func (r *Report) panicMetadata() map[string]interface{} {
	metadata := make(map[string]interface{})

	if r.PanicInformation.Error != nil {
		metadata["error"] = r.PanicInformation.Error.Error()
	}

	metadata["received_at"] = r.PanicInformation.ReceivedAt
	metadata["trace"] = r.PanicInformation.Trace
	metadata["context"] = r.PanicInformation.Context

	return metadata
}

// ReportHandler is the Crash handler responsible for creating the report file.
func (r *Report) ReportHandler(pi PanicInformation) {
	r.PanicInformation = pi
	if err := r.Save(); err != nil {
		logger.Errorf("Could not generate crash report: %v", err)
	}
}

// WriteMapToFile is a utility function that dumps contents of a map to a file.
func WriteMapToFile(data map[string]interface{}, file *os.File) (err error) {
	marshalled, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		err = fmt.Errorf("Could not marshal report metadata: %w", err)
		return
	}

	reader := bytes.NewReader(marshalled)
	_, err = io.Copy(file, reader)

	return
}
