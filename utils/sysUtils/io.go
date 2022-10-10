/*
Io & IoUtil Interfaces Use this instead of Io and Io/ioutil packages.

usage example

import "github.com/rudderlabs/rudder-server/utils/sysUtils"

var	Io sysUtils.IoI = &sysUtils.Io{}
var	IoUtil sysUtils.IoUtilI = &sysUtils.IoUtil{}
			or
var	Io sysUtils.IoI = sysUtils.NewIo()
var	IoUtil sysUtils.IoUtilI = sysUtils.NewIoUtil()

...

Io.Copy(...)
IoUtil.Copy(...)
*/
//go:generate mockgen -destination=../../mocks/utils/sysUtils/mock_io.go -package mock_sysUtils github.com/rudderlabs/rudder-server/utils/sysUtils IoI,IoUtilI

package sysUtils

import (
	"io"
	"os"
)

type IoI interface {
	Copy(dst io.Writer, src io.Reader) (written int64, err error)
}

type Io struct{}

// NewIo returns an Io instance
func NewIo() IoI {
	return &Io{}
}

// Copy copies from src to dst until either EOF is reached
// on src or an error occurs. It returns the number of bytes
// copied and the first error encountered while copying, if any.
//
// A successful Copy returns err == nil, not err == EOF.
// Because Copy is defined to read from src until EOF, it does
// not treat an EOF from Read as an error to be reported.
//
// If src implements the WriterTo interface,
// the copy is implemented by calling src.WriteTo(dst).
// Otherwise, if dst implements the ReaderFrom interface,
// the copy is implemented by calling dst.ReadFrom(src).
func (*Io) Copy(dst io.Writer, src io.Reader) (written int64, err error) {
	return io.Copy(dst, src)
}

type IoUtilI interface {
	ReadFile(filename string) ([]byte, error)
	WriteFile(filename string, data []byte, perm os.FileMode) error
	ReadAll(r io.Reader) ([]byte, error)
	NopCloser(r io.Reader) io.ReadCloser
}

type IoUtil struct{}

// NewIo returns an Io instance
func NewIoUtil() IoUtilI {
	return &IoUtil{}
}

// ReadFile reads the file named by filename and returns the contents.
// A successful call returns err == nil, not err == EOF. Because ReadFile
// reads the whole file, it does not treat an EOF from Read as an error
// to be reported.
func (*IoUtil) ReadFile(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

// WriteFile writes data to a file named by filename.
// If the file does not exist, WriteFile creates it with permissions perm
// (before umask); otherwise WriteFile truncates it before writing.
func (*IoUtil) WriteFile(filename string, data []byte, perm os.FileMode) error {
	return os.WriteFile(filename, data, perm)
}

// ReadAll reads from r until an error or EOF and returns the data it read.
// A successful call returns err == nil, not err == EOF. Because ReadAll is
// defined to read from src until EOF, it does not treat an EOF from Read
// as an error to be reported.
func (*IoUtil) ReadAll(r io.Reader) ([]byte, error) {
	return io.ReadAll(r)
}

// NopCloser returns a ReadCloser with a no-op Close method wrapping
// the provided Reader r.
func (*IoUtil) NopCloser(r io.Reader) io.ReadCloser {
	return io.NopCloser(r)
}
