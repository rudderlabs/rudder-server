/*
Zip & Gzip Interfaces Use this instead of archive/zip and compress/gzip packages.

usage example

import "github.com/rudderlabs/rudder-server/utils/sysUtils"

var	Zip sysUtils.ZipI = &sysUtils.Zip{}
			or
var	Zip sysUtils.ZipI = sysUtils.NewZip()

...

Zip.OpenReader(...)
*/
//go:generate mockgen -destination=../../mocks/utils/sysUtils/mock_archive.go -package mock_sysUtils github.com/rudderlabs/rudder-server/utils/sysUtils ZipI
package sysUtils

import (
	"archive/zip"
	"io"
	"os"
)

type ZipI interface {
	NewWriter(w io.Writer) *zip.Writer
	FileInfoHeader(fi os.FileInfo) (*zip.FileHeader, error)
	OpenReader(name string) (*zip.ReadCloser, error)
}
type Zip struct{}

// NewZip returns a Zip instance
func NewZip() ZipI {
	return &Zip{}
}

// NewWriter returns a new Writer writing a zip file to w.
func (*Zip) NewWriter(w io.Writer) *zip.Writer {
	return zip.NewWriter(w)
}

// FileInfoHeader creates a partially-populated FileHeader from an
// os.FileInfo.
// Because os.FileInfo's Name method returns only the base name of
// the file it describes, it may be necessary to modify the Name field
// of the returned header to provide the full path name of the file.
// If compression is desired, callers should set the FileHeader.Method
// field; it is unset by default.
func (*Zip) FileInfoHeader(fi os.FileInfo) (*zip.FileHeader, error) {
	return zip.FileInfoHeader(fi)
}

// OpenReader will open the Zip file specified by name and return a ReadCloser.
func (*Zip) OpenReader(name string) (*zip.ReadCloser, error) {
	return zip.OpenReader(name)
}
