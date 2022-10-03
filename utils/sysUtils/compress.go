/*
Zip & Gzip Interfaces Use this instead of archive/zip and compress/gzip packages.

usage example

import "github.com/rudderlabs/rudder-server/utils/sysUtils"

var	GZip sysUtils.GZipI = &sysUtils.GZip{}
			or
var	GZip sysUtils.GZipI = sysUtils.NewGZip()

...

GZip.NewWriter(...)
*/

//go:generate mockgen -destination=../../mocks/utils/sysUtils/mock_compress.go -package mock_sysUtils github.com/rudderlabs/rudder-server/utils/sysUtils GZipI
package sysUtils

import (
	"compress/gzip"
	"io"
)

type GZipI interface {
	NewReader(r io.Reader) (*gzip.Reader, error)
	NewWriter(w io.Writer) *gzip.Writer
}

type GZip struct{}

// NewZip returns a Zip instance
func NewGZip() GZipI {
	return &GZip{}
}

// NewWriter returns a new Writer.
// Writes to the returned writer are compressed and written to w.
//
// It is the caller's responsibility to call Close on the Writer when done.
// Writes may be buffered and not flushed until Close.
//
// Callers that wish to set the fields in Writer.Header must do so before
// the first call to Write, Flush, or Close.
func (*GZip) NewWriter(w io.Writer) *gzip.Writer {
	return gzip.NewWriter(w)
}

// NewReader creates a new Reader reading the given reader.
// If r does not also implement io.ByteReader,
// the decompressor may read more data than necessary from r.
//
// It is the caller's responsibility to call Close on the Reader when done.
//
// The Reader.Header fields will be valid in the Reader returned.
func (*GZip) NewReader(r io.Reader) (*gzip.Reader, error) {
	return gzip.NewReader(r)
}
