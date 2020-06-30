package crash

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func (r *Report) packageTempDir() error {
	tempDir := r.TempDir()
	packageName := fmt.Sprintf("%s.tar.gz", tempDir)

	// package and compress tempDir to buffer
	var buffer bytes.Buffer
	if err := compress(tempDir, &buffer); err != nil {
		return err
	}

	// create package file and copy buffer
	packageFile, err := os.OpenFile(packageName, os.O_CREATE|os.O_RDWR, os.FileMode(0700))
	if err != nil {
		return err
	}
	defer packageFile.Close()

	if _, err = io.Copy(packageFile, &buffer); err != nil {
		return err
	}

	// remove tempDir
	if err = os.RemoveAll(tempDir); err != nil {
		return err
	}

	return nil
}

// from https://gist.githubusercontent.com/mimoo/25fc9716e0f1353791f5908f94d6e726/raw/4383bf695f4ba8bbb0687d83b0882bb7e71fd521/compress_tar_gzip.go
func compress(src string, buf io.Writer) error {
	// tar > gzip > buf
	zr := gzip.NewWriter(buf)
	tw := tar.NewWriter(zr)

	// walk through every file in the folder
	err := filepath.Walk(src, func(file string, fi os.FileInfo, err error) error {
		// generate tar header
		header, err := tar.FileInfoHeader(fi, file)
		if err != nil {
			return err
		}

		// must provide real name
		// (see https://golang.org/src/archive/tar/common.go?#L626)
		header.Name = filepath.ToSlash(file)

		// write header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		// if not a dir, write file content
		if !fi.IsDir() {
			data, err := os.Open(file)
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, data); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	// produce tar
	if err = tw.Close(); err != nil {
		return err
	}

	// produce gzip
	if err := zr.Close(); err != nil {
		return err
	}

	return nil
}
