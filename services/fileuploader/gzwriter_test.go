package fileuploader

import (
	"os"
	"testing"

	. "github.com/onsi/gomega"
)

func TestGzWriter(t *testing.T) {
	RegisterTestingT(t)
	gzWriter := NewGzMultiFileWriter()
	_, err := gzWriter.Write("test", []byte("test1"))
	Expect(err).To(BeNil())
	Expect(gzWriter.Count()).To(Equal(1))

	_, err = gzWriter.Write("test", []byte("test2"))
	Expect(err).To(BeNil())
	Expect(gzWriter.Count()).To(Equal(1))

	_, err = gzWriter.Write("make", []byte("test3"))
	Expect(err).To(BeNil())
	Expect(gzWriter.Count()).To(Equal(2))

	err = gzWriter.Close()
	Expect(err).To(BeNil())

	Expect(gzWriter.Count()).To(Equal(0))
	os.Remove("test")
	os.Remove("make")
}
