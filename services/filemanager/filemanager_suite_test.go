package filemanager_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestFilemanager(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Filemanager Suite")
}
