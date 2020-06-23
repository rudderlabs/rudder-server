package streammanager_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestStreammanager(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Streammanager Suite")
}
