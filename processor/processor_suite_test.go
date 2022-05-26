package processor_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestProcessor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Processor Suite")
}
