package sourcedebugger_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestSourceDebugger(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SourceDebugger Suite")
}
