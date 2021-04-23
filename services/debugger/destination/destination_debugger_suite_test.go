package destinationdebugger_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestDestinationDebugger(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DestinationDebugger Suite")
}
