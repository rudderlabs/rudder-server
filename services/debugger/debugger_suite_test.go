package debugger_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestDebugger(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Debugger Suite")
}
