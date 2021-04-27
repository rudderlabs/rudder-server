package transformationdebugger_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTransformationDebugger(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "TransformationDebugger Suite")
}
