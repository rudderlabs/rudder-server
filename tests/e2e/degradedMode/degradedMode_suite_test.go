package degradedMode_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestDegradedMode(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DegradedMode Suite")
}
