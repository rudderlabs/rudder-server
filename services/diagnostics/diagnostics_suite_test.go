package diagnostics_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestDiagnostics(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Diagnostics Suite")
}
