package diagnostics_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDiagnostics(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Diagnostics Suite")
}
