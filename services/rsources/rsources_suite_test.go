package rsources

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestRsources(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rsources Suite")
}
