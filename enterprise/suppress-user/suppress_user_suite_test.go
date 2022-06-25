package suppression_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSuppressUser(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SuppressUser Suite")
}
