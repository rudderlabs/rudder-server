package multitenant_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMultitenant(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Multitenant Suite")
}
