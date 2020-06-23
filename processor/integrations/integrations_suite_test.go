package integrations_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestIntegrations(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integrations Suite")
}
