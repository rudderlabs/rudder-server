package integrations_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestIntegrations(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integrations Suite")
}
