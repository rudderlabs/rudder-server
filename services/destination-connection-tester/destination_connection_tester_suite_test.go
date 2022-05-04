package destination_connection_tester

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDestinationDebugger(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DestinationConnectionTester Suite")
}
