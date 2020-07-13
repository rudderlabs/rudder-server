package eventbridge_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestEventBridge(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EventBridge Suite")
}
