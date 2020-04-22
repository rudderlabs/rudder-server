package eventSchema_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestEventSchema(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EventSchema Suite")
}
