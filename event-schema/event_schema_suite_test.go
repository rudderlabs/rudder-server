package event_schema

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEventSchemas(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Event Schemas Suite")
}
