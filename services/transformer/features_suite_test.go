package transformer

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTransformerFeatures(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Transformer features Suite")
}
