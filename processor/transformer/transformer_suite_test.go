package transformer_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTransformer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Transformer Suite")
}
