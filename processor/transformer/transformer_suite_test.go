package transformer_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTransformer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Transformer Suite")
}
