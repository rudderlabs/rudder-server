package warehouseutils_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUtils(t *testing.T) {
	t.Skip("Skip warehouse utils tests") // TODO: Remove this line when you have tests to run
	RegisterFailHandler(Fail)
	RunSpecs(t, "Warehouse Utils Suite")
}
