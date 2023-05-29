package warehouse_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestWarehouse(t *testing.T) {
	t.Skip() // TODO: Remove this line when you have tests to run
	RegisterFailHandler(Fail)
	RunSpecs(t, "Warehouse Suite")
}
