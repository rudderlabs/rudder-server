package warehouse_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestWarehouse(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Warehouse Suite")
}
