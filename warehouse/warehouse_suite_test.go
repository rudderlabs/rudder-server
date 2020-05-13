package warehouse_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	testutils "github.com/rudderlabs/rudder-server/utils/tests"
)

func TestUtils(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecsWithDefaultAndCustomReporters(t, "Warehouse Suite", []Reporter{testutils.NewJUnitReporter()})
}
