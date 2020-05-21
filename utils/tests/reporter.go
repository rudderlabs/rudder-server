package testutils

import (
	"fmt"

	"github.com/onsi/ginkgo/config"
	"github.com/onsi/ginkgo/reporters"
)

// NewJUnitReporter creates a new JUnit compatible reporter that will output to a junit_<node_id>.xml file in each suite's directory.
func NewJUnitReporter() reporters.Reporter {
	return reporters.NewJUnitReporter(fmt.Sprintf("junit_%d.xml", config.GinkgoConfig.ParallelNode))
}
