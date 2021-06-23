package jobsdb_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestJobsdb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Jobsdb Suite")
}
