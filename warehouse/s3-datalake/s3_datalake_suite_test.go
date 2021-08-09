package s3datalake_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestS3Datalake(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "S3Datalake Suite")
}
