package reporting

import (
	"net/http"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestShouldReport(t *testing.T) {
	RegisterTestingT(t)

	// Test case 1: Event failure case
	metric1 := &types.PUReportedMetric{
		StatusDetail: &types.StatusDetail{
			StatusCode: http.StatusBadRequest,
		},
	}
	Expect(shouldReport(metric1)).To(BeTrue())

	// Test case 2: Filter event case
	metric2 := &types.PUReportedMetric{
		StatusDetail: &types.StatusDetail{
			StatusCode: types.FilterEventCode,
		},
	}
	Expect(shouldReport(metric2)).To(BeTrue())

	// Test case 3: Supress event case
	metric3 := &types.PUReportedMetric{
		StatusDetail: &types.StatusDetail{
			StatusCode: types.SuppressEventCode,
		},
	}
	Expect(shouldReport(metric3)).To(BeTrue())

	// Test case 4: Success cases
	metric4 := &types.PUReportedMetric{
		StatusDetail: &types.StatusDetail{
			StatusCode: http.StatusOK,
		},
	}
	Expect(shouldReport(metric4)).To(BeFalse())
}
