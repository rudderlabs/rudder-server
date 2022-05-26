package jobsdb

import (
	"testing"

	"github.com/golang/mock/gomock"
	mock_stats "github.com/rudderlabs/rudder-server/mocks/services/stats"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/stretchr/testify/require"
)

const expected = "expected"

func Test_executeDbRequest_read_direct(t *testing.T) {
	initMocks(t)

	h := HandleT{}

	res := h.executeDbRequest(&dbRequest{
		reqType: readReqType,
		name:    "test",
		tags:    nil,
		command: func() interface{} { return expected },
	}).(string)

	require.Equal(t, expected, res, "Unexpected result")
}

func Test_executeDbRequest_read_channel(t *testing.T) {
	initMocks(t)

	h := HandleT{
		enableReaderQueue: true,
		readCapacity:      make(chan struct{}, 1),
	}
	res := h.executeDbRequest(&dbRequest{
		reqType: readReqType,
		name:    "test",
		tags:    nil,
		command: func() interface{} { return expected },
	}).(string)

	require.Equal(t, expected, res, "Unexpected result")
}

func Test_executeDbRequest_write_direct(t *testing.T) {
	initMocks(t)

	h := HandleT{}

	res := h.executeDbRequest(&dbRequest{
		reqType: writeReqType,
		name:    "test",
		tags:    nil,
		command: func() interface{} { return expected },
	}).(string)

	require.Equal(t, expected, res, "Unexpected result")
}

func Test_executeDbRequest_write_channel(t *testing.T) {
	initMocks(t)

	h := HandleT{
		enableWriterQueue: true,
		writeCapacity:     make(chan struct{}, 1),
	}
	res := h.executeDbRequest(&dbRequest{
		reqType: writeReqType,
		name:    "test",
		tags:    nil,
		command: func() interface{} { return expected },
	}).(string)

	require.Equal(t, expected, res, "Unexpected result")
}

func initMocks(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockStats := mock_stats.NewMockStats(ctrl)
	mockRudderStats := mock_stats.NewMockRudderStats(ctrl)

	mockStats.EXPECT().NewTaggedStat(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(mockRudderStats)
	mockRudderStats.EXPECT().Start().AnyTimes()
	mockRudderStats.EXPECT().End().AnyTimes()

	stats.DefaultStats = mockStats
}
