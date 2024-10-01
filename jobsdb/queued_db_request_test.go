package jobsdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

const expected = "expected"

func Test_executeDbRequest_read_direct(t *testing.T) {
	h := Handle{
		stats: stats.NOP,
	}

	res := executeDbRequest(&h, &dbRequest[string]{
		reqType: readReqType,
		name:    "test",
		tags:    nil,
		command: func() string { return expected },
	})

	require.Equal(t, expected, res, "Unexpected result")
}

func Test_executeDbRequest_read_channel(t *testing.T) {
	h := Handle{
		stats: stats.NOP,
	}
	h.conf.enableReaderQueue = true
	h.conf.readCapacity = make(chan struct{}, 1)
	res := executeDbRequest(&h, &dbRequest[string]{
		reqType: readReqType,
		name:    "test",
		tags:    nil,
		command: func() string { return expected },
	})

	require.Equal(t, expected, res, "Unexpected result")
}

func Test_executeDbRequest_write_direct(t *testing.T) {
	h := Handle{
		stats: stats.NOP,
	}

	res := executeDbRequest(&h, &dbRequest[string]{
		reqType: writeReqType,
		name:    "test",
		tags:    nil,
		command: func() string { return expected },
	})

	require.Equal(t, expected, res, "Unexpected result")
}

func Test_executeDbRequest_write_channel(t *testing.T) {
	h := Handle{
		stats: stats.NOP,
	}
	h.conf.enableWriterQueue = true
	h.conf.writeCapacity = make(chan struct{}, 1)
	res := executeDbRequest(&h, &dbRequest[string]{
		reqType: writeReqType,
		name:    "test",
		tags:    nil,
		command: func() string { return expected },
	})

	require.Equal(t, expected, res, "Unexpected result")
}
