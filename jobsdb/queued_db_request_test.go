package jobsdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const expected = "expected"

func Test_executeDbRequest_read_direct(t *testing.T) {
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
	h := HandleT{}
	h.conf.enableReaderQueue = true
	h.conf.readCapacity = make(chan struct{}, 1)
	res := h.executeDbRequest(&dbRequest{
		reqType: readReqType,
		name:    "test",
		tags:    nil,
		command: func() interface{} { return expected },
	}).(string)

	require.Equal(t, expected, res, "Unexpected result")
}

func Test_executeDbRequest_write_direct(t *testing.T) {
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
	h := HandleT{}
	h.conf.enableWriterQueue = true
	h.conf.writeCapacity = make(chan struct{}, 1)
	res := h.executeDbRequest(&dbRequest{
		reqType: writeReqType,
		name:    "test",
		tags:    nil,
		command: func() interface{} { return expected },
	}).(string)

	require.Equal(t, expected, res, "Unexpected result")
}
