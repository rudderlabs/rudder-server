package jobsdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

const expected = "expected"

func Test_executeDbRequest_read_direct(t *testing.T) {
	h := Handle{
		stats: stats.NOP,
	}

	res := executeDbRequest(context.Background(), &h, &dbRequest[string]{
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
	res := executeDbRequest(context.Background(), &h, &dbRequest[string]{
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

	res := executeDbRequest(context.Background(), &h, &dbRequest[string]{
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
	res := executeDbRequest(context.Background(), &h, &dbRequest[string]{
		reqType: writeReqType,
		name:    "test",
		tags:    nil,
		command: func() string { return expected },
	})

	require.Equal(t, expected, res, "Unexpected result")
}

func Test_executeDbRequest_priority_pool_context_without_pool_uses_queue(t *testing.T) {
	// Test that when priorityPool context is set but priorityPool is nil,
	// the request still goes through the normal queue path
	h := Handle{
		stats: stats.NOP,
	}
	h.conf.enableReaderQueue = true
	h.conf.readCapacity = make(chan struct{}, 1) // capacity of 1, not filled

	// Use priority pool context but with no priorityPool set on handle
	ctx := WithPriorityPool(context.Background())
	h.priorityPool = nil

	// This should use the queue since priorityPool is nil
	res := executeDbRequest(ctx, &h, &dbRequest[string]{
		reqType: readReqType,
		name:    "test",
		tags:    nil,
		command: func() string { return expected },
	})

	require.Equal(t, expected, res, "Unexpected result")
}
