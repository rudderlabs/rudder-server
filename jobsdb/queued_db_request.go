package jobsdb

import (
	"fmt"
	"time"
)

func executeDbRequest[T any](jd *Handle, c *dbRequest[T]) T {
	defer jd.getTimerStat(
		fmt.Sprintf("jobsdb_%s_total_time", c.name),
		c.tags,
	).RecordDuration()()

	var queueEnabled bool
	var queueCap chan struct{}
	switch c.reqType {
	case readReqType:
		queueEnabled = jd.conf.enableReaderQueue
		queueCap = jd.conf.readCapacity
	case writeReqType:
		queueEnabled = jd.conf.enableWriterQueue
		queueCap = jd.conf.writeCapacity
	case undefinedReqType:
		fallthrough
	default:
		panic(fmt.Errorf("unsupported command type: %d", c.reqType))
	}

	if queueEnabled {
		queuedAt := time.Now()
		waitTimeStat := jd.getTimerStat(fmt.Sprintf("jobsdb_%s_wait_time", c.name), c.tags)
		queueCap <- struct{}{}
		defer func() { <-queueCap }()
		waitTimeStat.Since(queuedAt)
	}

	return c.command()
}

type dbReqType int

const (
	undefinedReqType dbReqType = iota
	readReqType
	writeReqType
)

type dbRequest[T any] struct {
	reqType dbReqType
	name    string
	tags    *statTags
	command func() T
}

func newReadDbRequest[T any](name string, tags *statTags, command func() T) *dbRequest[T] {
	return &dbRequest[T]{
		reqType: readReqType,
		name:    name,
		tags:    tags,
		command: command,
	}
}

func newWriteDbRequest[T any](name string, tags *statTags, command func() T) *dbRequest[T] {
	return &dbRequest[T]{
		reqType: writeReqType,
		name:    name,
		tags:    tags,
		command: command,
	}
}
