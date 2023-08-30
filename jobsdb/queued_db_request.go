package jobsdb

import (
	"fmt"
	"time"
)

func (jd *HandleT) executeDbRequest(c *dbRequest) interface{} {
	defer jd.getTimerStat(
		fmt.Sprintf("%s_total_time", c.name),
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
		waitTimeStat := jd.getTimerStat(fmt.Sprintf("%s_wait_time", c.name), c.tags)
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

type dbRequest struct {
	reqType dbReqType
	name    string
	tags    *statTags
	command func() interface{}
}

func newReadDbRequest(name string, tags *statTags, command func() interface{}) *dbRequest {
	return &dbRequest{
		reqType: readReqType,
		name:    name,
		tags:    tags,
		command: command,
	}
}

func newWriteDbRequest(name string, tags *statTags, command func() interface{}) *dbRequest {
	return &dbRequest{
		reqType: writeReqType,
		name:    name,
		tags:    tags,
		command: command,
	}
}
