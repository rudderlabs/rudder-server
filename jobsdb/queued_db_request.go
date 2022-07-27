package jobsdb

import (
	"fmt"
	"time"
)

type earlyQueueToken struct {
	waitTime time.Duration
	redeemed bool
	queue    chan struct{}
}

func (t *earlyQueueToken) redeem() {
	if !t.redeemed {
		t.redeemed = true
		<-t.queue
	}
}

func (jd *HandleT) executeDbRequest(c *dbRequest) interface{} {
	totalTimeStat := jd.getTimerStat(fmt.Sprintf("%s_total_time", c.name), c.tags)
	var waitTime time.Duration
	now := time.Now()
	defer func() {
		totalTimeStat.SendTiming(time.Since(now) + waitTime)
	}()

	var queueEnabled bool
	var queueCap chan struct{}
	switch c.reqType {
	case readReqType:
		queueEnabled = jd.enableReaderQueue
		queueCap = jd.readCapacity
	case writeReqType:
		queueEnabled = jd.enableWriterQueue
		queueCap = jd.writeCapacity
	case undefinedReqType:
		fallthrough
	default:
		panic(fmt.Errorf("unsupported command type: %d", c.reqType))
	}

	if queueEnabled {
		waitTimeStat := jd.getTimerStat(fmt.Sprintf("%s_wait_time", c.name), c.tags)
		if c.eqt != nil && !c.eqt.redeemed { // in case we have a token and it isn't redeemed yet we will use it
			waitTime = c.eqt.waitTime
			waitTimeStat.SendTiming(waitTime)
			defer func() { c.eqt.redeem() }()
		} else {
			waitTimeStat.Start()
			queueCap <- struct{}{}
			waitTimeStat.End()
			defer func() { <-queueCap }()
		}
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
	eqt     *earlyQueueToken
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

func newWriteDbRequest(name string, tags *statTags, command func() interface{}, eqt *earlyQueueToken) *dbRequest {
	return &dbRequest{
		reqType: writeReqType,
		name:    name,
		tags:    tags,
		command: command,
		eqt:     eqt,
	}
}
