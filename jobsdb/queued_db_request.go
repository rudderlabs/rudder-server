package jobsdb

import (
	"fmt"
)

type queuedDbRequest struct {
	execute  func() interface{}
	response chan interface{}
}

func (jd *HandleT) executeDbRequest(c *dbRequest) interface{} {
	totalTimeStat := jd.getTimerStat(fmt.Sprintf("%s_total_time", c.name), c.tags)
	totalTimeStat.Start()
	defer totalTimeStat.End()

	var queueEnabled bool
	var queueChannel chan *queuedDbRequest
	switch c.reqType {
	case readReqType:
		queueEnabled = jd.enableReaderQueue
		queueChannel = jd.readChannel
	case writeReqType:
		queueEnabled = jd.enableWriterQueue
		queueChannel = jd.writeChannel
	case undefinedReqType:
		fallthrough
	default:
		panic(fmt.Errorf("unsupported command type: %d", c.reqType))
	}

	if queueEnabled {
		waitTimeStat := jd.getTimerStat(fmt.Sprintf("%s_wait_time", c.name), c.tags)
		waitTimeStat.Start()
		respCh := make(chan interface{})
		queueChannel <- &queuedDbRequest{execute: c.command, response: respCh}
		waitTimeStat.End()
		err := <-respCh
		return err
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
