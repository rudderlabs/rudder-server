package flusher

import (
	"github.com/segmentio/go-hll"
)

type TrackedUsersFlusher struct {
	*Flusher
}

func NewTrackedUsersFlusher() *TrackedUsersFlusher {
	trackedUsersFlusher := &TrackedUsersFlusher{}
	trackedUsersFlusher.Flusher = NewFlusher(trackedUsersFlusher)
	return trackedUsersFlusher
}

func (t *TrackedUsersFlusher) AddReportToAggregate(aggregatedReports map[string]interface{}, report map[string]interface{}) error {
	return nil
}

func (t *TrackedUsersFlusher) decodeHLL(encoded string) (*hll.Hll, error) {
	return nil, nil
}

func (t *TrackedUsersFlusher) deserialize(report map[string]interface{}) (*TrackedUsersReport, error) {
	return nil, nil
}
