package transientsource

import (
	"context"
	"encoding/json"
	"sync"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/tidwall/gjson"
)

// Service provides services related to transient source ids
type Service interface {

	// SourceIdsSupplier provides an up-to-date supplier of transient source ids
	SourceIdsSupplier() func() []string

	// Apply performs transient source_id filtering logic against a single source_id.
	// If it corresponds to a transient source_id, the result will be true, otherwise false.
	Apply(sourceId string) bool

	// ApplyJob performs transient source_id filtering logic against a single job.
	// If the job corresponds to a transient source_id, the result will be true, otherwise false.
	ApplyJob(job *jobsdb.JobT) bool

	// ApplyParams performs transient source_id filtering logic against a job's parameters.
	// If the parameters contain a transient source_id, the result will be true, otherwise false.
	ApplyParams(params json.RawMessage) bool
}

// NewService creates a new service that updates its transient source ids while
// backend configuration gets updated.
func NewService(ctx context.Context, config backendconfig.BackendConfig) Service {
	s := &service{
		init: make(chan struct{}),
	}
	go s.updateLoop(ctx, config)
	return s
}

// NewEmptyService creates a new service that operates against an empty list of transient source ids
// Useful for tests, when you are not interested in testing for transient sources.
func NewEmptyService() Service {
	return NewStaticService([]string{})
}

// NewStaticService creates a new service that operates against a predefined list of transient source ids.
// Useful for tests.
func NewStaticService(sourceIds []string) Service {
	s := &service{
		init:         make(chan struct{}),
		sourceIds:    sourceIds,
		sourceIdsMap: asMap(sourceIds),
	}
	close(s.init)
	return s
}

type service struct {
	onceInit     sync.Once
	init         chan struct{}
	sourceIds    []string
	sourceIdsMap map[string]struct{}
}

func (r *service) SourceIdsSupplier() func() []string {
	return func() []string {
		<-r.init
		return r.sourceIds
	}
}

func (r *service) Apply(sourceId string) bool {
	<-r.init
	_, ok := r.sourceIdsMap[sourceId]
	return ok
}

func (r *service) ApplyParams(params json.RawMessage) bool {
	sourceId := gjson.GetBytes(params, "source_id").String()
	return r.Apply(sourceId)
}

func (r *service) ApplyJob(job *jobsdb.JobT) bool {
	return r.ApplyParams(job.Parameters)
}

// updateLoop uses backend config to retrieve & keep up-to-date the list of transient source ids
func (r *service) updateLoop(ctx context.Context, config backendconfig.BackendConfig) {

	ch := make(chan pubsub.DataEvent)
	config.Subscribe(ch, backendconfig.TopicBackendConfig)

	for {
		select {
		case ev := <-ch:
			c := ev.Data.(backendconfig.ConfigT)
			newSourceIds := transientSourceIds(&c)
			r.sourceIds = newSourceIds
			r.sourceIdsMap = asMap(newSourceIds)
			r.onceInit.Do(func() {
				close(r.init)
			})
		case <-ctx.Done():
			r.onceInit.Do(func() {
				close(r.init)
			})
			return
		}
	}
}

// transientSourceIds scans a backend configuration and extracts
// source ids which have the following configuration option
//
//    transient : true
//
func transientSourceIds(c *backendconfig.ConfigT) []string {
	r := make([]string, 0)
	for i := range c.Sources {
		source := &c.Sources[i]
		if source.Config["transient"] == true {
			r = append(r, source.ID)
		}
	}
	return r
}

// asMap converts a slice of strings to a set, i.e. a map of strings to empty structs
func asMap(arr []string) map[string]struct{} {
	res := map[string]struct{}{}
	for _, excludedSourceId := range arr {
		res[excludedSourceId] = struct{}{}
	}
	return res
}
