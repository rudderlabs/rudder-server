package cache

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

const (
	wildcard = "*"
)

type Option[T ParameterFilter] func(*NoResultsCache[T])

// WithWarnOnBranchInvalidation is a config option that enables logging of branch invalidations.
func WithWarnOnBranchInvalidation[T ParameterFilter](enabled config.ValueLoader[bool], logger logger.Logger) Option[T] {
	return func(c *NoResultsCache[T]) {
		c.warnOnBranchInvalidation = enabled
		c.logger = logger
	}
}

// NewNoResultsCache creates a new, properly initialised NoResultsCache.
func NewNoResultsCache[T ParameterFilter](supportedParams []string, ttlFn func() time.Duration, opts ...Option[T]) *NoResultsCache[T] {
	c := &NoResultsCache[T]{
		ttl:                      ttlFn,
		supportedParams:          supportedParams,
		cacheTree:                make(cacheTree),
		warnOnBranchInvalidation: config.SingleValueLoader(false),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type ParameterFilter interface {
	GetName() string
	GetValue() string
}

type NoResultsCache[T ParameterFilter] struct {
	ttl                      func() time.Duration // returns the time to live for a cache entry
	supportedParams          []string             // a list of parameters that are supported by the cache
	warnOnBranchInvalidation config.ValueLoader[bool]
	logger                   logger.Logger

	cacheTreeMu sync.RWMutex // protects the cacheTree
	cacheTree   cacheTree    // a hierarchical tree of cache entries
}

// Get returns true if the cache contains a valid entry for the provided dataset, partitions, workspace, customVals, states and parameters filters.
func (c *NoResultsCache[T]) Get(dataset string, partitions []string, workspace string, customVals, states []string, parameters []T) bool {
	if c.skipCache(states, parameters) {
		return false
	}
	partitionKeys, workspace, states, customVals, params := filtersToCacheKeys(partitions, workspace, states, customVals, parameters)

	c.cacheTreeMu.RLock()
	defer c.cacheTreeMu.RUnlock()

	if _, ok := c.cacheTree[dataset]; !ok {
		return false
	}

	for _, partition := range partitionKeys {
		if _, ok := c.cacheTree[dataset][partition]; !ok {
			return false
		}
		if _, ok := c.cacheTree[dataset][partition][workspace]; !ok {
			return false
		}
		for _, customVal := range customVals {
			if _, ok := c.cacheTree[dataset][partition][workspace][customVal]; !ok {
				return false
			}
			for _, state := range states {
				if _, ok := c.cacheTree[dataset][partition][workspace][customVal][state]; !ok {
					return false
				}
				for _, param := range params {
					if mark, ok := c.cacheTree[dataset][partition][workspace][customVal][state][param]; !ok || !mark.noJobs || time.Now().After(mark.t.Add(c.ttl())) {
						return false
					}
				}
			}
		}
	}

	return true
}

// Invalidate invalidates all cache entries for the provided dataset, partitions, workspace, customVals, states and parameters.
func (c *NoResultsCache[T]) Invalidate(dataset string, partitions []string, workspace string, customVals, states []string, parameters []T) {
	c.cacheTreeMu.Lock()
	defer c.cacheTreeMu.Unlock()
	partitions, workspaces, states, customVals, params := c.filtersToInvalidationKeys(partitions, workspace, states, customVals, parameters)

	if len(partitions) == 0 { // if no partitions are provided, invalidate all by deleting the partitions's parent node
		if c.warnOnBranchInvalidation.Load() && (len(customVals) > 0 || len(states) > 0 || len(parameters) > 0) {
			c.logger.Warnn("Invalidating entire dataset",
				logger.NewStringField("dataset", dataset),
				logger.NewStringField("partitions", strings.Join(partitions, ",")),
				logger.NewStringField("workspace", workspace),
				logger.NewStringField("customVals", strings.Join(customVals, ",")),
				logger.NewStringField("states", strings.Join(states, ",")),
				logger.NewStringField("parameters", strings.Join(lo.Map(parameters, func(pf T, _ int) string { return pf.GetName() + ":" + pf.GetValue() }), ",")),
			)
		}
		delete(c.cacheTree, dataset)
		return
	}
	if _, ok := c.cacheTree[dataset]; !ok {
		return
	}
	for _, partition := range partitions {
		if len(workspaces) == 0 { // if no workspace is provided, invalidate all by deleting the workspace's parent node
			if c.warnOnBranchInvalidation.Load() && (len(customVals) > 0 || len(states) > 0 || len(parameters) > 0) {
				c.logger.Warnn("Invalidating entire partition",
					logger.NewStringField("dataset", dataset),
					logger.NewStringField("partition", partition),
					logger.NewStringField("workspace", workspace),
					logger.NewStringField("customVals", strings.Join(customVals, ",")),
					logger.NewStringField("states", strings.Join(states, ",")),
					logger.NewStringField("parameters", strings.Join(lo.Map(parameters, func(pf T, _ int) string { return pf.GetName() + ":" + pf.GetValue() }), ",")),
				)
			}
			delete(c.cacheTree[dataset], partition)
			continue
		}
		if _, ok := c.cacheTree[dataset][partition]; !ok {
			continue
		}
		for _, workspace := range workspaces {
			if len(customVals) == 0 { // if no custom value is provided, invalidate all by deleting the customVal's parent node
				if c.warnOnBranchInvalidation.Load() {
					c.logger.Warnn("Invalidating entire workspace branch",
						logger.NewStringField("dataset", dataset),
						logger.NewStringField("partition", partition),
						logger.NewStringField("workspace", workspace),
						logger.NewStringField("customVals", strings.Join(customVals, ",")),
						logger.NewStringField("states", strings.Join(states, ",")),
						logger.NewStringField("parameters", strings.Join(lo.Map(parameters, func(pf T, _ int) string { return pf.GetName() + ":" + pf.GetValue() }), ",")),
					)
				}
				delete(c.cacheTree[dataset][partition], workspace)
				continue
			}
			if _, ok := c.cacheTree[dataset][partition][workspace]; !ok {
				continue
			}
			for _, customVal := range customVals {
				if len(states) == 0 { // if no state is provided, invalidate all by deleting the state's parent node
					if c.warnOnBranchInvalidation.Load() {
						c.logger.Warnn("Invalidating entire customVal branch",
							logger.NewStringField("dataset", dataset),
							logger.NewStringField("partition", partition),
							logger.NewStringField("workspace", workspace),
							logger.NewStringField("customVal", customVal),
							logger.NewStringField("states", strings.Join(states, ",")),
							logger.NewStringField("parameters", strings.Join(lo.Map(parameters, func(pf T, _ int) string { return pf.GetName() + ":" + pf.GetValue() }), ",")),
						)
					}
					delete(c.cacheTree[dataset][partition][workspace], customVal)
					continue
				}
				if _, ok := c.cacheTree[dataset][partition][workspace][customVal]; !ok {
					continue
				}
				for _, state := range states {
					if len(params) == 0 { // if no parameter is provided, invalidate all by deleting the param's parent node
						if c.warnOnBranchInvalidation.Load() {
							c.logger.Warnn("Invalidating entire state branch",
								logger.NewStringField("dataset", dataset),
								logger.NewStringField("partition", partition),
								logger.NewStringField("workspace", workspace),
								logger.NewStringField("customVal", customVal),
								logger.NewStringField("state", state),
								logger.NewStringField("parameters", strings.Join(lo.Map(parameters, func(pf T, _ int) string { return pf.GetName() + ":" + pf.GetValue() }), ",")),
							)
						}
						delete(c.cacheTree[dataset][partition][workspace][customVal], state)
						continue
					}
					if _, ok := c.cacheTree[dataset][partition][workspace][customVal][state]; !ok {
						continue
					}
					for _, param := range params {
						if c.warnOnBranchInvalidation.Load() { // if logging is enabled, log the invalidation of the leaf node at debug level, since this is the most granular level
							c.logger.Debugn("Invalidating leaf",
								logger.NewStringField("dataset", dataset),
								logger.NewStringField("workspace", workspace),
								logger.NewStringField("customVal", customVal),
								logger.NewStringField("state", state),
								logger.NewStringField("parameter", param),
							)
						}
						delete(c.cacheTree[dataset][partition][workspace][customVal][state], param)
					}
				}
			}
		}
	}
}

// InvalidateDataset invalidates all cache entries for a given dataset.
func (c *NoResultsCache[T]) InvalidateDataset(dataset string) {
	c.Invalidate(dataset, nil, "", nil, nil, nil)
}

// StartNoResultTx prepares the cache for accepting new no result entries.
// The cache uses a special marker to prevent synchronisation issues between competing calls of Invalidate & SetNoResult.
func (c *NoResultsCache[T]) StartNoResultTx(dataset string, partitions []string, workspace string, customVals, states []string, parameters []T) (tx *NoResultTx[T]) {
	tx = &NoResultTx[T]{
		id:         uuid.New().String(),
		dataset:    dataset,
		partitions: partitions,
		workspace:  workspace,
		customVals: customVals,
		states:     states,
		parameters: parameters,
		c:          c,
	}
	if c.skipCache(states, parameters) {
		return tx
	}
	partitions, workspace, states, customVals, params := filtersToCacheKeys(partitions, workspace, states, customVals, parameters)

	c.cacheTreeMu.Lock()
	defer c.cacheTreeMu.Unlock()

	if _, ok := c.cacheTree[dataset]; !ok {
		c.cacheTree[dataset] = map[string]map[string]map[string]map[string]map[string]cacheEntry{}
	}
	for _, partition := range partitions {
		if _, ok := c.cacheTree[dataset][partition]; !ok {
			c.cacheTree[dataset][partition] = map[string]map[string]map[string]map[string]cacheEntry{}
		}
		if _, ok := c.cacheTree[dataset][partition][workspace]; !ok {
			c.cacheTree[dataset][partition][workspace] = map[string]map[string]map[string]cacheEntry{}
		}
		for _, customVal := range customVals {
			if _, ok := c.cacheTree[dataset][partition][workspace][customVal]; !ok {
				c.cacheTree[dataset][partition][workspace][customVal] = map[string]map[string]cacheEntry{}
			}
			for _, state := range states {
				if _, ok := c.cacheTree[dataset][partition][workspace][customVal][state]; !ok {
					c.cacheTree[dataset][partition][workspace][customVal][state] = map[string]cacheEntry{}
				}
				for _, param := range params {
					e := c.cacheTree[dataset][partition][workspace][customVal][state][param]
					e.AddToken(tx.id)
					c.cacheTree[dataset][partition][workspace][customVal][state][param] = e
				}
			}
		}
	}

	return tx
}

// NoResultTx is a transaction for the NoResultsCache.
type NoResultTx[T ParameterFilter] struct {
	id                 string
	dataset            string
	partitions         []string
	workspace          string
	customVals, states []string
	parameters         []T
	c                  *NoResultsCache[T]
}

// Commit sets the necessary cache entries for the relevant dataset, workspace, states, customVals and parameters filters.
// It returns [true] if the cache was successfully updated for all cache entries, [false] otherwise.
func (tx *NoResultTx[T]) Commit() bool {
	if tx.c.skipCache(tx.states, tx.parameters) {
		return false
	}
	partitions, workspace, states, customVals, params := filtersToCacheKeys(tx.partitions, tx.workspace, tx.states, tx.customVals, tx.parameters)

	tx.c.cacheTreeMu.Lock()
	defer tx.c.cacheTreeMu.Unlock()

	if _, ok := tx.c.cacheTree[tx.dataset]; !ok {
		return false
	}

	var missed bool
	for _, partition := range partitions {
		if _, ok := tx.c.cacheTree[tx.dataset][partition]; !ok {
			missed = true
			continue
		}
		if _, ok := tx.c.cacheTree[tx.dataset][partition][workspace]; !ok {
			missed = true
			continue
		}
		for _, customVal := range customVals {
			if _, ok := tx.c.cacheTree[tx.dataset][partition][workspace][customVal]; !ok {
				missed = true
				continue
			}
			for _, state := range states {
				if _, ok := tx.c.cacheTree[tx.dataset][partition][workspace][customVal][state]; !ok {
					missed = true
					continue
				}
				for _, param := range params {
					e := tx.c.cacheTree[tx.dataset][partition][workspace][customVal][state][param]
					if e.SetNoJobs(tx.id) {
						tx.c.cacheTree[tx.dataset][partition][workspace][customVal][state][param] = e
					} else {
						missed = true
					}
				}
			}
		}
	}

	return !missed
}

// skipCache returns true if the cache should be skipped for the provided states and parameters.
func (c *NoResultsCache[T]) skipCache(states []string, parameters []T) bool {
	// if no state filters are provided, we don't use the cache
	if len(states) == 0 {
		return true
	}
	// if not all parameter filters are a subset of the supported parameters, we don't use the cache
	if !lo.EveryBy(parameters, func(pf T) bool {
		return slices.Contains(c.supportedParams, pf.GetName())
	}) {
		return true
	}
	return false
}

// filtersToCacheKeys returns the cache keys for the provided partition, workspace, states, customVals and parameters filters.
// Wildcards are used if empty parameters are provided.
func filtersToCacheKeys[T ParameterFilter](partitionFilter []string, workspaceFilter string, statesFilter, customValsFilter []string, parametersFilter []T) (partitionKeys []string, workspaceKey string, stateKeys, customValKeys, paramKeys []string) {
	partitionKeys = partitionFilter
	if len(partitionKeys) == 0 { // if no partition is provided, we use the wildcard
		partitionKeys = []string{wildcard}
	}
	workspaceKey = workspaceFilter
	if workspaceKey == "" { // if no workspace is provided, we use the wildcard
		workspaceKey = wildcard
	}
	stateKeys = statesFilter

	customValKeys = customValsFilter
	if len(customValKeys) == 0 { // if no custom value is provided, use the wildcard
		customValKeys = []string{wildcard}
	}
	paramKeys = lo.Map(parametersFilter, func(pf T, _ int) string {
		return pf.GetName() + ":" + pf.GetValue()
	})
	if len(paramKeys) == 0 { // if no parameter is provided, we use the wildcard
		paramKeys = []string{wildcard}
	}
	return partitionKeys, workspaceKey, stateKeys, customValKeys, paramKeys
}

// filtersToInvalidationKeys returns the cache keys that need to be invalidated for the provided workspace, states, customVals and parameters filters.
// Wildcard keys are also returned if needed. An empty slice is returned if all keys need to be invalidated at that level.
func (c *NoResultsCache[T]) filtersToInvalidationKeys(partitionFilter []string, workspaceFilter string, statesFilter, customValsFilter []string, parametersFilter []T) (partitionKeys, workspaceKeys, stateKeys, customValKeys, paramKeys []string) {
	if len(partitionFilter) > 0 { // include partitions along with the wildcard
		partitionKeys = make([]string, len(partitionFilter)+1)
		copy(partitionKeys, partitionFilter)
		partitionKeys[len(partitionFilter)] = wildcard
	}
	if workspaceFilter != "" {
		workspaceKeys = []string{workspaceFilter, wildcard}
	}
	stateKeys = statesFilter
	if len(customValsFilter) > 0 { // include customVals along with the wildcard
		customValKeys = make([]string, len(customValsFilter)+1)
		copy(customValKeys, customValsFilter)
		customValKeys[len(customValsFilter)] = wildcard
	}

	paramKeys = lo.FilterMap(parametersFilter, func(pf T, _ int) (string, bool) {
		return pf.GetName() + ":" + pf.GetValue(), slices.Contains(c.supportedParams, pf.GetName())
	})
	if len(paramKeys) > 0 { // include params along with the wildcard
		paramKeys = append(paramKeys, wildcard)
	}
	return partitionKeys, workspaceKeys, stateKeys, customValKeys, paramKeys
}

// String returns a string representation of the cache's tree contents.
func (c *NoResultsCache[T]) String() string {
	if c == nil {
		return "nil"
	}
	c.cacheTreeMu.RLock()
	defer c.cacheTreeMu.RUnlock()
	return fmt.Sprintf("%+v", c.cacheTree)
}

type (
	datasetKey   = string
	partitionKey = string
	workspaceKey = string
	customValKey = string
	stateKey     = string
	paramKey     = string
	cacheTree    map[datasetKey]map[partitionKey]map[workspaceKey]map[customValKey]map[stateKey]map[paramKey]cacheEntry
)

type cacheEntry struct {
	noJobs bool
	tokens []string
	t      time.Time
}

// AddToken adds a token to the cache entry and removes the oldest one if there are more than 10.
func (ce *cacheEntry) AddToken(token string) {
	ce.tokens = append(ce.tokens, token)
	if len(ce.tokens) > 10 {
		ce.tokens = lo.Slice(ce.tokens, len(ce.tokens)-10, len(ce.tokens))
	}
}

// SetNoJobs sets the noJobs flag to true if the provided token is found in the cache entry.
func (ce *cacheEntry) SetNoJobs(token string) bool {
	for i := len(ce.tokens) - 1; i >= 0; i-- {
		if ce.tokens[i] == token {
			ce.noJobs = true
			ce.t = time.Now()
			ce.tokens = slices.Delete(ce.tokens, i, i+1)
			return true
		}
	}
	return false
}
