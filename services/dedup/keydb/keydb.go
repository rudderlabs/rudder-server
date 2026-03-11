package keydb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/keydb/client"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/services/dedup/types"
)

type keyDB struct {
	client *client.Client
	window config.ValueLoader[time.Duration]
	logger logger.Logger

	stats struct {
		getTimer stats.Timer
		setTimer stats.Timer
	}
}

func NewKeyDB(conf *config.Config, stat stats.Stats, log logger.Logger) (types.DB, error) {
	dedupWindow := conf.GetReloadableDurationVar(3600, time.Second, "KeyDB.Dedup.dedupWindow", "Dedup.dedupWindow", "Dedup.dedupWindowInS")
	nodeAddresses := conf.GetStringVar("", "KeyDB.Dedup.Addresses")
	if len(nodeAddresses) == 0 {
		return nil, fmt.Errorf("keydb dedup: no node addresses provided")
	}

	clientConfig := client.Config{
		Addresses:          strings.Split(nodeAddresses, ","),
		TotalHashRanges:    conf.GetInt64Var(client.DefaultTotalHashRanges, 1, "KeyDB.Dedup.TotalHashRanges"),
		ConnectionPoolSize: conf.GetIntVar(client.DefaultConnectionPoolSize, 1, "KeyDB.Dedup.ConnectionPoolSize"),
		RetryPolicy: client.RetryPolicy{
			Disabled:        conf.GetBoolVar(false, "KeyDB.Dedup.RetryPolicy.Disabled"),
			InitialInterval: conf.GetDurationVar(100, time.Millisecond, "KeyDB.Dedup.RetryPolicy.InitialInterval"),
			Multiplier:      conf.GetFloat64Var(1.5, "KeyDB.Dedup.RetryPolicy.Multiplier"),
			MaxInterval:     conf.GetDurationVar(30, time.Second, "KeyDB.Dedup.RetryPolicy.MaxInterval"),
			// No MaxElapsedTime, the client will retry forever.
			// To detect issues monitor the client metrics:
			// https://github.com/rudderlabs/keydb/blob/v0.4.2-alpha/client/client.go#L160
		},
		GrpcConfig: client.GrpcConfig{
			// After a duration of this time if the client doesn't see any activity it
			// pings the server to see if the transport is still alive.
			KeepAliveTime: conf.GetDurationVar(10, time.Second, "KeyDB.Dedup.GrpcConfig.KeepAliveTime"),
			// After having pinged for keepalive check, the client waits for a duration
			// of Timeout and if no activity is seen even after that the connection is
			// closed.
			KeepAliveTimeout: conf.GetDurationVar(2, time.Second, "KeyDB.Dedup.GrpcConfig.KeepAliveTimeout"),
			// If false, client sends keepalive pings even with no active RPCs. If true,
			// when there are no active RPCs, KeepAliveTime and KeepAliveTimeout will be ignored and no
			// keepalive pings will be sent.
			DisableKeepAlivePermitWithoutStream: conf.GetBoolVar(false, "KeyDB.Dedup.GrpcConfig.DisableKeepAlivePermitWithoutStream"),
			// BackoffBaseDelay is the amount of time to backoff after the first failure.
			BackoffBaseDelay: conf.GetDurationVar(1, time.Second, "KeyDB.Dedup.GrpcConfig.BackoffBaseDelay"),
			// BackoffMultiplier is the factor with which to multiply backoffs after a
			// failed retry. Should ideally be greater than 1.
			BackoffMultiplier: conf.GetFloat64Var(1.6, "KeyDB.Dedup.GrpcConfig.BackoffMultiplier"),
			// BackoffJitter is the factor with which backoffs are randomized.
			BackoffJitter: conf.GetFloat64Var(0.2, "KeyDB.Dedup.GrpcConfig.BackoffJitter"),
			// BackoffMaxDelay is the upper bound of backoff delay.
			BackoffMaxDelay: conf.GetDurationVar(2, time.Minute, "KeyDB.Dedup.GrpcConfig.BackoffMaxDelay"),
			// MinConnectTimeout is the minimum amount of time we are willing to give a
			// connection to complete.
			MinConnectTimeout: conf.GetDurationVar(20, time.Second, "KeyDB.Dedup.GrpcConfig.MinConnectTimeout"),
		},
	}

	c, err := client.NewClient(clientConfig, log.Child("keydb"), client.WithStats(stat))
	if err != nil {
		return nil, err
	}

	db := &keyDB{
		client: c,
		window: dedupWindow,
		logger: log,
	}
	db.stats.getTimer = stat.NewTaggedStat("dedup_get_duration_seconds", stats.TimerType, stats.Tags{"mode": "keydb"})
	db.stats.setTimer = stat.NewTaggedStat("dedup_set_duration_seconds", stats.TimerType, stats.Tags{"mode": "keydb"})

	return db, nil
}

func (d *keyDB) Get(keys []string) (map[string]bool, error) {
	defer d.stats.getTimer.RecordDuration()()
	results := make(map[string]bool, len(keys))
	exist, err := d.client.Get(context.TODO(), keys)
	if err != nil {
		return nil, err
	}
	for i, key := range keys {
		if exist[i] {
			results[key] = true
		}
	}
	return results, err
}

func (d *keyDB) Set(keys []string) error {
	defer d.stats.setTimer.RecordDuration()()
	return d.client.Put(context.TODO(), keys, d.window.Load())
}

func (d *keyDB) Close() {
	if d.client != nil {
		_ = d.client.Close()
	}
}
