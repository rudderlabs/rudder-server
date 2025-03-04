package bench

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb/bench/scenario"
)

type Bench interface {
	Run(ctx context.Context) error
}

func New(conf *config.Config, stat stats.Stats, log logger.Logger, db *sql.DB) (Bench, error) {
	scenarioName := conf.GetStringVar("simple", "JobsDB.Bench.scenario")
	switch scenarioName {
	case "simple":
		return scenario.NewSimple(conf, stat, log, db), nil
	case "two_stage":
		return scenario.NewTwoStage(conf, stat, log, db), nil
	default:
		return nil, fmt.Errorf("unknown jobsdb bench scenario name: %s", conf.GetStringVar("processor", "JobsDB.bench.scenario"))
	}
}
