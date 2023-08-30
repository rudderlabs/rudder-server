package app

//go:generate mockgen -destination=../mocks/app/mock_features.go -package=mock_app github.com/rudderlabs/rudder-server/app SuppressUserFeature

import (
	"context"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/types"
)

// SuppressUserFeature handles webhook event requests
type SuppressUserFeature interface {
	Setup(ctx context.Context, backendConfig backendconfig.BackendConfig) (types.UserSuppression, error)
}

/*********************************
DestinationConfig Env Support
*********************************/

// ConfigEnvFeature handles override of config from ENV variables.
type ConfigEnvFeature interface {
	Setup() types.ConfigEnvI
}

/*********************************
Reporting Feature
*********************************/

// ReportingFeature handles reporting statuses / errors to reporting service
type ReportingFeature interface {
	Setup(backendConfig backendconfig.BackendConfig) types.Reporting
	GetReportingInstance() types.Reporting
}

/*********************************
Replay Feature
*********************************/

// ReplayFeature handles inserting of failed jobs into respective gw/rt jobsdb
type ReplayFeature interface {
	Setup(ctx context.Context, replayDB, gwDB, routerDB, batchRouterDB *jobsdb.HandleT)
}

// ReplayFeatureSetup is a function that initializes a Replay feature
type ReplayFeatureSetup func(App) ReplayFeature

// Features contains optional implementations of Enterprise only features.
type Features struct {
	SuppressUser SuppressUserFeature
	ConfigEnv    ConfigEnvFeature
	Reporting    ReportingFeature
	Replay       ReplayFeature
}
