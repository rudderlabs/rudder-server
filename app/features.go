package app

//go:generate mockgen -destination=../mocks/app/mock_features.go -package=mock_app github.com/rudderlabs/rudder-server/app SuppressUserFeature

import (
	"context"

	"github.com/rudderlabs/rudder-server/enterprise/trackedusers"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
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
	Setup(cxt context.Context, conf *config.Config, backendConfig backendconfig.BackendConfig) types.Reporting
}

// Features contains optional implementations of Enterprise only features.
type Features struct {
	SuppressUser SuppressUserFeature
	ConfigEnv    ConfigEnvFeature
	Reporting    ReportingFeature
	TrackedUsers TrackedUsersFeature
}

type TrackedUsersFeature interface {
	Setup(c *config.Config) (trackedusers.UsersReporter, error)
}
