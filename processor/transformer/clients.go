//go:generate mockgen -destination=../../mocks/processor/transformer/mock_transformer_clients.go -package=mocks_transformer_clients github.com/rudderlabs/rudder-server/processor/transformer TransformerClients

package transformer

import (
	"context"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/trackingplan_validation"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/user_transformer"
	"github.com/rudderlabs/rudder-server/processor/types"
	transformerfs "github.com/rudderlabs/rudder-server/services/transformer"
)

type DestinationClient interface {
	Transform(ctx context.Context, events []types.TransformerEvent) types.Response
}

type UserClient interface {
	Transform(ctx context.Context, events []types.TransformerEvent) types.Response
}

type TrackingPlanClient interface {
	Validate(ctx context.Context, events []types.TransformerEvent) types.Response
}

type Clients struct {
	user         UserClient
	destination  DestinationClient
	trackingplan TrackingPlanClient
}

type TransformerClients interface {
	User() UserClient
	Destination() DestinationClient
	TrackingPlan() TrackingPlanClient
}

// WithFeatureService is used to set the feature service for the destination transformer.
func WithFeatureService(featuresService transformerfs.FeaturesService) func(*opts) {
	return func(o *opts) {
		o.destinationOpts = append(o.destinationOpts, destination_transformer.WithFeatureService(featuresService))
	}
}

// NewClients creates a new instance of TransformerClients.
func NewClients(conf *config.Config, log logger.Logger, statsFactory stats.Stats, options ...func(*opts)) TransformerClients {
	var opts opts
	for _, option := range options {
		option(&opts)
	}
	return &Clients{
		user:         user_transformer.New(conf, log, statsFactory),
		destination:  destination_transformer.New(conf, log, statsFactory, opts.destinationOpts...),
		trackingplan: trackingplan_validation.New(conf, log, statsFactory),
	}
}

func (c *Clients) User() UserClient {
	return c.user
}

func (c *Clients) Destination() DestinationClient {
	return c.destination
}

func (c *Clients) TrackingPlan() TrackingPlanClient {
	return c.trackingplan
}

type opts struct {
	destinationOpts []destination_transformer.Opt
}
