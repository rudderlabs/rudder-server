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

func NewClients(conf *config.Config, log logger.Logger, statsFactory stats.Stats) *Clients {
	return &Clients{
		user:         user_transformer.New(conf, log, statsFactory),
		destination:  destination_transformer.New(conf, log, statsFactory),
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
