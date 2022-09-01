package features

import (
	"context"

	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

type Sender interface {
	Send(ctx context.Context, registry *Registry) error
}

func NewFromEnv() (Sender, error) {

	t, err := deployment.GetFromEnv()
	if err != nil {
		return nil, err
	}

	switch t {
	case deployment.DedicatedType:





	case deployment.MultiTenantType:

		return 
	}

}
