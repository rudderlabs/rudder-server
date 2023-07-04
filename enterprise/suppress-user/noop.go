package suppression

import "github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"

type NOOP struct{}

func (*NOOP) GetSuppressedUser(_, _, _ string) *model.Metadata {
	return nil
}
