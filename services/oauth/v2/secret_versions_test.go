package v2_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_backendconfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

func mockSecretVersions(ctrl *gomock.Controller) (*mock_backendconfig.MockBackendConfig, *v2.SecretVersions, chan pubsub.DataEvent) {
	bc := mock_backendconfig.NewMockBackendConfig(ctrl)
	ch := make(chan pubsub.DataEvent, 1)
	DeferCleanup(func() { close(ch) })
	bc.EXPECT().Subscribe(gomock.Any(), backendconfig.TopicBackendConfig).Return(pubsub.DataChannel(ch)).Times(1)
	versions := v2.SecretVersionsFor(bc)
	ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{
		"ws-1": {Accounts: map[string]backendconfig.Account{
			"acct-1": {ID: "acct-1", SecretVersion: 8},
		}},
	}}
	require.Eventually(GinkgoT(), func() bool {
		return versions.Get("acct-1") == 8
	}, time.Second, time.Millisecond)
	return bc, versions, ch
}

var _ = Describe("SecretVersions", func() {
	It("returns nil without a backend config", func() {
		Expect(v2.SecretVersionsFor(nil)).To(BeNil())
	})

	It("A7a: shares one mirror and subscription per backend config", func() {
		ctrl := gomock.NewController(GinkgoT())
		bc, versions, _ := mockSecretVersions(ctrl)
		Expect(versions.Get("acct-1")).To(Equal(8))
		Expect(versions.Get("other")).To(BeZero())
		Expect(v2.SecretVersionsFor(bc)).To(BeIdenticalTo(versions))
	})

	It("replaces the mirror across workspaces and removes missing accounts", func() {
		ctrl := gomock.NewController(GinkgoT())
		_, versions, ch := mockSecretVersions(ctrl)
		ch <- pubsub.DataEvent{Data: map[string]backendconfig.ConfigT{
			"ws-1": {Accounts: map[string]backendconfig.Account{
				"acct-2": {ID: "acct-2", SecretVersion: 9},
			}},
			"ws-2": {Accounts: map[string]backendconfig.Account{
				"acct-3": {ID: "acct-3", SecretVersion: 10},
			}},
		}}
		require.Eventually(GinkgoT(), func() bool {
			return versions.Get("acct-1") == 0 && versions.Get("acct-2") == 9 && versions.Get("acct-3") == 10
		}, time.Second, time.Millisecond)
	})
})
