package gateway_test

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transformer"
)

func TestInternalBatch(t *testing.T) {
	conf := config.New()

	var err error
	serverPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	conf.Set("Gateway.webPort", strconv.Itoa(serverPort))

	internalBatchEndpoint := fmt.Sprintf("http://localhost:%d/internal/v1/batch", serverPort)

	gw := &gateway.Handle{}
	err = gw.Setup(
		context.Background(),
		conf, logger.NOP, stats.NOP,
		nil, c.mockBackendConfig, c.mockJobsDB, c.mockErrJobsDB,
		nil, c.mockVersionHandler,
		rsources.NewNoOpService(), transformer.NewNoOpService(),
		srcDebugger, nil,
	)

	ctx, cancel := context.WithCancel(context.Background())
	wait := make(chan struct{})
	go func() {
		err = gateway.StartWebHandler(ctx)
		Expect(err).To(BeNil())
		close(wait)
	}()
	defer func() {
		cancel()
		<-wait
	}()

	Eventually(func() bool {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/version", serverPort))
		if err != nil {
			return false
		}
		return resp.StatusCode == http.StatusOK
	}, time.Second*10, time.Second).Should(BeTrue())
}
