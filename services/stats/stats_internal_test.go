package stats

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/foxcpp/go-mockdns"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/metric"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

// Verifying that even though a Stats instance might initially
// fail to connect to the statsd server, it will retry in the background
// and eventually succeed.
//
// Note:
// Since we are using udp (connectionless protocol), even if nothing is listening at the target port,
// the statsd client creation will succeed, as long as the provided address can be resolved
// (e.g. if we are using an ip address such as 127.0.0.1).
// The only way for the statsd client creation to fail is to provide a non-existent domain in the address field.
// Next step, after we verify that client creation has failed, is to make the domain name resolvable so that
// statsd client creation can succeed when retried.
// To make the domain name resolvable we need to intercept go's dns resolution and use our own (mock) dns server.
func Test_statsd_server_initially_unavailable(t *testing.T) {
	port, err := testhelper.GetFreePort()
	require.NoError(t, err)

	localServerAddr := fmt.Sprintf("127.0.0.1:%d", port)

	// 1. Start a mock statsd server listening on localhost
	var lastMetric string
	statsdSrv := newStatsdServer(t, localServerAddr, func(s string) {
		lastMetric = s
	})
	defer statsdSrv.Close()

	//  2. Start a Stats instance using as server url a domain name that doesn't resolve to any address (yet)
	domainName := "my.example.org"
	serverAddr := net.JoinHostPort(domainName, strconv.Itoa(port))
	c := config.New()
	c.Set("STATSD_SERVER_URL", serverAddr)
	c.Set("INSTANCE_ID", "test")
	c.Set("RuntimeStats.enabled", false)
	l := logger.NewFactory(c)
	m := metric.NewManager()
	s := NewStats(c, l, m).(*statsdStats)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	defer s.Stop()

	//  3. Create a stats measurement and try to post some stats
	counter := s.NewTaggedStat("test", CountType, Tags{"tag1": "value1"})
	counter.Increment()

	// 4. No statsd client should have been created yet, thus we expect pendingClients to be non-empty
	require.Equal(t, 1, len(s.state.pendingClients))
	require.Equal(t, "", lastMetric, "statsd server shouldn't have received any metrics yet")

	// 5. Make statsd server's domain name resolvable to localhost
	srv, _ := mockdns.NewServer(map[string]mockdns.Zone{
		"my.example.org.": {
			A: []string{"127.0.0.1"},
		},
	}, false)
	defer srv.Close()
	srv.PatchNet(net.DefaultResolver)
	defer mockdns.UnpatchNet(net.DefaultResolver)

	// 6. Wait until the Stats instance is connected to it (by checking pendingClients)
	require.Eventually(t, func() bool {
		s.state.clientsLock.RLock()
		defer s.state.clientsLock.RUnlock()

		// there should be no pending clients
		if len(s.state.pendingClients) != 0 {
			return false
		}

		// all cached clients should have a non nil statsd client
		for _, c := range s.state.clients {
			if c.statsd == nil {
				return false
			}
		}

		// connEstablished should be true
		return s.state.connEstablished
	}, 2*time.Second, time.Millisecond)

	// 7. The previously created measurement should now be able to successfully send metrics to the statsd server
	counter.Increment()
	require.Eventually(t, func() bool {
		return lastMetric == "test,instanceName=test,tag1=value1:1|c"
	}, 2*time.Second, time.Millisecond)

	// 8. A new measurement should also be able to successfully send metrics to statsd server
	s.NewTaggedStat("test-2", CountType, Tags{"tag1": "value1"}).Increment()
	require.Eventually(t, func() bool {
		return lastMetric == "test-2,instanceName=test,tag1=value1:1|c"
	}, 2*time.Second, time.Millisecond)
}

type statsdServer struct {
	t      *testing.T
	closer io.Closer
	closed chan bool
}

func newStatsdServer(t *testing.T, addr string, f func(string)) *statsdServer {
	s := &statsdServer{t: t, closed: make(chan bool)}
	laddr, err := net.ResolveUDPAddr("udp", addr)
	require.NoError(t, err)
	conn, err := net.ListenUDP("udp", laddr)
	require.NoError(t, err)
	s.closer = conn
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				s.closed <- true
				return
			}
			s := string(buf[:n])
			lines := strings.Split(s, "\n")
			if n > 0 {
				for _, line := range lines {
					f(line)
				}
			}
		}
	}()

	return s
}

func (s *statsdServer) Close() {
	require.NoError(s.t, s.closer.Close())
	<-s.closed
}
