package customdestinationmanager

import (
	"fmt"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/streammanager/kafka"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/assert"
)

func TestCircuitBreaker(t *testing.T) {
	const (
		normalError  = "could not ping: could not dial tcp/unknown.example.com:9999: failed to dial: failed to open connection to unknown.example.com:9999: dial tcp: lookup unknown.example.com: no such host"
		breakerError = "circuit breaker is open"
	)

	// init various packages
	config.Load()
	Init()
	logger.Init()
	stats.Setup()
	kafka.Init()

	// don't let manager subscribe to backend-config
	skipBackendConfigSubscriber = true

	// set a custom circuit breaker timeout (default is 60 seconds)
	breakerTimeout := 1 * time.Second

	manager := New("KAFKA", Opts{Timeout: 1 * time.Microsecond}).(*CustomManagerT)
	manager.breakerTimeout = breakerTimeout

	dest := getDestConfig()
	// during the first 6 failed attempts the circuit is closed
	count := 1
	newDestination(t, manager, dest, count, normalError)
	count++
	for ; count <= 6; count++ {
		newClientAttempt(t, manager, dest.ID, count, normalError)
	}
	// after the 6th failed attempt the circuit opens
	newClientAttempt(t, manager, dest.ID, count, breakerError)
	count++
	<-time.After(breakerTimeout)
	// after the circuit breaker's timeout passes the circuit becomes half-open
	newClientAttempt(t, manager, dest.ID, count, normalError)
	count++
	// after another failure the circuit opens again
	newClientAttempt(t, manager, dest.ID, count, breakerError)
	count++

	// sending the same destination again should not try to create a client
	assert.Nil(t, manager.onNewDestination(dest))
	// and shouldn't reset the circuit breaker either
	newClientAttempt(t, manager, dest.ID, count, breakerError)

	// sending a modified destination should reset the circuit breaker
	dest = getDestConfig()
	dest.Config["modified"] = true
	count = 1
	newDestination(t, manager, dest, count, normalError)
	count++
	for ; count <= 6; count++ {
		newClientAttempt(t, manager, dest.ID, count, normalError)
	}
	// after the 6th attempt the new circuit opens
	newClientAttempt(t, manager, dest.ID, count, breakerError)
}

func newDestination(t *testing.T, manager *CustomManagerT, dest backendconfig.DestinationT, attempt int, errorString string) { // skipcq: CRT-P0003
	err := manager.onNewDestination(dest)
	assert.EqualError(t, err, errorString, fmt.Sprintf("wrong error for attempt no %d", attempt))
}
func newClientAttempt(t *testing.T, manager *CustomManagerT, destId string, attempt int, errorString string) {
	err := manager.newClient(destId)
	assert.EqualError(t, err, errorString, fmt.Sprintf("wrong error for attempt no %d", attempt))
}

func getDestConfig() backendconfig.DestinationT {
	return backendconfig.DestinationT{
		ID:   "test",
		Name: "test",
		Config: map[string]interface{}{
			"hostName":      "unknown.example.com",
			"port":          "9999",
			"topic":         "topic",
			"sslEnabled":    true,
			"useSASL":       true,
			"caCertificate": "",
			"saslType":      "plain",
			"username":      "username",
			"password":      "password",
		},
		Enabled: true,
	}
}
