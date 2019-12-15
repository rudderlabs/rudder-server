package backendconfig

import (
	"reflect"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats"

	"github.com/rudderlabs/rudder-server/utils/logger"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils"
)

var (
	backendConfig                        BackendConfig
	hostedService                        bool
	hostedServiceSecret                  string
	configBackendURL, configBackendToken string
	pollInterval                         time.Duration
	curSourceJSON                        SourcesT
	initialized                          bool
)

var Eb *utils.EventBus

type DestinationDefinitionT struct {
	ID          string
	Name        string
	DisplayName string
}

type SourceDefinitionT struct {
	ID   string
	Name string
}

type DestinationT struct {
	ID                    string
	Name                  string
	DestinationDefinition DestinationDefinitionT
	Config                interface{}
	Enabled               bool
	Transformations       []TransformationT
}

type SourceT struct {
	ID               string
	Name             string
	SourceDefinition SourceDefinitionT
	Config           interface{}
	Enabled          bool
	Destinations     []DestinationT
	WriteKey         string
}

type SourcesT struct {
	Sources []SourceT `json:"sources"`
}

type TransformationT struct {
	ID          string
	Name        string
	Description string
	VersionID   string
}

type BackendConfig interface {
	SetUp()
	GetBackendConfig() (SourcesT, bool)
	GetWorkspaceIDForWriteKey(string) string
}

func loadConfig() {
	// Rudder as Hosted service. false by default
	hostedService = config.GetEnvAsBool("HOSTED_SERVICE", false)
	// Secret to be sent in basic auth for Hosted service. password by default
	hostedServiceSecret = config.GetEnv("HOSTED_SERVICE_SECRET", "password")

	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
	configBackendToken = config.GetEnv("CONFIG_BACKEND_TOKEN", "1P2tfQQKarhlsG6S3JGLdXptyZY")
	pollInterval = config.GetDuration("BackendConfig.pollIntervalInS", 5) * time.Second
}

func GetConfigBackendToken() string {
	return configBackendToken
}

func init() {
	config.Initialize()
	loadConfig()
}

func pollConfigUpdate() {
	statConfigBackendError := stats.NewStat("config_backend.errors", stats.CountType)
	for {
		sourceJSON, ok := backendConfig.GetBackendConfig()
		if !ok {
			statConfigBackendError.Increment()
		}
		if ok && !reflect.DeepEqual(curSourceJSON, sourceJSON) {
			curSourceJSON = sourceJSON
			initialized = true
			Eb.Publish("backendconfig", sourceJSON)
		}
		time.Sleep(time.Duration(pollInterval))
	}
}

func GetConfig() SourcesT {
	return curSourceJSON
}

func GetWorkspaceIDForWriteKey(writeKey string) string {
	return backendConfig.GetWorkspaceIDForWriteKey(writeKey)
}

func Subscribe(channel chan utils.DataEvent) {
	Eb.Subscribe("backendconfig", channel)
	Eb.PublishToChannel(channel, "backendconfig", curSourceJSON)
}

func WaitForConfig() {
	for {
		if initialized {
			break
		}
		logger.Info("Waiting for initializing backend config")
		time.Sleep(time.Duration(pollInterval))

	}
}

// Setup backend config
func Setup() {
	if hostedService {
		backendConfig = new(MultiWorkspaceConfig)
	} else {
		backendConfig = new(WorkspaceConfig)
	}

	backendConfig.SetUp()
	Eb = new(utils.EventBus)
	go pollConfigUpdate()
}
