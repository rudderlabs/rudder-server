package model

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

type DestinationConfigSetting interface {
	String() string
	protected()
}

type destConfSetting string

func (destConfSetting) protected()       {}
func (s destConfSetting) String() string { return string(s) }

var (
	PreferAppendSetting           DestinationConfigSetting = destConfSetting("preferAppend")
	UseRudderStorageSetting       DestinationConfigSetting = destConfSetting("useRudderStorage")
	SecureSetting                 DestinationConfigSetting = destConfSetting("secure")
	SkipVerifySetting             DestinationConfigSetting = destConfSetting("skipVerify")
	EnableExternalLocationSetting DestinationConfigSetting = destConfSetting("enableExternalLocation")
	UseSTSTokensSetting           DestinationConfigSetting = destConfSetting("useSTSTokens")
	UseGlueSetting                DestinationConfigSetting = destConfSetting("useGlue")
	HostSetting                   DestinationConfigSetting = destConfSetting("host")
	DatabaseSetting               DestinationConfigSetting = destConfSetting("database")
	UserSetting                   DestinationConfigSetting = destConfSetting("user")
	PasswordSetting               DestinationConfigSetting = destConfSetting("password")
	PortSetting                   DestinationConfigSetting = destConfSetting("port")
	SSLModeSetting                DestinationConfigSetting = destConfSetting("sslMode")
	ProjectSetting                DestinationConfigSetting = destConfSetting("project")
	CredentialsSetting            DestinationConfigSetting = destConfSetting("credentials")
	LocationSetting               DestinationConfigSetting = destConfSetting("location")
	CACertificateSetting          DestinationConfigSetting = destConfSetting("caCertificate")
	ClusterSetting                DestinationConfigSetting = destConfSetting("cluster")
	AWSAccessKeySetting           DestinationConfigSetting = destConfSetting("accessKey")
	AWSAccessSecretSetting        DestinationConfigSetting = destConfSetting("accessKeyID")
	AWSBucketNameSetting          DestinationConfigSetting = destConfSetting("bucketName")
	AWSPrefixSetting              DestinationConfigSetting = destConfSetting("prefix")
	MinioAccessKeyIDSetting       DestinationConfigSetting = destConfSetting("accessKeyID")
	MinioSecretAccessKeySetting   DestinationConfigSetting = destConfSetting("secretAccessKey")
	TimeWindowLayoutSetting       DestinationConfigSetting = destConfSetting("timeWindowLayout")
	PathSetting                   DestinationConfigSetting = destConfSetting("path")
	TokenSetting                  DestinationConfigSetting = destConfSetting("token")
	CatalogSetting                DestinationConfigSetting = destConfSetting("catalog")
	ExternalLocationSetting       DestinationConfigSetting = destConfSetting("externalLocation")
	UseIAMForAuthSetting          DestinationConfigSetting = destConfSetting("useIAMForAuth")
	IAMRoleARNForAuthSetting      DestinationConfigSetting = destConfSetting("iamRoleARNForAuth")
	ClusterIDSetting              DestinationConfigSetting = destConfSetting("clusterId")
	ClusterRegionSetting          DestinationConfigSetting = destConfSetting("clusterRegion")
	StorageIntegrationSetting     DestinationConfigSetting = destConfSetting("storageIntegration")
	AccountSetting                DestinationConfigSetting = destConfSetting("account")
	WarehouseSetting              DestinationConfigSetting = destConfSetting("warehouse")
	RoleSetting                   DestinationConfigSetting = destConfSetting("role")
	UseKeyPairAuthSetting         DestinationConfigSetting = destConfSetting("useKeyPairAuth")
	PrivateKeySetting             DestinationConfigSetting = destConfSetting("privateKey")
	PrivateKeyPassphraseSetting   DestinationConfigSetting = destConfSetting("privateKeyPassphrase")
	TableSuffixSetting            DestinationConfigSetting = destConfSetting("tableSuffix")
	SyncFrequencySetting          DestinationConfigSetting = destConfSetting("syncFrequency")
	SyncStartAtSetting            DestinationConfigSetting = destConfSetting("syncStartAt")
	ExcludeWindowSetting          DestinationConfigSetting = destConfSetting("excludeWindow")
)

type Warehouse struct {
	WorkspaceID string
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
	Namespace   string
	Type        string
	Identifier  string
}

func (w *Warehouse) GetBoolDestinationConfig(key DestinationConfigSetting) bool {
	destConfig := w.Destination.Config
	if destConfig[key.String()] != nil {
		if val, ok := destConfig[key.String()].(bool); ok {
			return val
		}
	}
	return false
}

func (w *Warehouse) GetStringDestinationConfig(conf *config.Config, key DestinationConfigSetting) string {
	configKey := fmt.Sprintf("Warehouse.pipeline.%s.%s.%s", w.Source.ID, w.Destination.ID, key)
	if conf.IsSet(configKey) {
		return conf.GetString(configKey, "")
	}

	destConfig := w.Destination.Config
	if destConfig[key.String()] != nil {
		if val, ok := destConfig[key.String()].(string); ok {
			return val
		}
	}
	return ""
}

func (w *Warehouse) GetMapDestinationConfig(key DestinationConfigSetting) map[string]interface{} {
	destConfig := w.Destination.Config
	if destConfig[key.String()] != nil {
		if val, ok := destConfig[key.String()].(map[string]interface{}); ok {
			return val
		}
	}
	return map[string]interface{}{}
}

func (w *Warehouse) GetPreferAppendSetting() bool {
	destConfig := w.Destination.Config
	value, ok := destConfig[PreferAppendSetting.String()].(bool)
	if !ok {
		// when the value is not defined it is important we choose the right default
		// in order to maintain backward compatibility for existing destinations
		return w.Type == "BQ" // defaulting to true for BQ, false for other destination types
	}
	return value
}
