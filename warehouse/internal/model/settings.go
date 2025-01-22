package model

type DestinationConfigSetting interface {
	String() string
	protected()
}

type destConfSetting string

func (destConfSetting) protected()       {}
func (s destConfSetting) String() string { return string(s) }

var (
	PreferAppendSetting              DestinationConfigSetting = destConfSetting("preferAppend")
	UseRudderStorageSetting          DestinationConfigSetting = destConfSetting("useRudderStorage")
	SecureSetting                    DestinationConfigSetting = destConfSetting("secure")
	SkipVerifySetting                DestinationConfigSetting = destConfSetting("skipVerify")
	EnableExternalLocationSetting    DestinationConfigSetting = destConfSetting("enableExternalLocation")
	UseSTSTokensSetting              DestinationConfigSetting = destConfSetting("useSTSTokens")
	UseGlueSetting                   DestinationConfigSetting = destConfSetting("useGlue")
	HostSetting                      DestinationConfigSetting = destConfSetting("host")
	DatabaseSetting                  DestinationConfigSetting = destConfSetting("database")
	UserSetting                      DestinationConfigSetting = destConfSetting("user")
	PasswordSetting                  DestinationConfigSetting = destConfSetting("password")
	PortSetting                      DestinationConfigSetting = destConfSetting("port")
	SSLModeSetting                   DestinationConfigSetting = destConfSetting("sslMode")
	ProjectSetting                   DestinationConfigSetting = destConfSetting("project")
	CredentialsSetting               DestinationConfigSetting = destConfSetting("credentials")
	LocationSetting                  DestinationConfigSetting = destConfSetting("location")
	CACertificateSetting             DestinationConfigSetting = destConfSetting("caCertificate")
	ClusterSetting                   DestinationConfigSetting = destConfSetting("cluster")
	AWSAccessKeySetting              DestinationConfigSetting = destConfSetting("accessKey")
	AWSAccessSecretSetting           DestinationConfigSetting = destConfSetting("accessKeyID")
	AWSBucketNameSetting             DestinationConfigSetting = destConfSetting("bucketName")
	AWSPrefixSetting                 DestinationConfigSetting = destConfSetting("prefix")
	MinioAccessKeyIDSetting          DestinationConfigSetting = destConfSetting("accessKeyID")
	MinioSecretAccessKeySetting      DestinationConfigSetting = destConfSetting("secretAccessKey")
	TimeWindowLayoutSetting          DestinationConfigSetting = destConfSetting("timeWindowLayout")
	PathSetting                      DestinationConfigSetting = destConfSetting("path")
	TokenSetting                     DestinationConfigSetting = destConfSetting("token")
	CatalogSetting                   DestinationConfigSetting = destConfSetting("catalog")
	ExternalLocationSetting          DestinationConfigSetting = destConfSetting("externalLocation")
	UseIAMForAuthSetting             DestinationConfigSetting = destConfSetting("useIAMForAuth")
	IAMRoleARNForAuthSetting         DestinationConfigSetting = destConfSetting("iamRoleARNForAuth")
	ClusterIDSetting                 DestinationConfigSetting = destConfSetting("clusterId")
	UseServerlessSetting             DestinationConfigSetting = destConfSetting("useServerless")
	WorkgroupNameSetting             DestinationConfigSetting = destConfSetting("workgroupName")
	ClusterRegionSetting             DestinationConfigSetting = destConfSetting("clusterRegion")
	StorageIntegrationSetting        DestinationConfigSetting = destConfSetting("storageIntegration")
	AccountSetting                   DestinationConfigSetting = destConfSetting("account")
	WarehouseSetting                 DestinationConfigSetting = destConfSetting("warehouse")
	RoleSetting                      DestinationConfigSetting = destConfSetting("role")
	UseKeyPairAuthSetting            DestinationConfigSetting = destConfSetting("useKeyPairAuth")
	PrivateKeySetting                DestinationConfigSetting = destConfSetting("privateKey")
	PrivateKeyPassphraseSetting      DestinationConfigSetting = destConfSetting("privateKeyPassphrase")
	TableSuffixSetting               DestinationConfigSetting = destConfSetting("tableSuffix")
	SyncFrequencySetting             DestinationConfigSetting = destConfSetting("syncFrequency")
	SyncStartAtSetting               DestinationConfigSetting = destConfSetting("syncStartAt")
	ExcludeWindowSetting             DestinationConfigSetting = destConfSetting("excludeWindow")
	PartitionColumnSetting           DestinationConfigSetting = destConfSetting("partitionColumn")
	PartitionTypeSetting             DestinationConfigSetting = destConfSetting("partitionType")
	CleanupObjectStorageFilesSetting DestinationConfigSetting = destConfSetting("cleanupObjectStorageFiles")
)
