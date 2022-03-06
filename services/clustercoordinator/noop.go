package clustercoordinator

type NOOPManager struct {
	Config *NOOPConfig
}

func GetNOOPConfig() *NOOPConfig {
	return &NOOPConfig{}
}

type NOOPConfig struct {
}
