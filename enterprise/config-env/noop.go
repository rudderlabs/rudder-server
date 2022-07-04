package configenv

type NOOP struct{}

func (*NOOP) ReplaceConfigWithEnvVariables(workspaceConfig []byte) (updatedConfig []byte) {
	return workspaceConfig
}
