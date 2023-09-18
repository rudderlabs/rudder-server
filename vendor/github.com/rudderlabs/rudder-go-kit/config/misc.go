package config

import (
	"os"
)

// GetWorkspaceToken returns the workspace token provided in the environment variables
// Env variable CONFIG_BACKEND_TOKEN is deprecating soon
// WORKSPACE_TOKEN is newly introduced. This will override CONFIG_BACKEND_TOKEN
func GetWorkspaceToken() string {
	token := GetString("WORKSPACE_TOKEN", "")
	if token != "" && token != "<your_token_here>" {
		return token
	}
	return GetString("CONFIG_BACKEND_TOKEN", "")
}

// GetNamespaceIdentifier returns value stored in KUBE_NAMESPACE env var or "none" if empty
func GetNamespaceIdentifier() string {
	k8sNamespace := GetKubeNamespace()
	if k8sNamespace != "" {
		return k8sNamespace
	}
	return "none"
}

// GetKubeNamespace returns value stored in KUBE_NAMESPACE env var
func GetKubeNamespace() string {
	return os.Getenv("KUBE_NAMESPACE")
}

func GetReleaseName() string {
	return os.Getenv("RELEASE_NAME")
}
