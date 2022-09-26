package config

import (
	"os"
	"strings"
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

// GetNamespaceIdentifier
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

func GetInstanceID() string {
	instance := GetString("INSTANCE_ID", "")
	if instance == "" {
		return ""
	}
	instanceArr := strings.Split(instance, "-")
	return instanceArr[len(instanceArr)-1]
}

func GetReleaseName() string {
	return os.Getenv("RELEASE_NAME")
}
