package config

import (
	"os"
	"strconv"
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
	instanceArr := strings.Split(instance, "-")
	len := len(instanceArr)
	// This handles 2 kinds of server instances
	// a. Processor OR Gateway running in non HA mod where the instance name ends with the index
	// b. Gateway running in HA mode, where the instance name is of the form *-gw-ha-<index>-<statefulset-id>-<pod-id>
	potentialServerIndexIndicees := []int{len - 1, len - 3}
	for _, i := range potentialServerIndexIndicees {
		if i < 0 {
			continue
		}
		serverIndex := instanceArr[i]
		_, err := strconv.Atoi(serverIndex)
		if err == nil {
			return serverIndex
		}
	}
	return ""
}

func GetReleaseName() string {
	return os.Getenv("RELEASE_NAME")
}
