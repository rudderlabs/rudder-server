package destination

import (
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

func MINIOFromResource(id string, m *minio.Resource) backendconfig.DestinationT {
	destType := "MINIO"

	return backendconfig.DestinationT{
		ID:                 id,
		Name:               "S3 Destination" + rand.String(5),
		Enabled:            true,
		IsProcessorEnabled: true,
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:   rand.UniqueString(10),
			Name: destType,
		},
		Config: map[string]any{
			"bucketProvider":   "MINIO",
			"bucketName":       m.BucketName,
			"accessKeyID":      m.AccessKeyID,
			"secretAccessKey":  m.AccessKeySecret,
			"useSSL":           false,
			"endPoint":         m.Endpoint,
			"useRudderStorage": false,
		},
	}
}
