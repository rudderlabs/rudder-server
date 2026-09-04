package backendconfig

import (
	"context"
	"io"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

// shadowUploader is the one method of filemanager.FileManager the comparer uses.
type shadowUploader interface {
	UploadReader(ctx context.Context, objName string, rdr io.Reader) (filemanager.UploadedFile, error)
}

// newShadowSamplingUploader returns the interface rather than the concrete manager: the caller
// keys its degraded path on a nil uploader, which a typed nil pointer would defeat.
func newShadowSamplingUploader(conf *config.Config, log logger.Logger) (shadowUploader, error) {
	s3Config := map[string]any{
		"bucketName":       conf.GetStringVar("backend-config-shadow-sampling", "BackendConfigShadow.Bucket"),
		"endpoint":         conf.GetStringVar("", "BackendConfigShadow.Endpoint"),
		"accessKeyID":      conf.GetStringVar("", "BackendConfigShadow.AccessKeyId", "AWS_ACCESS_KEY_ID"),
		"accessKey":        conf.GetStringVar("", "BackendConfigShadow.AccessKey", "AWS_SECRET_ACCESS_KEY"),
		"s3ForcePathStyle": conf.GetBoolVar(false, "BackendConfigShadow.S3ForcePathStyle"),
		"disableSSL":       conf.GetBoolVar(false, "BackendConfigShadow.DisableSsl"),
		"enableSSE":        conf.GetBoolVar(false, "BackendConfigShadow.EnableSse", "AWS_ENABLE_SSE"),
		"useGlue":          conf.GetBoolVar(false, "BackendConfigShadow.UseGlue"),
		"region":           conf.GetStringVar("us-east-1", "BackendConfigShadow.Region", "AWS_DEFAULT_REGION"),
	}
	manager, err := filemanager.NewS3Manager(conf, s3Config,
		log.Withn(logger.NewStringField("component", "bcv2-shadow-uploader")),
		func() time.Duration {
			return conf.GetDurationVar(120, time.Second, "BackendConfigShadow.Timeout")
		})
	if err != nil {
		return nil, err
	}
	return manager, nil
}
