package maxcompute

import (
	"context"
	"errors"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	osscredentials "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aliyun/credentials-go/credentials"

	"github.com/rudderlabs/rudder-go-kit/config"
)

// ossObjectSize reads the actual OSS Content-Length without downloading data.
// The credential source matches the converted filemanager configuration.
func ossObjectSize(ctx context.Context, conf map[string]any, key string) (int64, error) {
	bucket, _ := conf["bucketName"].(string)
	region, _ := conf["region"].(string)
	var credential credentials.Credential
	if role, _ := conf["roleBasedAuth"].(bool); role {
		id := config.Default.GetStringVar("", "FileManager.ALIBABA_OSS.callerAccessKeyID")
		secret := config.Default.GetStringVar("", "FileManager.ALIBABA_OSS.callerAccessKeySecret")
		arn, _ := conf["iamRoleARN"].(string)
		external, _ := conf["externalID"].(string)
		if id == "" || secret == "" || arn == "" || external == "" {
			return 0, errors.New("OSS credentials unavailable")
		}
		cfg := new(credentials.Config).SetType("ram_role_arn").SetAccessKeyId(id).
			SetAccessKeySecret(secret).SetRoleArn(arn).SetRoleSessionName("rudder-warehouse")
		cfg.ExternalId = &external
		var err error
		credential, err = credentials.NewCredential(cfg)
		if err != nil {
			return 0, errors.New("OSS credentials unavailable")
		}
	} else {
		id, _ := conf["accessKeyID"].(string)
		secret, _ := conf["accessKey"].(string)
		if id == "" || secret == "" {
			return 0, errors.New("OSS credentials unavailable")
		}
		var err error
		credential, err = credentials.NewCredential(new(credentials.Config).SetType("access_key").SetAccessKeyId(id).SetAccessKeySecret(secret))
		if err != nil {
			return 0, errors.New("OSS credentials unavailable")
		}
	}
	provider := osscredentials.CredentialsProviderFunc(func(ctx context.Context) (osscredentials.Credentials, error) {
		if err := ctx.Err(); err != nil {
			return osscredentials.Credentials{}, err
		}
		model, err := credential.GetCredential()
		if err != nil || model == nil || model.AccessKeyId == nil || model.AccessKeySecret == nil ||
			*model.AccessKeyId == "" || *model.AccessKeySecret == "" {
			return osscredentials.Credentials{}, errors.New("OSS credentials unavailable")
		}
		result := osscredentials.Credentials{AccessKeyID: *model.AccessKeyId, AccessKeySecret: *model.AccessKeySecret}
		if role, _ := conf["roleBasedAuth"].(bool); role && (model.SecurityToken == nil || *model.SecurityToken == "") {
			return osscredentials.Credentials{}, errors.New("OSS credentials unavailable")
		}
		if model.SecurityToken != nil {
			result.SecurityToken = *model.SecurityToken
		}
		return result, nil
	})
	cfg := oss.LoadDefaultConfig().WithRegion(region).WithCredentialsProvider(provider).WithLogLevel(oss.LogOff)
	if internal, _ := conf["useInternalEndpoint"].(bool); internal {
		cfg.WithUseInternalEndpoint(true)
	}
	if endpoint, _ := conf["endPoint"].(string); endpoint != "" {
		if endpoint != "https://oss-"+region+".aliyuncs.com" && endpoint != "https://oss-"+region+"-internal.aliyuncs.com" {
			return 0, errors.New("invalid MaxCompute OSS metadata endpoint")
		}
		cfg.WithEndpoint(endpoint)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := oss.NewClient(cfg).GetObjectMeta(ctx, &oss.GetObjectMetaRequest{Bucket: new(bucket), Key: new(key)})
	if err != nil || result == nil {
		return 0, errors.New("reading MaxCompute OSS metadata failed")
	}
	return result.ContentLength, nil
}
