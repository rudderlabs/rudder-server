package client

import (
	"errors"
	"sync"

	"github.com/aliyun/credentials-go/credentials"
)

// guardCredentials sanitizes role refresh results before SDK signing.
func guardCredentials(credential credentials.Credential) credentials.Credential {
	if credential == nil {
		return nil
	}
	if _, guarded := credential.(*odpsCredentialGuard); guarded {
		return credential
	}
	return &odpsCredentialGuard{credential: credential}
}

type odpsCredentialGuard struct {
	mu         sync.Mutex
	credential credentials.Credential
}

func odpsCredentialModel(model *credentials.CredentialModel, err error) (*credentials.CredentialModel, error) {
	if err != nil || model == nil || model.AccessKeyId == nil || *model.AccessKeyId == "" ||
		model.AccessKeySecret == nil || *model.AccessKeySecret == "" ||
		model.SecurityToken == nil || *model.SecurityToken == "" {
		return nil, errors.New("ODPS credentials unavailable")
	}
	id, secret := *model.AccessKeyId, *model.AccessKeySecret
	copy := &credentials.CredentialModel{AccessKeyId: &id, AccessKeySecret: &secret}
	if model.SecurityToken != nil {
		token := *model.SecurityToken
		copy.SecurityToken = &token
	}
	return copy, nil
}

func (c *odpsCredentialGuard) GetCredential() (*credentials.CredentialModel, error) {
	if c == nil || c.credential == nil {
		return nil, errors.New("ODPS credentials unavailable")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	model, err := c.credential.GetCredential()
	return odpsCredentialModel(model, err)
}

func (c *odpsCredentialGuard) GetType() *string {
	// The SDK signs using GetCredential, not GetType. Avoid invoking an
	// untrusted provider here, since it may expose its own state.
	t := "sts"
	return &t
}

func (c *odpsCredentialGuard) GetAccessKeyId() (*string, error) {
	model, err := c.GetCredential()
	if err != nil {
		return nil, err
	}
	return model.AccessKeyId, nil
}

func (c *odpsCredentialGuard) GetAccessKeySecret() (*string, error) {
	model, err := c.GetCredential()
	if err != nil {
		return nil, err
	}
	return model.AccessKeySecret, nil
}

func (c *odpsCredentialGuard) GetSecurityToken() (*string, error) {
	model, err := c.GetCredential()
	if err != nil {
		return nil, err
	}
	return model.SecurityToken, nil
}

func (c *odpsCredentialGuard) GetBearerToken() *string { return nil }
