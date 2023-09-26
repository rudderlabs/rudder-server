// Copyright 2015 Yahoo Inc.
// Licensed under the terms of the Apache version 2.0 license. See LICENSE file for terms.

package rdl

//
// Principal is the subject of authentication.
//
type Principal interface {
	GetDomain() string
	GetName() string
	GetYRN() string
	GetCredentials() string
	GetHTTPHeaderName() string
}

//
// An Authenticator takes some credentials and, if valid, returns a Principal representing them.
//
type Authenticator interface {
	Authenticate(nToken string) Principal //the method that authenticates
	HTTPHeader() string                   //the header the credential is stored in when using HTTP
}

//
// Authorizer is the interface for an object that can authorize access. Usually set up to ZMSAuthorizer.
//
type Authorizer interface {
	//returns a boolean indication if the principal can perform the action on the resource. The error return value
	//means that an error was encountered before this could be determined.
	Authorize(action string, resource string, principal Principal) (bool, error)
}
