package app

import (
	"net/http"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

// MigratorFeature handles migration of nodes during cluster's scale up/down.
type MigratorFeature interface {
	Setup(*jobsdb.HandleT, *jobsdb.HandleT, *jobsdb.HandleT, func(), func())
}

// MigratorFeatureSetup is a function that initializes a Migrator feature, based on application instance
type MigratorFeatureSetup func(Interface) MigratorFeature

var migratorFeatureSetup MigratorFeatureSetup

// RegisterMigratorFeature registers a Migration implementation
func RegisterMigratorFeature(f MigratorFeatureSetup) {
	migratorFeatureSetup = f
}

// WebhookHandler is the interface returned on setting up a WebhookFeature
type WebhookHandler interface {
	BatchHandler(http.ResponseWriter, *http.Request)
	Register(name string, writeKey string)
}

// WebhookFeature handles webhook event requests
type WebhookFeature interface {
	Setup(interface{}) WebhookHandler
}

// WebhookFeatureSetup is a function that initializes a Webhook feature, based on application instance
type WebhookFeatureSetup func(Interface) WebhookFeature

var webhookFeatureSetup WebhookFeatureSetup

// RegisterWebhookFeature registers a Webhook implementation
func RegisterWebhookFeature(f WebhookFeatureSetup) {
	webhookFeatureSetup = f
}

// Features contains optional implementations of Enterprise only features.
type Features struct {
	Migrator MigratorFeature
	Webhook  WebhookFeature
}
