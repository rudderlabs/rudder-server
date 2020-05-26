package app

import (
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/types"
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

// WebhookFeature handles webhook event requests
type WebhookFeature interface {
	Setup(types.GatewayWebhookI) types.WebHookI
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
