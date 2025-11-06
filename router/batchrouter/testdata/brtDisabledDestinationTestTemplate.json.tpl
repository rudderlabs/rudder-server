{
    "{{$.workspace}}" : {
        "enableMetrics": false,
        "workspaceId": "{{$.workspace}}",
        "sources": [
            {
                "config": {},
                "liveEventsConfig": {},
                "id": "{{$.workspace}}",
                "workspaceId": "{{$.workspace}}",
                "destinations": [
                    {
                        "id": "{{$.workspace}}-MINIO",
                        "name": "batchrt-isolation-minio",
                        "secretConfig": {},
                        "config": {
                            "endPoint": "{{$.minioEndpoint}}",
                            "bucketName": "{{$.minioBucket}}",
                            "accessKeyID": "{{$.minioAccessKeyID}}",
                            "secretAccessKey": "{{$.minioSecretAccessKey}}",
                            "prefix": "",
                            "useSSL": false,
                            "oneTrustCookieCategories": []
                        },
                        "liveEventsConfig": {},
                        "workspaceId": "{{$.workspace}}",
                        "destinationDefinition": {
                            "config": {
                                "destConfig": {
                                    "defaultConfig": [
                                        "endPoint",
                                        "accessKeyID",
                                        "secretAccessKey",
                                        "bucketName",
                                        "prefix",
                                        "useSSL",
                                        "oneTrustCookieCategories"
                                    ]
                                },
                                "secretKeys": [
                                    "accessKeyID",
                                    "secretAccessKey"
                                ],
                                "excludeKeys": [],
                                "includeKeys": [
                                    "oneTrustCookieCategories"
                                ],
                                "transformAt": "none",
                                "transformAtV1": "none",
                                "supportedSourceTypes": [
                                    "android",
                                    "ios",
                                    "web",
                                    "unity",
                                    "amp",
                                    "cloud",
                                    "warehouse",
                                    "reactnative",
                                    "flutter",
                                    "cordova"
                                ],
                                "saveDestinationResponse": true
                            },
                            "configSchema": {},
                            "responseRules": null,
                            "options": null,
                            "uiConfig": [
                                {
                                    "title": "1. Connection Settings",
                                    "fields": [
                                        {
                                            "type": "textInput",
                                            "label": "MinIO Endpoint",
                                            "regex": "(?!.*\\.ngrok\\.io)^(.{0,100})$",
                                            "value": "endPoint",
                                            "required": true,
                                            "placeholder": "e.g: play.min.io:9000",
                                            "regexErrorMessage": "Invalid MinIO Endpoint"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "MinIO Access Key ID",
                                            "regex": "^(.{0,100})$",
                                            "value": "accessKeyID",
                                            "secret": true,
                                            "required": true,
                                            "placeholder": "e.g: Q3AM3UQ867SPQQA43P2F",
                                            "regexErrorMessage": "Invalid MinIO Access Key ID"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "MinIO Secret Access Key",
                                            "regex": "^(.{0,100})$",
                                            "value": "secretAccessKey",
                                            "secret": true,
                                            "required": true,
                                            "placeholder": "e.g: zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
                                            "regexErrorMessage": "Invalid MinIO Secret Access Key"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "MINIO Bucket Name",
                                            "regex": "^(.{0,100})$",
                                            "value": "bucketName",
                                            "required": true,
                                            "placeholder": "e.g: minio-event-logs",
                                            "regexErrorMessage": "Invalid MINIO Bucket Name"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Prefix",
                                            "regex": "^(.{0,100})$",
                                            "value": "prefix",
                                            "required": false,
                                            "placeholder": "e.g: rudder",
                                            "regexErrorMessage": "Invalid Prefix"
                                        },
                                        {
                                            "type": "checkbox",
                                            "label": "Use SSL for connection",
                                            "value": "useSSL",
                                            "default": true
                                        }
                                    ]
                                },
                                {
                                    "title": "Consent Settings",
                                    "fields": [
                                        {
                                            "type": "dynamicCustomForm",
                                            "label": "OneTrust Cookie Categories",
                                            "value": "oneTrustCookieCategories",
                                            "customFields": [
                                                {
                                                    "type": "textInput",
                                                    "label": "Category Name/ID",
                                                    "value": "oneTrustCookieCategory",
                                                    "required": false,
                                                    "placeholder": "Marketing"
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ],
                            "id": "1RtHfnhUWl7dSaBpX06aELPxk88",
                            "name": "MINIO",
                            "displayName": "MinIO",
                            "category": null,
                            "createdAt": "2019-10-07T20:19:37.039Z",
                            "updatedAt": "2023-03-27T11:41:11.179Z"
                        },
                        "transformations": [],
                        "isConnectionEnabled": true,
                        "isProcessorEnabled": true,
                        "enabled": false,
                        "deleted": false,
                        "createdAt": "2023-05-02T11:34:16.039Z",
                        "updatedAt": "2023-05-02T11:34:16.039Z",
                        "revisionId": "2PEfeUJ7eOAp1vdFTlIqhQXD7Vn",
                        "secretVersion": "1"
                    }
                ],
                "sourceDefinition": {
                    "options": null,
                    "config": null,
                    "configSchema": {},
                    "uiConfig": null,
                    "name": "webhook",
                    "id": "1wIQy7WpN1CmQQnx6kHE7H5hTHA",
                    "displayName": "Webhook Source",
                    "category": "webhook",
                    "createdAt": "2021-08-05T06:11:14.646Z",
                    "updatedAt": "2023-04-26T10:59:31.176Z",
                    "type": "cloud"
                },
                "name": "Test",
                "writeKey": "myRandomWritekey",
                "enabled": true,
                "deleted": false,
                "createdBy": "26i2MWBQvNqlSjN3Wdi8vaxWf3K",
                "transient": false,
                "secretVersion": null,
                "createdAt": "2022-12-07T09:25:57.249Z",
                "updatedAt": "2022-12-07T09:25:57.249Z",
                "sourceDefinitionId": "1wIQy7WpN1CmQQnx6kHE7H5hTHA"
            }
        ],
        "whtProjects": [],
        "libraries": [],
        "settings": {
            "dataRetention": {
                "disableReportingPii": false,
                "useSelfStorage": false,
                "retentionPeriod": "default",
                "storagePreferences": {
                    "gatewayDumps": false
                }
            }
        },
        "updatedAt": "2023-05-02T11:36:09.084Z"
    }
}
