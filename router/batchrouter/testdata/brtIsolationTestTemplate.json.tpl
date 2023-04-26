{
{{- range $index, $workspace := .workspaces}}
    {{if $index }},{{ end }}
    "{{$workspace}}" : {
        "enableMetrics": false,
        "workspaceId": "{{$workspace}}",
        "sources": [
            {
                "config": {},
                "liveEventsConfig": {},
                "id": "{{$workspace}}",
                "workspaceId": "{{$workspace}}",
                "destinations": [
                    {
                        "id": "{{$workspace}}-MINIO",
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
                        "workspaceId": "{{$workspace}}",
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
                        "enabled": true,
                        "deleted": false,
                        "createdAt": "2023-05-02T11:34:16.039Z",
                        "updatedAt": "2023-05-02T11:34:16.039Z",
                        "revisionId": "2PEfeUJ7eOAp1vdFTlIqhQXD7Vn",
                        "secretVersion": "1"
                    },
                    {
                        "id": "{{$workspace}}-POSTGRES",
                        "name": "batchrt-isolation-postgres",
                        "secretConfig": {},
                        "config": {
                            "host": "localhost",
                            "database": "database",
                            "user": "user",
                            "password": "password",
                            "port": "5432",
                            "namespace": "",
                            "sslMode": "disable",
                            "bucketProvider": "MINIO",
                            "endPoint": "{{$.minioEndpoint}}",
                            "bucketName": "{{$.minioBucket}}",
                            "accessKeyID": "{{$.minioAccessKeyID}}",
                            "secretAccessKey": "{{$.minioSecretAccessKey}}",
                            "useSSL": false,
                            "syncFrequency": "30",
                            "jsonPaths": "",
                            "useRudderStorage": false,
                            "oneTrustCookieCategories": []
                        },
                        "liveEventsConfig": {},
                        "workspaceId": "{{$workspace}}",
                        "destinationDefinition": {
                            "config": {
                                "destConfig": {
                                    "defaultConfig": [
                                        "host",
                                        "database",
                                        "user",
                                        "password",
                                        "port",
                                        "namespace",
                                        "useSSH",
                                        "sshHost",
                                        "sshPort",
                                        "sshUser",
                                        "sshPublicKey",
                                        "sslMode",
                                        "bucketProvider",
                                        "bucketName",
                                        "iamRoleARN",
                                        "roleBasedAuth",
                                        "accessKeyID",
                                        "accessKey",
                                        "accountName",
                                        "accountKey",
                                        "useSASTokens",
                                        "sasToken",
                                        "credentials",
                                        "secretAccessKey",
                                        "useSSL",
                                        "containerName",
                                        "endPoint",
                                        "syncFrequency",
                                        "syncStartAt",
                                        "excludeWindow",
                                        "jsonPaths",
                                        "useRudderStorage",
                                        "clientKey",
                                        "clientCert",
                                        "serverCA",
                                        "oneTrustCookieCategories"
                                    ]
                                },
                                "secretKeys": [
                                    "password",
                                    "accessKeyID",
                                    "accessKey",
                                    "accountKey",
                                    "sasToken",
                                    "secretAccessKey",
                                    "credentials"
                                ],
                                "excludeKeys": [],
                                "includeKeys": [
                                    "oneTrustCookieCategories"
                                ],
                                "transformAt": "processor",
                                "transformAtV1": "processor",
                                "supportedSourceTypes": [
                                    "android",
                                    "ios",
                                    "web",
                                    "unity",
                                    "amp",
                                    "cloud",
                                    "reactnative",
                                    "cloudSource",
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
                                    "title": "Connection Credentials",
                                    "fields": [
                                        {
                                            "type": "textInput",
                                            "label": "Host",
                                            "regex": "(?!.*\\.ngrok\\.io)^(.{0,100})$",
                                            "value": "host",
                                            "required": true,
                                            "placeholder": "e.g: psql.mydomain.com",
                                            "regexErrorMessage": "Invalid Host"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Database",
                                            "regex": "^(.{0,100})$",
                                            "value": "database",
                                            "required": true,
                                            "placeholder": "e.g: rudderdb",
                                            "regexErrorMessage": "Invalid Database"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "User",
                                            "regex": "^(.{0,100})$",
                                            "value": "user",
                                            "required": true,
                                            "placeholder": "e.g: rudder",
                                            "regexErrorMessage": "Invalid User"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Password",
                                            "regex": ".*",
                                            "value": "password",
                                            "required": true,
                                            "placeholder": "e.g: rudder-password"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Port",
                                            "regex": "^(.{0,100})$",
                                            "value": "port",
                                            "required": true,
                                            "placeholder": "5432",
                                            "regexErrorMessage": "Invalid Port"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Namespace",
                                            "regex": "^((?!pg_|PG_|pG_|Pg_).{0,64})$",
                                            "value": "namespace",
                                            "required": false,
                                            "labelNote": "Schema name for the warehouse where the tables are created",
                                            "footerNote": "Default will be the source name",
                                            "placeholder": "e.g: rudder-schema",
                                            "regexErrorMessage": "Invalid Namespace"
                                        },
                                        {
                                            "type": "singleSelect",
                                            "label": "SSL Mode",
                                            "value": "sslMode",
                                            "options": [
                                                {
                                                    "name": "disable",
                                                    "value": "disable"
                                                },
                                                {
                                                    "name": "require",
                                                    "value": "require"
                                                },
                                                {
                                                    "name": "verify ca",
                                                    "value": "verify-ca"
                                                }
                                            ],
                                            "required": true,
                                            "defaultOption": {
                                                "name": "disable",
                                                "value": "disable"
                                            }
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Client Key Pem File",
                                            "regex": "-----BEGIN RSA PRIVATE KEY-----.*-----END RSA PRIVATE KEY-----",
                                            "value": "clientKey",
                                            "required": true,
                                            "preRequisiteField": {
                                                "name": "sslMode",
                                                "selectedValue": "verify-ca"
                                            }
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Client Cert Pem File",
                                            "regex": "-----BEGIN CERTIFICATE-----.*-----END CERTIFICATE-----",
                                            "value": "clientCert",
                                            "required": true,
                                            "preRequisiteField": {
                                                "name": "sslMode",
                                                "selectedValue": "verify-ca"
                                            }
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Server CA Pem File",
                                            "regex": "-----BEGIN CERTIFICATE-----.*-----END CERTIFICATE-----",
                                            "value": "serverCA",
                                            "required": true,
                                            "preRequisiteField": {
                                                "name": "sslMode",
                                                "selectedValue": "verify-ca"
                                            }
                                        },
                                        {
                                            "type": "singleSelect",
                                            "label": "Sync Frequency",
                                            "value": "syncFrequency",
                                            "options": [
                                                {
                                                    "name": "Every 30 minutes",
                                                    "value": "30"
                                                },
                                                {
                                                    "name": "Every 1 hour",
                                                    "value": "60"
                                                },
                                                {
                                                    "name": "Every 3 hours",
                                                    "value": "180"
                                                },
                                                {
                                                    "name": "Every 6 hours",
                                                    "value": "360"
                                                },
                                                {
                                                    "name": "Every 12 hours",
                                                    "value": "720"
                                                },
                                                {
                                                    "name": "Every 24 hours",
                                                    "value": "1440"
                                                }
                                            ],
                                            "required": false,
                                            "defaultOption": {
                                                "name": "Every 30 minutes",
                                                "value": "30"
                                            }
                                        },
                                        {
                                            "type": "timePicker",
                                            "label": "Sync Starting At (Optional)",
                                            "value": "syncStartAt",
                                            "options": {
                                                "minuteStep": 15,
                                                "omitSeconds": true
                                            },
                                            "required": false,
                                            "footerNote": "Note: Please specify time in UTC"
                                        },
                                        {
                                            "type": "timeRangePicker",
                                            "label": "Exclude window (Optional)",
                                            "value": "excludeWindow",
                                            "endTime": {
                                                "label": "end time",
                                                "value": "excludeWindowEndTime"
                                            },
                                            "options": {
                                                "minuteStep": 1,
                                                "omitSeconds": true
                                            },
                                            "required": false,
                                            "startTime": {
                                                "label": "start time",
                                                "value": "excludeWindowStartTime"
                                            },
                                            "footerNote": "Note: Please specify time in UTC"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Json columns (Optional)",
                                            "regex": "^(.*)$",
                                            "value": "jsonPaths",
                                            "required": false,
                                            "footerURL": {
                                                "link": "https://www.rudderstack.com/docs/data-warehouse-integrations/postgresql/#configuring-postgresql-destination-in-rudderstack",
                                                "text": "Instructions for setting up the json columns"
                                            },
                                            "labelNote": "Specify required json properties in dot notation separated by commas",
                                            "placeholder": "e.g: testMap.nestedMap, testArray"
                                        }
                                    ]
                                },
                                {
                                    "title": "Object Storage Configuration",
                                    "fields": [
                                        {
                                            "type": "checkbox",
                                            "label": "Use RudderStack managed object storage",
                                            "value": "useRudderStorage",
                                            "default": false,
                                            "footerNote": "Note: Only available for RudderStack managed data planes"
                                        },
                                        {
                                            "type": "singleSelect",
                                            "label": "Choose your Storage Provider",
                                            "value": "bucketProvider",
                                            "options": [
                                                {
                                                    "name": "S3",
                                                    "value": "S3"
                                                },
                                                {
                                                    "name": "GCS",
                                                    "value": "GCS"
                                                },
                                                {
                                                    "name": "AZURE_BLOB",
                                                    "value": "AZURE_BLOB"
                                                },
                                                {
                                                    "name": "MINIO",
                                                    "value": "MINIO"
                                                }
                                            ],
                                            "required": true,
                                            "defaultOption": {
                                                "name": "MINIO",
                                                "value": "MINIO"
                                            },
                                            "preRequisiteField": {
                                                "name": "useRudderStorage",
                                                "selectedValue": false
                                            }
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Staging S3 Storage Bucket Name",
                                            "regex": "^((?!^xn--)(?!.*\\.\\..*)(?!^(\\d+(\\.|$)){4}$)[a-z0-9][a-z0-9-.]{1,61}[a-z0-9])$",
                                            "value": "bucketName",
                                            "required": true,
                                            "labelNote": "S3 Bucket to store data before loading into Postgres",
                                            "footerNote": "Please make sure the bucket exists in your S3",
                                            "placeholder": "e.g: s3-event-logs",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "S3"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                }
                                            ],
                                            "regexErrorMessage": "Invalid Staging S3 Storage Bucket Name"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Staging GCS Object Storage Bucket Name",
                                            "regex": "^((?!goog)(?!.*google.*)(?!^(\\d+(\\.|$)){4}$)(?!.*\\.\\..*)[a-z0-9][a-z0-9-._]{1,61}[a-z0-9])$",
                                            "value": "bucketName",
                                            "required": true,
                                            "labelNote": "GCS Bucket to store data before loading into Postgres",
                                            "footerNote": "Please make sure the bucket exists in your GCS",
                                            "placeholder": "e.g: gcs-event-logs",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "GCS"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                }
                                            ],
                                            "regexErrorMessage": "Invalid Staging GCS Object Storage Bucket Name"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Staging Azure Blob Storage Container Name",
                                            "regex": "^(?=.{3,63}$)[a-z0-9]+(-[a-z0-9]+)*$",
                                            "value": "containerName",
                                            "required": true,
                                            "labelNote": "Container to store data before loading into Postgres",
                                            "footerNote": "Please make sure the container exists in your Azure Blob Storage",
                                            "placeholder": "e.g: azure-event-logs",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "AZURE_BLOB"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                }
                                            ],
                                            "regexErrorMessage": "Invalid Staging Azure Blob Storage Container Name"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Staging MINIO Storage Bucket Name",
                                            "regex": "^((?!^(\\d+(\\.|$)){4}$)[a-z0-9][a-z0-9-.]{1,61}[a-z0-9])$",
                                            "value": "bucketName",
                                            "required": true,
                                            "labelNote": "MINIO Bucket to store data before loading into Postgres",
                                            "footerNote": "Please make sure the bucket exists in your MINIO",
                                            "placeholder": "e.g: minio-event-logs",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "MINIO"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                }
                                            ],
                                            "regexErrorMessage": "Invalid Staging MINIO Storage Bucket Name"
                                        },
                                        {
                                            "type": "checkbox",
                                            "label": "Role Based Authentication",
                                            "value": "roleBasedAuth",
                                            "default": true,
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "S3"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                }
                                            ]
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "IAM Role ARN",
                                            "regex": "^(.{0,100})$",
                                            "value": "iamRoleARN",
                                            "required": true,
                                            "footerURL": {
                                                "link": "https://www.rudderstack.com/docs/destinations/aws-iam-role-for-rudderstack/",
                                                "text": "Instructions for creating IAM Role"
                                            },
                                            "placeholder": "e.g: arn:aws:iam::123456789012:role/S3Access",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "S3"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                },
                                                {
                                                    "name": "roleBasedAuth",
                                                    "selectedValue": true
                                                }
                                            ],
                                            "regexErrorMessage": "Invalid Role ARN"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "AWS Access Key ID",
                                            "regex": "^(.{0,100})$",
                                            "value": "accessKeyID",
                                            "required": true,
                                            "placeholder": "e.g: access-key-id",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "S3"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                },
                                                {
                                                    "name": "roleBasedAuth",
                                                    "selectedValue": false
                                                }
                                            ],
                                            "regexErrorMessage": "Invalid AWS Access Key ID"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "AWS Secret Access Key",
                                            "regex": "^(.{0,100})$",
                                            "value": "accessKey",
                                            "required": true,
                                            "placeholder": "e.g: secret-access-key",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "S3"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                },
                                                {
                                                    "name": "roleBasedAuth",
                                                    "selectedValue": false
                                                }
                                            ],
                                            "regexErrorMessage": "Invalid AWS Secret Access Key"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Azure Blob Storage Account Name",
                                            "regex": "^(.{0,100})$",
                                            "value": "accountName",
                                            "required": true,
                                            "placeholder": "e.g: account-name",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "AZURE_BLOB"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                }
                                            ],
                                            "regexErrorMessage": "Invalid Azure Blob Storage Account Name"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Azure Blob Storage Account Key",
                                            "regex": "^(.{0,100})$",
                                            "value": "accountKey",
                                            "secret": true,
                                            "required": true,
                                            "placeholder": "e.g: account-key",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "AZURE_BLOB"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                },
                                                {
                                                    "name": "useSASTokens",
                                                    "selectedValue": false
                                                }
                                            ],
                                            "regexErrorMessage": "Invalid Azure Blob Storage Account Key"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "Azure Blob Storage SAS Token",
                                            "regex": "^(.+)$",
                                            "value": "sasToken",
                                            "secret": true,
                                            "required": true,
                                            "placeholder": "e.g: sas-token",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "AZURE_BLOB"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                },
                                                {
                                                    "name": "useSASTokens",
                                                    "selectedValue": true
                                                }
                                            ],
                                            "regexErrorMessage": "Invalid Azure Blob Storage SAS Token"
                                        },
                                        {
                                            "type": "checkbox",
                                            "label": "Use shared access signature (SAS) Tokens",
                                            "value": "useSASTokens",
                                            "default": false,
                                            "footerNote": "Use this to Grant limited access to Azure Storage resources using shared access signatures (SAS)",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "AZURE_BLOB"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                }
                                            ]
                                        },
                                        {
                                            "type": "textareaInput",
                                            "label": "Credentials",
                                            "regex": ".*",
                                            "value": "credentials",
                                            "required": true,
                                            "labelNote": "GCP Service Account credentials JSON for RudderStack to use in loading data into your Google Cloud Storage",
                                            "footerNote": "Create a service account in your GCP Project for RudderStack with roles of 'storage.objectCreator'",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "GCS"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                }
                                            ]
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "MinIO Endpoint",
                                            "regex": "(?!.*\\.ngrok\\.io)^(.{0,100})$",
                                            "value": "endPoint",
                                            "required": true,
                                            "placeholder": "e.g: play.min.io:9000",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "MINIO"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                }
                                            ],
                                            "regexErrorMessage": "Invalid MinIO Endpoint"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "MINIO Access Key ID",
                                            "regex": "^(.{0,100})$",
                                            "value": "accessKeyID",
                                            "required": true,
                                            "placeholder": "e.g: access-key-id",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "MINIO"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                }
                                            ],
                                            "regexErrorMessage": "Invalid MinIO Access Key ID"
                                        },
                                        {
                                            "type": "textInput",
                                            "label": "MINIO Secret Access Key",
                                            "regex": "^(.{0,100})$",
                                            "value": "secretAccessKey",
                                            "required": true,
                                            "placeholder": "e.g: secret-access-key",
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "MINIO"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                }
                                            ],
                                            "regexErrorMessage": "Invalid MinIO Secret Access Key"
                                        },
                                        {
                                            "type": "checkbox",
                                            "label": "Use SSL for connection",
                                            "value": "useSSL",
                                            "default": true,
                                            "preRequisiteField": [
                                                {
                                                    "name": "bucketProvider",
                                                    "selectedValue": "MINIO"
                                                },
                                                {
                                                    "name": "useRudderStorage",
                                                    "selectedValue": false
                                                }
                                            ]
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
                            "id": "1bJ4YC7INdkvBTzotNh0zta5jDm",
                            "name": "POSTGRES",
                            "displayName": "Postgres",
                            "category": "warehouse",
                            "createdAt": "2020-05-01T12:41:47.463Z",
                            "updatedAt": "2023-03-27T11:40:47.251Z"
                        },
                        "transformations": [],
                        "isConnectionEnabled": true,
                        "isProcessorEnabled": true,
                        "enabled": true,
                        "deleted": false,
                        "createdAt": "2023-05-02T11:36:00.037Z",
                        "updatedAt": "2023-05-02T11:36:00.037Z",
                        "revisionId": "2PEfraataiwY0AawjqEjv3L2bBh",
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
                "writeKey": "2Ia21P796M61LiiV4pZoOOfPvln",
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
                    "procErrors": false,
                    "gatewayDumps": false
                }
            }
        },
        "updatedAt": "2023-05-02T11:36:09.084Z"
    }
{{- end }}
}
