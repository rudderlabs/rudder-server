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
                        "config": {
                            "webhookUrl": "{{$.webhookURL}}",
                            "webhookMethod": "POST"
                        },
                        "secretConfig": {},
                        "id": "{{$workspace}}",
                        "name": "rt-isolation-webhook-{{$workspace}}",
                        "enabled": true,
                        "workspaceId": "{{$workspace}}",
                        "deleted": false,
                        "createdAt": "2021-08-27T06:49:38.546Z",
                        "updatedAt": "2021-08-27T06:49:38.546Z",
                        "transformations": [],
                        "destinationDefinition": {
                            "config": {
                                "destConfig": {
                                    "defaultConfig": [
                                        "webhookUrl",
                                        "webhookMethod",
                                        "headers"
                                    ]
                                },
                                "secretKeys": [
                                    "headers.to"
                                ],
                                "excludeKeys": [],
                                "includeKeys": [],
                                "transformAt": "processor",
                                "transformAtV1": "processor",
                                "supportedSourceTypes": [
                                    "android",
                                    "ios",
                                    "web",
                                    "unity",
                                    "amp",
                                    "cloud",
                                    "warehouse",
                                    "reactnative",
                                    "flutter"
                                ],
                                "supportedMessageTypes": [
                                    "alias",
                                    "group",
                                    "identify",
                                    "page",
                                    "screen",
                                    "track"
                                ],
                                "saveDestinationResponse": false
                            },
                            "configSchema": null,
                            "responseRules": null,
                            "id": "xxxyyyzzSOU9pLRavMf0GuVnWV3",
                            "name": "WEBHOOK",
                            "displayName": "Webhook",
                            "category": null,
                            "createdAt": "2020-03-16T19:25:28.141Z",
                            "updatedAt": "2021-08-26T07:06:01.445Z"
                        },
                        "isConnectionEnabled": true,
                        "isProcessorEnabled": true
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
