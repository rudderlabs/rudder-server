{
    "{{.workspaceId}}": {
        "enableMetrics": false,
        "workspaceId": "{{.workspaceId}}",
        "sources": [
            {
                "config": {},
                "id": "{{.sourceId}}",
                "name": "Destination Isolation Source",
                "writeKey": "{{.writeKey}}",
                "enabled": true,
                "sourceDefinitionId": "xxxyyyzzpWDzNxgGUYzq9sZdZZB",
                "workspaceId": "{{.workspaceId}}",
                "deleted": false,
                "createdAt": "2021-08-27T06:33:00.305Z",
                "updatedAt": "2021-08-27T06:33:00.305Z",
                "destinations": [
                    {{- range $i, $dest := .destinations}}
                    {{if $i}},{{end}}
                    {
                        "config": {
                            "webhookUrl": "{{$.webhookUrl}}",
                            "webhookMethod": "POST"
                        },
                        "secretConfig": {},
                        "id": "{{$dest.id}}",
                        "name": "Webhook {{$dest.id}}",
                        "enabled": true,
                        "workspaceId": "{{$.workspaceId}}",
                        "deleted": false,
                        "createdAt": "2021-08-27T06:49:38.546Z",
                        "updatedAt": "2021-08-27T06:49:38.546Z",
                        "transformations": [
                            {{- if $dest.transformationVersionID}}
                            {
                                "id": "{{$dest.transformationVersionID}}",
                                "versionId": "{{$dest.transformationVersionID}}",
                                "config": {}
                            }
                            {{- end}}
                        ],
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
                    {{- end}}
                ],
                "sourceDefinition": {
                    "id": "xxxyyyzzpWDzNxgGUYzq9sZdZZB",
                    "name": "HTTP",
                    "options": null,
                    "displayName": "HTTP",
                    "category": "",
                    "createdAt": "2020-06-12T06:35:35.962Z",
                    "updatedAt": "2020-06-12T06:35:35.962Z"
                },
                "dgSourceTrackingPlanConfig": null
            }
        ],
        "libraries": []
    }
}
