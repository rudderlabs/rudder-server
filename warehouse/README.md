# Warehouse Configurations

### Environment

| Config | Default Value | Description |
|--------|---------------| --- |
| WAREHOUSE_JOBS_DB_HOST       |localhost|  |
| WAREHOUSE_JOBS_DB_USER       |ubuntu|  |
| WAREHOUSE_JOBS_DB_DB_NAME       |ubuntu|  |
| WAREHOUSE_JOBS_DB_PORT       |5432|  |
| WAREHOUSE_JOBS_DB_PASSWORD       |ubuntu|  |
| WAREHOUSE_JOBS_DB_SSL_MODE       |disable|  |
| RUDDER_AWS_S3_COPY_USER_ACCESS_KEY_ID       ||  |
| RUDDER_AWS_S3_COPY_USER_ACCESS_KEY       ||  |
| RSERVER_WAREHOUSE_RUNNING_MODE       ||  |
| HOSTED_SERVICE       |false|  |
| HOSTED_SERVICE_SECRET       |password|  |
| CP_ROUTER_USE_TLS       |true|  |
| CONFIG_BACKEND_URL       |https://api.rudderlabs.com|  |
| WAREHOUSE_DATALAKE_FOLDER_NAME       |rudder-datalake|  |
| WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME       |rudder-warehouse-load-object|  |
| DATABRICKS_CONNECTOR_URL       |localhost:50051|  |
| RUDDER_CONNECTION_TESTING_BUCKET_FOLDER_NAME       |rudder-test-payload|  |
| JOBS_BACKUP_STORAGE_PROVIDER       |S3|  |
| WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME       |rudder-warehouse-load-objects|  |

### Global

| Config                         | Default Value | Hot Reloadable | Multiplier                                                         | Description                                                              |
|--------------------------------|---------------|----------------|--------------------------------------------------------------------|--------------------------------------------------------------------------|
| Warehouse.webPort | 8082          | false          | 1                                                                  |                                                                          |
| Warehouse.noOfSlaveWorkerRoutines | 4             | true           | 1                                                                  |                                                                          |
| Warehouse.stagingFilesBatchSize | 960           | true           | 1                                                                  |                                                                          |
| Warehouse.uploadFreqInS | 1800          | true           | 1                                                                  |                                                                          |
| Warehouse.mode | embedded      | false          |                                                                    |                                                                          |
| Warehouse.warehouseSyncPreFetchCount | 10            | true           | 1                                                                  |                                                                          |
| Warehouse.stagingFilesSchemaPaginationSize | 100           | true           | 1                                                                  |                                                                          |
| Warehouse.warehouseSyncFreqIgnore | false         | true           |                                                                    |                                                                          |
| Warehouse.minRetryAttempts | 3             | true           | 1                                                                  |                                                                          |
| Warehouse.numLoadFileUploadWorkers | 8             | true           | 1                                                                  |                                                                          |
| SQLMigrator.forceSetLowerVersion | true          | false          |                                                                    |                                                                          |
| Reporting.enabled | true          | false          |                                                                    |                                                                          |
| Warehouse.skipDeepEqualSchemas | false         | true           |                                                                    |                                                                          |
| Warehouse.enableJitterForSyncs | false         | true           |                                                                    |                                                                          |
| Warehouse.useParquetLoadFilesRS | false         | true           |                                                                    |                                                                          |
| Warehouse.enableIDResolution | false         | false          |                                                                    |                                                                          |
| Warehouse.archiveUploadRelatedRecords | 5             | true           | 1                                                                  |                                                                          |
| Warehouse.uploadsArchivalTimeInDays | true          | true           |                                                                    |                                                                          |
| Warehouse.populateHistoricIdentities | false         | false          |                                                                    |                                                                          |
| Warehouse.enableConstraintsViolations | true          | true           |                                                                    |                                                                          |
| Warehouse.alwaysRegenerateAllLoadFiles | true          | false          |                                                                    |                                                                          |
| Warehouse.generateTableLoadCountMetrics | true          | false          |                                                                    |                                                                          |
| Warehouse.skipMetricTagForEachEventTable | false          | false          |                                                                    |                                                                          |
| Warehouse.uploadBufferTimeInMin | 180           | false          | 1                                                                  |                                                                          |
| Warehouse.pgNotifierPublishBatchSize | 100           | false          |  | Controls batching of staging files to be sent over to pg_notifier queue. |
| Warehouse.Archiver.backupRowsBatchSize| 100           | false          |                                                                    |                                                                          |
| Warehouse.maxParallelJobCreation | 8             | true           | 1                                                                  |                                                                          |
| Warehouse.parquetParallelWriters | 8             | true           | 1                                                                  |                                                                          |
| Warehouse.awsCredsExpiryInS | 3600          | true           | 1  | Expiry time for AWS session Token in seconds                             |
| Warehouse.maxStagingFileReadBufferCapacityInK | 10240         | false          | 1                                                                  |                                                                          |
| Warehouse.minUploadBackoff<br/>Warehouse.minUploadBackoffInS | 60            | true           | 1                                                                  |                                                                          |
| Warehouse.minUploadBackoff<br/>Warehouse.minUploadBackoffInS | 60            | true           | 1                                                                  |                                                                          |
| Warehouse.archiverTickerTime<br/>Warehouse.archiverTickerTimeInMin | 360           | true           | 1                                                                  |                                                                          |
| Warehouse.waitForConfig<br/>Warehouse.waitForConfigInS | 5             | false          | 1                                                                  |                                                                          |
| Warehouse.waitForWorkerSleep<br/>Warehouse.waitForWorkerSleepInS | 5             | false          | 1                                                                  |                                                                          |
| Warehouse.uploadAllocatorSleep<br/>Warehouse.uploadAllocatorSleepInS | 5             | false          | 1                                                                  |                                                                          |
| Warehouse.uploadStatusTrackFrequency<br/>Warehouse.uploadStatusTrackFrequencyInMin | 30            | false          | 1                                                                  |                                                                          |
| Warehouse.maxStagingFileReadBufferCapacityInK | 10240         | true           | 1                                                                  |                                                                          |
| Warehouse.slaveUploadTimeout<br/>Warehouse.slaveUploadTimeoutInMin | 10            | true           | 1                                                                  |                                                                          |
| Warehouse.longRunningUploadStatThreshold<br/>Warehouse.longRunningUploadStatThresholdInMin | 120           | true           | 1                                                                  |                                                                          |
| Warehouse.retryTimeWindow<br/>Warehouse.retryTimeWindowInMins | 180           | true           | 1                                                                  |                                                                          |
| Warehouse.mainLoopSleepInS<br/>Warehouse.mainLoopSleepInS | 5             | true           | 1                                                                  |                                                                          |

### Across All Destination

Please add corresponding destination type. Corresponding values includes:

1. postgres
2. redshift
3. snowflake
4. gcs_datalake
5. s3_datalake
6. azure_datalake
7. mssql
8. azure_synapse
9. big_query
10. clickhouse
11. deltalake

| Config                          | Default Value | Hot Reloadable | Multiplier | Description |
|---------------------------------|--------------| --- |----------| --- |
| Warehouse.{destType}.noOfWorkers<br/>Warehouse.noOfWorkers | 8            | true | 1        |             |
| Warehouse.{destType}.maxConcurrentUploadJobs | 1            | false | 1        |             |
| Warehouse.{destType}.allowMultipleSourcesForJobsPickup | false            | false |          |             |

### Destination specific

### REDSHIFT

| Config                                           | Default Value | Hot Reloadable | Multiplier | Description |
|--------------------------------------------------|---------------| --- | --- | --- |
| Warehouse.redshift.skipComputingUserLatestTraits | false         | true |          |             |
| Warehouse.redshift.setVarCharMax                 | false         | false | 1         |             |
| Warehouse.redshift.maxParallelLoads              | 3             | false          |            |             |
| Warehouse.redshift.columnCountThreshold          | 1200          | false          |            |             |
| Warehouse.redshift.columnCountThreshold          | 1200          | false          |            |             |
| Warehouse.redshift.customDatasetPrefix           |           | false          |            |             |

### SNOWFLAKE

| Config                                   | Default Value | Hot Reloadable | Multiplier | Description |
|------------------------------------------|---------------| --- | --- | --- |
| Warehouse.snowflake.maxParallelLoads     | 3             | false          |            |             |
| Warehouse.snowflake.columnCountThreshold | 1600          | false          |            |             |
| Warehouse.snowflake.customDatasetPrefix  |           | false          |            |             |

### POSTGRES

| Config                                             | Default Value | Hot Reloadable | Multiplier | Description |
|----------------------------------------------------|---------------| --- | --- | --- |
| Warehouse.postgres.skipComputingUserLatestTraits   | false         | true |          |             |
| Warehouse.postgres.txnRollbackTimeout              | 30            | true | 1         |             |
| Warehouse.postgres.enableSQLStatementExecutionPlan | false         | true |          |             |
| Warehouse.postgres.maxParallelLoads                | 3             | false          |            |             |
| Warehouse.postgres.columnCountThreshold            | 1200          | false          |            |             |
| Warehouse.postgres.customDatasetPrefix             |           | false          |            |             |

### DELTALAKE

| Config                                  | Default Value | Hot Reloadable | Multiplier | Description |
|-----------------------------------------| --- | --- |------------| --- |
| Warehouse.deltalake.schema              | default       | false          |            |             |
| Warehouse.deltalake.sparkServerType     | 3             | false          | 1          |             |
| Warehouse.deltalake.authMech            | 3             | false          |            |             |
| Warehouse.deltalake.uid                 | token         | false          |            |             |
| Warehouse.deltalake.thriftTransport     | 2             | false          |            |             |
| Warehouse.deltalake.ssl                 | 1             | false          |            |             |
| Warehouse.deltalake.userAgent           | RudderStack   | false          |            |             |
| Warehouse.deltalake.grpcTimeout         | 2             | false          | 1          |             |
| Warehouse.deltalake.healthTimeout       | 15            | false          | 1          |             |
| Warehouse.deltalake.loadTableStrategy   | MERGE         | true           | 1          |             |
| Warehouse.deltalake.maxParallelLoads    | 3             | false          |            |             |
| Warehouse.deltalake.customDatasetPrefix |           | false          |            |             |

### BIGQUERY

| Config                                                   | Default Value | Hot Reloadable | Multiplier | Description |
|----------------------------------------------------------|---------------| --- |-------------| --- |
| Warehouse.bigquery.setUsersLoadPartitionFirstEventFilter | true          | true |         |             |
| Warehouse.bigquery.customPartitionsEnabled               | false         | true |         |             |
| Warehouse.bigquery.isUsersTableDedupEnabled              | false         | true |         |             |
| Warehouse.bigquery.isUsersTableDedupEnabled              | false         | true |         |             |
| Warehouse.bigquery.maxParallelLoads                      | 20            | false          |            |             |
| Warehouse.bigquery.columnCountThreshold                  | 8000          | false          |            |             |
| Warehouse.bogquery.customDatasetPrefix                   |           | false          |            |             |

### Clickhouse

| Config                                           | Default Value | Hot Reloadable | Multiplier | Description |
|--------------------------------------------------|---------------| --- |-------------| --- |
| Warehouse.clickhouse.queryDebugLogs              | false         | true |          |             |
| Warehouse.clickhouse.blockSize                   | 1000000       | true | 1         |             |
| Warehouse.clickhouse.poolSize                    | 100           | true |          |             |
| Warehouse.clickhouse.disableNullable             | false         | false |         |             |
| Warehouse.clickhouse.readTimeout                 | 300           | true |   |             |
| Warehouse.clickhouse.writeTimeout                | 1800          | true |   |             |
| Warehouse.clickhouse.compress                    | false         | true |   |             |
| Warehouse.clickhouse.execTimeOutInSeconds        | 600           | true | 1   |             |
| Warehouse.clickhouse.commitTimeOutInSeconds      | 600           | true | 1   |             |
| Warehouse.clickhouse.loadTableFailureRetries     | 3             | true | 1 |             |
| Warehouse.clickhouse.numWorkersDownloadLoadFiles | 8             | true | 1   |             |
| Warehouse.clickhouse.maxParallelLoads            | 3             | false          |            |             |
| Warehouse.clickhouse.columnCountThreshold        | 800           | false          |            |             |
| Warehouse.clickhouse.customDatasetPrefix         |           | false          |            |             |

### MSSQL

| Config                               | Default Value | Hot Reloadable | Multiplier | Description |
|--------------------------------------|---------------| --- |-------------| --- |
| Warehouse.mssql.maxParallelLoads     | 3             | false          |            |             |
| Warehouse.mssql.columnCountThreshold | 800           | false          |            |             |
| Warehouse.mssql.customDatasetPrefix  |           | false          |            |             |

### AZURE SYNAPSE

| Config                                   | Default Value | Hot Reloadable | Multiplier | Description |
|------------------------------------------|--------------| --- |-------------| --- |
| Warehouse.azure_synapse.maxParallelLoads | 3            | false          |            |             |
| Warehouse.azure_synapse.customDatasetPrefix |           | false          |            |             |

### GCS DATALAKE

| Config                                     | Default Value | Hot Reloadable | Multiplier | Description |
|--------------------------------------------|---------------| --- |-------------| --- |
| Warehouse.gcs_datalake.customDatasetPrefix |           | false          |            |             |

### AZURE DATALAKE

| Config                                       | Default Value | Hot Reloadable | Multiplier | Description |
|----------------------------------------------|---------------| --- |-------------| --- |
| Warehouse.azure_datalake.customDatasetPrefix |           | false          |            |             |

### S3 DATALAKE

| Config                                      | Default Value | Hot Reloadable | Multiplier | Description |
|---------------------------------------------|---------------| --- |-------------| --- |
| Warehouse.sq_datalake.customDatasetPrefix   |           | false          |            |             |
