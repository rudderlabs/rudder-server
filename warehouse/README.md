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
| Warehouse.webPort | 8082          | ❌              | 1                                                                  |                                                                          |
| Warehouse.noOfSlaveWorkerRoutines | 4             | ✅              | 1                                                                  |                                                                          |
| Warehouse.stagingFilesBatchSize | 960           | ✅              | 1                                                                  |                                                                          |
| Warehouse.uploadFreqInS | 1800          | ✅       1      | 1                                                                  |                                                                          |
| Warehouse.mode | embedded      | ❌              |                                                                    |                                                                          |
| Warehouse.warehouseSyncPreFetchCount | 10            | ✅              | 1                                                                  |                                                                          |
| Warehouse.stagingFilesSchemaPaginationSize | 100           | ✅              | 1                                                                  |                                                                          |
| Warehouse.warehouseSyncFreqIgnore | false         | ✅              |                                                                    |                                                                          |
| Warehouse.minRetryAttempts | 3             | ✅              | 1                                                                  |                                                                          |
| Warehouse.numLoadFileUploadWorkers | 8             | ✅              | 1                                                                  |                                                                          |
| SQLMigrator.forceSetLowerVersion | true          | ❌              |                                                                    |                                                                          |
| Reporting.enabled | true          | ❌              |                                                                    |                                                                          |
| Warehouse.skipDeepEqualSchemas | false         | ✅              |                                                                    |                                                                          |
| Warehouse.enableJitterForSyncs | false         | ✅              |                                                                    |                                                                          |
| Warehouse.useParquetLoadFilesRS | false         | ✅              |                                                                    |                                                                          |
| Warehouse.enableIDResolution | false         | ❌              |                                                                    |                                                                          |
| Warehouse.archiveUploadRelatedRecords | 5             | ✅              | 1                                                                  |                                                                          |
| Warehouse.uploadsArchivalTimeInDays | true          | ✅              |                                                                    |                                                                          |
| Warehouse.populateHistoricIdentities | false         | ❌              |                                                                    |                                                                          |
| Warehouse.enableConstraintsViolations | true          | ✅              |                                                                    |                                                                          |
| Warehouse.alwaysRegenerateAllLoadFiles | true          | ❌              |                                                                    |                                                                          |
| Warehouse.generateTableLoadCountMetrics | true          | ❌              |                                                                    |                                                                          |
| Warehouse.skipMetricTagForEachEventTable | false          | ❌              |                                                                    |                                                                          |
| Warehouse.uploadBufferTimeInMin | 180           | ❌              | 1                                                                  |                                                                          |
| Warehouse.pgNotifierPublishBatchSize | 100           | ❌              |  | Controls batching of staging files to be sent over to pg_notifier queue. |
| Warehouse.Archiver.backupRowsBatchSize| 100           | ❌              |                                                                    |                                                                          |
| Warehouse.maxParallelJobCreation | 8             | ✅              | 1                                                                  |                                                                          |
| Warehouse.parquetParallelWriters | 8             | ✅              | 1                                                                  |                                                                          |
| Warehouse.awsCredsExpiryInS | 3600          | ✅              | 1  | Expiry time for AWS session Token in seconds                             |
| Warehouse.maxStagingFileReadBufferCapacityInK | 10240         | ❌              | 1                                                                  |                                                                          |
| Warehouse.minUploadBackoff<br/>Warehouse.minUploadBackoffInS | 60            | ✅              | 1                                                                  |                                                                          |
| Warehouse.minUploadBackoff<br/>Warehouse.minUploadBackoffInS | 60            | ✅              | 1                                                                  |                                                                          |
| Warehouse.archiverTickerTime<br/>Warehouse.archiverTickerTimeInMin | 360           | ✅              | 1                                                                  |                                                                          |
| Warehouse.waitForConfig<br/>Warehouse.waitForConfigInS | 5             | ❌              | 1                                                                  |                                                                          |
| Warehouse.waitForWorkerSleep<br/>Warehouse.waitForWorkerSleepInS | 5             | ❌              | 1                                                                  |                                                                          |
| Warehouse.uploadAllocatorSleep<br/>Warehouse.uploadAllocatorSleepInS | 5             | ❌              | 1                                                                  |                                                                          |
| Warehouse.uploadStatusTrackFrequency<br/>Warehouse.uploadStatusTrackFrequencyInMin | 30            | ❌              | 1                                                                  |                                                                          |
| Warehouse.maxStagingFileReadBufferCapacityInK | 10240         | ✅              | 1                                                                  |                                                                          |
| Warehouse.slaveUploadTimeout<br/>Warehouse.slaveUploadTimeoutInMin | 10            | ✅              | 1                                                                  |                                                                          |
| Warehouse.longRunningUploadStatThreshold<br/>Warehouse.longRunningUploadStatThresholdInMin | 120           | ✅              | 1                                                                  |                                                                          |
| Warehouse.retryTimeWindow<br/>Warehouse.retryTimeWindowInMins | 180           | ✅              | 1                                                                  |                                                                          |
| Warehouse.mainLoopSleepInS<br/>Warehouse.mainLoopSleepInS | 5             | ✅              | 1                                                                  |                                                                          |

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
| Warehouse.{destType}.noOfWorkers<br/>Warehouse.noOfWorkers | 8            | ✅ | 1        |             |
| Warehouse.{destType}.maxConcurrentUploadJobs | 1            |  ❌ | 1        |             |
| Warehouse.{destType}.allowMultipleSourcesForJobsPickup | false            |  ❌ |          |             |

### Destination specific

### REDSHIFT

| Config                                           | Default Value | Hot Reloadable | Multiplier | Description |
|--------------------------------------------------|---------------| --- | --- | --- |
| Warehouse.redshift.skipComputingUserLatestTraits | false         | ✅ |          |             |
| Warehouse.redshift.setVarCharMax                 | false         |  ❌ | 1         |             |
| Warehouse.redshift.maxParallelLoads              | 3             |  ❌          |            |             |
| Warehouse.redshift.columnCountThreshold          | 1200          |  ❌          |            |             |
| Warehouse.redshift.columnCountThreshold          | 1200          |  ❌          |            |             |
| Warehouse.redshift.customDatasetPrefix           |           |  ❌          |            |             |

### SNOWFLAKE

| Config                                   | Default Value | Hot Reloadable | Multiplier | Description |
|------------------------------------------|---------------| --- | --- | --- |
| Warehouse.snowflake.maxParallelLoads     | 3             |  ❌          |            |             |
| Warehouse.snowflake.columnCountThreshold | 1600          |  ❌          |            |             |
| Warehouse.snowflake.customDatasetPrefix  |           |  ❌          |            |             |

### POSTGRES

| Config                                             | Default Value | Hot Reloadable | Multiplier | Description |
|----------------------------------------------------|---------------| --- | --- | --- |
| Warehouse.postgres.skipComputingUserLatestTraits   | false         | ✅ |          |             |
| Warehouse.postgres.txnRollbackTimeout              | 30            | ✅ | 1         |             |
| Warehouse.postgres.enableSQLStatementExecutionPlan | false         | ✅ |          |             |
| Warehouse.postgres.maxParallelLoads                | 3             |  ❌          |            |             |
| Warehouse.postgres.columnCountThreshold            | 1200          |  ❌          |            |             |
| Warehouse.postgres.customDatasetPrefix             |           |  ❌          |            |             |

### DELTALAKE

| Config                                  | Default Value | Hot Reloadable | Multiplier | Description |
|-----------------------------------------| --- | --- |------------| --- |
| Warehouse.deltalake.schema              | default       |  ❌          |            |             |
| Warehouse.deltalake.sparkServerType     | 3             |  ❌          | 1          |             |
| Warehouse.deltalake.authMech            | 3             |  ❌          |            |             |
| Warehouse.deltalake.uid                 | token         |  ❌          |            |             |
| Warehouse.deltalake.thriftTransport     | 2             |  ❌          |            |             |
| Warehouse.deltalake.ssl                 | 1             |  ❌          |            |             |
| Warehouse.deltalake.userAgent           | RudderStack   |  ❌          |            |             |
| Warehouse.deltalake.grpcTimeout         | 2             |  ❌          | 1          |             |
| Warehouse.deltalake.healthTimeout       | 15            |  ❌          | 1          |             |
| Warehouse.deltalake.loadTableStrategy   | MERGE         | ✅           | 1          |             |
| Warehouse.deltalake.maxParallelLoads    | 3             |  ❌          |            |             |
| Warehouse.deltalake.customDatasetPrefix |           |  ❌          |            |             |

### BIGQUERY

| Config                                                   | Default Value | Hot Reloadable | Multiplier | Description |
|----------------------------------------------------------|---------------| --- |-------------| --- |
| Warehouse.bigquery.setUsersLoadPartitionFirstEventFilter | true          | ✅ |         |             |
| Warehouse.bigquery.customPartitionsEnabled               | false         | ✅ |         |             |
| Warehouse.bigquery.isUsersTableDedupEnabled              | false         | ✅ |         |             |
| Warehouse.bigquery.isUsersTableDedupEnabled              | false         | ✅ |         |             |
| Warehouse.bigquery.maxParallelLoads                      | 20            |  ❌          |            |             |
| Warehouse.bigquery.columnCountThreshold                  | 8000          |  ❌          |            |             |
| Warehouse.bogquery.customDatasetPrefix                   |           |  ❌          |            |             |

### Clickhouse

| Config                                           | Default Value | Hot Reloadable | Multiplier | Description |
|--------------------------------------------------|---------------| --- |-------------| --- |
| Warehouse.clickhouse.queryDebugLogs              | false         | ✅ |          |             |
| Warehouse.clickhouse.blockSize                   | 1000000       | ✅ | 1         |             |
| Warehouse.clickhouse.poolSize                    | 100           | ✅ |          |             |
| Warehouse.clickhouse.disableNullable             | false         |  ❌ |         |             |
| Warehouse.clickhouse.readTimeout                 | 300           | ✅ |   |             |
| Warehouse.clickhouse.writeTimeout                | 1800          | ✅ |   |             |
| Warehouse.clickhouse.compress                    | false         | ✅ |   |             |
| Warehouse.clickhouse.execTimeOutInSeconds        | 600           | ✅ | 1   |             |
| Warehouse.clickhouse.commitTimeOutInSeconds      | 600           | ✅ | 1   |             |
| Warehouse.clickhouse.loadTableFailureRetries     | 3             | ✅ | 1 |             |
| Warehouse.clickhouse.numWorkersDownloadLoadFiles | 8             | ✅ | 1   |             |
| Warehouse.clickhouse.maxParallelLoads            | 3             |  ❌          |            |             |
| Warehouse.clickhouse.columnCountThreshold        | 800           |  ❌          |            |             |
| Warehouse.clickhouse.customDatasetPrefix         |           |  ❌          |            |             |

### MSSQL

| Config                               | Default Value | Hot Reloadable | Multiplier | Description |
|--------------------------------------|---------------| --- |-------------| --- |
| Warehouse.mssql.maxParallelLoads     | 3             |  ❌          |            |             |
| Warehouse.mssql.columnCountThreshold | 800           |  ❌          |            |             |
| Warehouse.mssql.customDatasetPrefix  |           |  ❌          |            |             |

### AZURE SYNAPSE

| Config                                   | Default Value | Hot Reloadable | Multiplier | Description |
|------------------------------------------|--------------| --- |-------------| --- |
| Warehouse.azure_synapse.maxParallelLoads | 3            |  ❌          |            |             |
| Warehouse.azure_synapse.customDatasetPrefix |           |  ❌          |            |             |

### GCS DATALAKE

| Config                                     | Default Value | Hot Reloadable | Multiplier | Description |
|--------------------------------------------|---------------| --- |-------------| --- |
| Warehouse.gcs_datalake.customDatasetPrefix |           |  ❌          |            |             |

### AZURE DATALAKE

| Config                                       | Default Value | Hot Reloadable | Multiplier | Description |
|----------------------------------------------|---------------| --- |-------------| --- |
| Warehouse.azure_datalake.customDatasetPrefix |           |  ❌          |            |             |

### S3 DATALAKE

| Config                                      | Default Value | Hot Reloadable | Multiplier | Description |
|---------------------------------------------|---------------| --- |-------------| --- |
| Warehouse.sq_datalake.customDatasetPrefix   |           |  ❌          |            |             |
