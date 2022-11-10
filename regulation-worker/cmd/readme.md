## To run regulation-worker in multi-tenant mode. 
Set env `DEPLOYMENT_TYPE` to "MULTITENANT", default mode is "DEDICATED".

## Multitenant Mode

# Compulsory Envs
1. NAMESPACE=<namespaceID>
2. WORKSPACE_NAMESPACE=<namespaceSecret>
3. DEST_TRANSFORM_URL=<transformer URL> This is required in case of API deletion job.

# Optinal Envs
1. CONFIG_BACKEND_URL = <configBackendURL> default is https://api.rudderlabs.com
2. CP_ROUTER_URL = <cpRouterURL> default is https://cp-router.rudderlabs.com

## Dedicated Mode

# Compulsory Envs
1. CONFIG_BACKEND_TOKEN = <workspace Token>


## Batch Destination Envs
1. REGULATION_WORKER_FILES_LIMIT=<value> default: 1000, this is used to limit number of files to be listed at a time by an object storage manager.

## API Destination Envs
<!-- Use this to configure transformer Request timeout -->
1. HttpClient.regulationWorker.transformer.timeout=<value in second> default value is 60 second. 