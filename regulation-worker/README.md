Set the following appropriately before running regulation-worker:-

1. CONFIG_BACKEND_URL
2. WORKSPACE_TOKEN (workspace secret: only required for single-tenant)
3. WORKSPACE_NAMESPACE (namespace secret: only required for multi-tenant)
4. DEST_TRANSFORM_URL (transformer url required to make downstream API call to destionations of API type.)