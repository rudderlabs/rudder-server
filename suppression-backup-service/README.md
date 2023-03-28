# suppression-backup-service can be used to speed-up loading of suppression regulation at gateway after it's restart. 

## It regularly syncs with suppression-service & then creates latest and full backuped badgerdb, which are served from following endpoints:-
## 1. /full-export
this endpoint returns the full backup of suppression regulation till date.
## 2. /latest-export
As the above data might be really large and a little less relevant, so this endpoint returns suppression data of last 30 days.


## To run this service following envs are requried:-
1. WORKSPACE_TOKEN #workspace secret is required only in case single tenant.
2. HOSTED_SERVICE_SECRET #namespace secret is required only in case of multi-tenant.
3. CONFIG_BACKEND_URL #url to hit for getting config
SUPPRESS_USER_BACKEND_URL #url to hit for getting regulation-manager