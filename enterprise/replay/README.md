### ENV Variables

```JSON
# All the JOBS_DB_* are needed to access postgres.
RSERVER_REPLAY_ENABLED = true  # [Mandatory]
WORKSPACE_TOKEN = <workspace_token_to_replay> eg. "1gwssBstpaI6lpfxO7DyGpcayNN" # [Mandatory]
LOG_LEVEL = Logging level. It can be INFO, DEBUG...
RUDDER_TMPDIR = Temp directory to store downloaded gateway dumps.
TO_REPLAY = To Replay should be set to either "gw" or "proc_errors" to be able to restream them. # [Mandatory]
REPLAY_TO_DB = Where should the extracted events ingested to eg. gw, rt # [Mandatory]
S3_DUMPS_BUCKET = S3 bucket to read gateway dumps. Make sure you have read permissions. eg. "rudder-saas"  # [Mandatory]
S3_DUMPS_BUCKET_PREFIX = Prefix inside the bucket if any. Else leave empty. eg. "acloudphigwf" for gw dumps/ "acloudphigwf/proc-err-logs" for proc_errors # [Mandatory]
START_TIME = Time in format RFC3339Milli format. Ex: 2006-01-02T15:04:05.000Z. Jobs with created_at after START_TIME are considered for replay. If S3 object key is not in expected format, S3 objects with last modified time after START_TIME will be considered for replay. eg. "2021-04-22T00:00:00.000Z" # [Mandatory]
END_TIME = Time in format RFC3339Milli format. Jobs with created_at before END_TIME are considered for replay. If S3 object key is not in expected format, S3 objects with last modified time before END_TIME will be considered for replay.  eg. "2021-04-22T00:00:00.000Z" # [Mandatory]
AWS_ACCESS_KEY_ID = AWS access key eg. "AKIAXXXXXXXXX" # [Mandatory]
AWS_SECRET_ACCESS_KEY = AWS secret access key eg. "e7v9K9HXXXXXX"  # [Mandatory]
DB_READ_SIZE = Number of jobs to read from DB. Default 10
WORKERS_PER_SOURCE = Number of workers per source. If you want to maintain order of events during replay, set this to 1. Default 4
TRANSFORMATION_VERSION_ID = Transformer versionId to use for transforming before insert in gw/rt db. Transformer can be created in any workspace. Set it to transformerVersionId (not transformerId) eg. "1tzEGlOLqjdWNkMguiLFvBRi82W" # [Mandatory] in case of inserting to gw db from proc_err to convert event into gw format. Refer below transformation for sample
```

### User Transformation Examples

Sample Transformer to transform event from proc_err dumps and load to router db (to modify auth header set)

```javascript
export function transformEvent(event, metadata) {
  if (event.Parameters.stage !== "router") return;
  if (event.CustomVal !== "MP") return;
  const errorResponse = JSON.stringify(event.Parameters.error_response);

  if (!errorResponse.includes("Missing credentials")) return;
  // mapping of destination id -> auth header
  const authMap = {
    "1kyaeOXtP8XXXXXXX": "Basic NGM2OWUxMTk0YXXXXXXXX",
    "1kWLA6lHgjXXXXXXX": "Basic MTNiNDdlNTRiMXXXXXXXX",
    "1klyBkXzY2XXXXXXX": "Basic MjlhOTE3NWFiXXXXXXXXXX",
  };
  event.EventPayload.headers.Authorization =
    authMap[event.Parameters.destination_id];
  return event;
}
```

Sample Transformer to transform event from proc_err dumps and load to gw db (to route events only to Redshift destinations)

```javascript
const sourceIDMap = {
  "1dp2WiXXXXXXXXXXXX": "1jtB5HAXXXXXXXXXXX",
  "1gPgj1XXXXXXXXXXXX": "1gPgj2nXXXXXXXXXXX",
  "1lA2N6XXXXXXXXXXXX": "1lA2N2nXXXXXXXXXXX",
  "1leGCoXXXXXXXXXXXX": "1leGCulXXXXXXXXXXX",
};

export function transformEvent(event, metadata) {
  if (event.Parameters.stage == "router") return;
  event.EventPayload.forEach((o) => {
    o.integrations = { All: false, Redshift: true };
  });
  const writeKey = sourceIDMap[event.Parameters.source_id];
  if (!writeKey) return;
  event = {
    EventPayload: {
      batch: event.EventPayload,
      writeKey: sourceIDMap[event.Parameters.source_id],
      receivedAt: event.EventPayload[0].receivedAt,
    },
    Parameters: event.Parameters,
    UserID: event.UserID,
    CustomVal: "GW",
  };
  return event;
}
```

Sample Transformer to transform event from gw dumps (to route events only to MixPanel destinations)

```javascript
export function transformEvent(event, metadata) {
  if (event.event_payload && event.event_payload.batch) {
    event.event_payload.batch.forEach((o) => {
      o.integrations = { All: false, MP: true };
    });
  }
  return event;
}
```
