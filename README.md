# salesforce-s3-archiver

Archives Salesforce Event Monitoring data to Amazon S3 as an immutable, queryable record,
without silent data loss.

It collects:

- **Real-Time Event Monitoring streams** via the Salesforce Pub/Sub API (`sf-archive-stream`)
- **EventLogFiles**, **custom SOQL query results** and **org limits** via the REST API (`sf-archive-eventlog`)

and writes them to S3 as gzip-compressed NDJSON, one object per batch or file, each with a manifest.

## Why this fork exists

This is a hard fork of the Apache-2.0 licensed
[newrelic/newrelic-salesforce-exporter](https://github.com/newrelic/newrelic-salesforce-exporter),
which sends Salesforce logs to New Relic for monitoring. The Salesforce-facing code (auth,
Pub/Sub client, EventLogFile and SOQL queries) is derived from it. It is not affiliated with or
endorsed by New Relic or Salesforce.

The goals are different, and that difference matters:

- **Monitoring tolerates gaps; an audit archive does not.** Upstream advanced its replay
  checkpoint as each event arrived, before the event was exported, and discarded a whole batch
  (logged at debug level) when export failed. Both lose events permanently. Here the checkpoint
  only moves after the data is durably in S3.
- **The destination is object storage, not a telemetry API.** The New Relic export path is
  replaced with an S3 archive: partitioned keys, a manifest with a SHA-256 per object, and
  server-side encryption. Upstream also refused to start without a New Relic licence key.
- **Fail closed rather than carry on.** A missing or rejected replay checkpoint stops the
  collector instead of silently restarting from "now", which would leave a gap nobody notices.
- **Keep the data as Salesforce sent it.** Upstream truncated strings at 4096 characters,
  converted numeric-looking values and dropped Avro fields it did not recognise.

Other fixed data-loss bugs (descriptor leak, ignored pagination, watermarks advancing past
failures) are listed under [Changes from upstream](#changes-from-upstream).

## Guarantees

| Source | Delivery | How |
|---|---|---|
| Pub/Sub streams | At-least-once | The replay checkpoint only advances after events are durably written to S3. Restarts may re-archive a batch; they never skip one. |
| EventLogFiles | Idempotent | One object per file at a deterministic key (`elf-<Id>`). The watermark only moves past archived files. |
| Custom SOQL queries | At-least-once | The watermark advances only after every object is written; overlapping windows are de-duplicated on `Id` + timestamp. |

Duplicates are possible, loss is not. De-duplicate downstream on `event_id`.

The stream collector **fails closed**. It refuses to start without a checkpoint unless
`initialReplay` is set, stops if Salesforce rejects its stored replay ID (for example it is older
than the retention window), stops if its checkpoint lease is lost, and stops if it cannot write to
S3. Each case is a condition a human needs to see; silently continuing would hide a gap.

Two limits worth knowing:

- Recovery depends on Salesforce retention. An outage longer than the Pub/Sub retention window
  cannot be replayed; back-fill from EventLogFiles or stored event objects instead.
- `initialReplay: LATEST` cannot guarantee events between subscribing and the first checkpoint.
  Bootstrap with `EARLIEST` when that matters.

## The record envelope

Every line is a small envelope around the source record, unchanged, in `payload`:

```json
{"event_id":"9b2c7f4e","event_type":"LoginEventStream","timestamp":"2026-09-15T01:02:03.004Z",
 "source":"stream","env":"prod","org_id":"00D...","instance":"myorg-prod","replay_id":"AAAAAAAAAmI=",
 "payload":{"EventUuid":"9b2c7f4e","UserId":"005...","SourceIp":"10.0.4.7","...":"..."}}
```

| Field | Meaning |
|---|---|
| `event_id` | Salesforce event UUID (streams), record Id plus its timestamp values, `<Id>@<timestamp>` (SOQL, so each archived version of a changed record is distinct), or a hash of file Id + line number (EventLogFile rows, which have no unique field of their own). The de-duplication key. |
| `event_type` | `LoginEventStream`, `Login`, `SetupAuditTrail`, ... |
| `timestamp` | Event time from the source, UTC |
| `source` | `stream`, `eventlog`, `soql` or `limits` |
| `env` | Deployment that collected the record, from config |
| `org_id`, `instance` | Salesforce org, and the collector instance name |
| `replay_id` | Pub/Sub replay position (streams only) |
| `payload` | The original record, unchanged |

Envelope keys are snake_case; payload keys keep the Salesforce spelling (`USER_ID`, `SourceIp`).
Each object also has a manifest under a separate `manifests/` prefix recording the count, sizes,
SHA-256, time range and lineage (topic and replay range, or EventLogFile Id). That manifest is
what `sf-archive-verify` checks, and what gives any row a chain of custody back to an immutable
object.

## Why records are schema-neutral

The collector never imposes a schema on the payload. It does not rename, reorder, coerce or drop
fields, and it needs no field list per event type.

- **Salesforce changes its schemas.** Event types gain fields every release, and each of the ~50
  EventLogFile types has its own columns. Anything that enumerated fields would silently drop new
  ones, which is exactly the upstream bug where every non-union Avro field was discarded.
- **Values keep their original form.** CSV columns stay strings (`"157"`, not `157`), so leading
  zeros, large IDs and oversized values survive; SOQL numbers keep full precision instead of
  becoming float64; nothing is truncated.
- **The evidence should be the raw record.** For an audit or investigation a normalised copy is a
  derived artefact. Normalisation belongs downstream, where it can be changed and re-run without
  re-reading Salesforce.
- **Schema-on-read fits the query engine.** Athena and Glue can read the payload as a map, or
  project only the columns of interest per event type, without the archive guessing in advance
  which fields matter.

The envelope carries the small, stable set of fields that have to be consistent to query across
sources at all.

## Querying with Athena

The layout is designed for Athena over the `raw/` prefix (manifests live elsewhere, so they are
never read as data):

```text
<prefix>/raw/source=<stream|eventlog|soql|limits>/org_id=<org>/event_type=<type>/year=YYYY/month=MM/day=DD/hour=HH/<name>.json.gz
```

Those are Hive-style partition keys, so a table with partition projection needs no crawler. Read
the payload as a map and let each query pick out the fields it needs:

```sql
CREATE EXTERNAL TABLE salesforce_archive (
  event_id   string,
  event_type string,
  ts         timestamp,
  source     string,
  env        string,
  org_id     string,
  instance   string,
  replay_id  string,
  payload    map<string,string>
)
PARTITIONED BY (year string, month string, day string, hour string)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('mapping.ts' = 'timestamp')
LOCATION 's3://my-archive/salesforce/raw/source=eventlog/org_id=00D.../event_type=Login/'
TBLPROPERTIES (
  'projection.enabled' = 'true',
  'projection.year.type' = 'integer',  'projection.year.range' = '2026,2030',
  'projection.month.type' = 'integer', 'projection.month.range' = '01,12', 'projection.month.digits' = '2',
  'projection.day.type' = 'integer',   'projection.day.range' = '01,31',   'projection.day.digits' = '2',
  'projection.hour.type' = 'integer',  'projection.hour.range' = '00,23',  'projection.hour.digits' = '2'
);
```

Typical investigator queries:

```sql
-- Everything one user did on one day
SELECT ts, event_type, payload['CLIENT_IP'], payload['URI']
FROM salesforce_archive
WHERE year = '2026' AND month = '09' AND day = '15'
  AND payload['USER_ID'] = '005xx000001Sv6AAAS'
ORDER BY ts;

-- Failed logins by source IP within an hour
SELECT payload['CLIENT_IP'] AS ip, count(*) AS attempts
FROM salesforce_archive
WHERE year = '2026' AND month = '09' AND day = '15' AND hour = '02'
  AND payload['LOGIN_STATUS'] <> 'LOGIN_NO_ERROR'
GROUP BY 1
ORDER BY attempts DESC;
```

Three things to know before scaling this up:

- **Always filter on the partition columns.** Without them Athena scans the whole archive, and
  cost grows with the archive rather than with the query.
- **Partitions are ingestion time for streams and SOQL**, and `LogDate` for EventLogFiles. Event
  time lives in `timestamp`. A question about "what happened on the 15th" should filter the
  partitions generously, then filter on `timestamp`.
- **Convert to Parquet for regular use.** JSON is the evidence copy: complete, immutable and slow
  to scan. A compaction job into Parquet, partitioned by event date, is the intended path for
  day-to-day analysis and is not built yet.

## Quick start (Docker, no Salesforce org needed)

The repository includes a mock Salesforce org (OAuth, REST, Pub/Sub gRPC) so everything can be
run and tested locally.

```bash
sh scripts/dev-certs.sh                                   # TLS certs for local Valkey
docker compose -f deploy/local/docker-compose.yml up -d --build
curl -XPOST localhost:18080/admin/generator -d '{"eventsPerSecond":50,"eventLogFileEverySeconds":30}'
```

- MinIO console: http://localhost:19001 (`minioadmin` / `minioadmin`), bucket `archive`
- Metrics: http://localhost:19091/metrics (stream), http://localhost:19092/metrics (event log)

Run the chaos test (kills collectors, restarts Valkey, pauses S3, injects Salesforce faults,
then reconciles every generated record against the archive):

```bash
DURATION=300 bash scripts/chaos-local.sh
```

## Configuration

See [config_sample_eventstream.yml](config_sample_eventstream.yml) and
[config_sample_eventlog.yml](config_sample_eventlog.yml). Values of the form `$VAR` are read
from environment variables.

```bash
sf-archive-stream   -config stream.yml
sf-archive-eventlog -config eventlog.yml          # add -once for scheduled tasks / CronJobs
```

### Redis / ElastiCache

Redis (or Valkey) stores replay checkpoints, watermarks, tokens and de-duplication markers.
It is **required** for the stream collector.

- TLS (`tls.enabled`, optional `caFile`, `serverName`)
- Password / AUTH token, or RBAC `username` + `password`
- **ElastiCache IAM authentication** (`iamAuth`), for replication groups and serverless caches
- Cluster mode (`mode: cluster`)
- Timeouts and retries

Checkpoints and watermarks are written without a TTL; `expireDays` only applies to tokens and
de-duplication markers. Configure the server with `maxmemory-policy noeviction`.

Each topic's checkpoint is protected by a lease: if two collectors for the same topic run at
once (for example during a node partition) the second waits, and a collector that loses its
lease stops before it can move the checkpoint.

### S3 layout

```text
<prefix>/raw/source=<stream|eventlog|soql|limits>/org_id=<org>/event_type=<type>/year=YYYY/month=MM/day=DD/hour=HH/<name>.json.gz
<prefix>/manifests/<same partition path>/<name>.manifest.json
```

Stream and SOQL objects are partitioned by ingestion time and use unique names; EventLogFiles are
partitioned by `LogDate` and named `elf-<Id>`, so reprocessing a file overwrites its own object.
See [the record envelope](#the-record-envelope) for the line format and
[querying with Athena](#querying-with-athena) for the table definition.

## Operations

See [docs/operations.md](docs/operations.md) for metrics, alerts and recovery procedures,
including resetting a checkpoint after Salesforce rejects a replay ID.

## Deploying to AWS (optional)

[deploy/aws](deploy/aws) contains a Terraform test stack: ECS Fargate services, S3 with
SSE-KMS, ElastiCache for Valkey (Multi-AZ, TLS, IAM and password RBAC users) and the mock org.

```bash
MOCK=1 bash scripts/aws-deploy.sh    # build, push and apply the mock test stack
bash scripts/chaos-aws.sh            # chaos test with reconciliation
bash scripts/aws-run-task.sh verify -strict          # one-off task inside the VPC
CONFIRM_DESTROY=sfarchive-test bash scripts/aws-destroy.sh   # tear down (FORCE_DESTROY_BUCKET=true also deletes archived data)
```

For a real org, put the settings in `deploy/aws/terraform.tfvars` (gitignored) and the secret in
`TF_VAR_salesforce_client_secret`; the deploy script refuses to run without an explicit mode, so
it cannot silently point a real deployment back at the mock.

Worth setting for production: `object_lock_mode` (write-once retention for archived evidence),
`alarm_email` (task failures and the log alarms in [docs/operations.md](docs/operations.md)), and
a remote Terraform backend, since local state holds the generated cache secrets. See
[deploy/aws/backend.tf.example](deploy/aws/backend.tf.example).

Before enabling Object Lock, note how it interacts with the rest of the stack:

- It can only be chosen when the bucket is created. Changing `object_lock_mode` later replaces
  the bucket.
- `COMPLIANCE` retention cannot be shortened or bypassed by anyone, including the account root.
  `terraform destroy` (and `aws-destroy.sh`, even with `FORCE_DESTROY_BUCKET=true`) fails while
  any object is still retained, so try the stack with `GOVERNANCE` or a short
  `object_lock_days` first.
- EventLogFile objects have deterministic keys (`elf-<Id>`), so a backfill that reprocesses a
  file writes a new version and the previous one becomes a noncurrent version. Under Object
  Lock that noncurrent version is also retained for `object_lock_days`, and is billed for that
  period.

To share one cache between deployments, set `cache_key_prefix` (for example `prod:`). It sets
`cache.redis.keyPrefix` in both collector configs and the Valkey ACL key patterns together; set
by hand, a prefix the ACLs do not allow would deny every cache operation.

## Development

```bash
go test ./...                                  # unit, mock-org, crash-matrix and chaos tests
go test -race ./...                            # requires cgo (or run in a golang container)
S3_TEST_ENDPOINT=http://localhost:9000 S3_TEST_BUCKET=archive go test ./internal/archive
REDIS_TEST_HOST=localhost REDIS_TEST_PORT=6379 go test ./internal/checkpoint ./internal/cache/...
```

| Path | Purpose |
|---|---|
| `cmd/sf-archive-stream` | Pub/Sub stream collector |
| `cmd/sf-archive-eventlog` | EventLogFile, SOQL and limits collector |
| `cmd/sf-archive-verify` | Reconciles an S3 archive against manifests and the mock ledger |
| `cmd/mock-salesforce` | Mock Salesforce org for local and CI testing |
| `internal/archive` | S3, local and in-memory archive sinks |
| `internal/checkpoint` | Leased replay checkpoint store |
| `internal/integration/stream` | Subscriber (archive, then checkpoint) |
| `internal/integration/eventlog` | EventLogFile / SOQL collector |
| `internal/mocksf` | Mock org implementation |

The mock is modelled on public Salesforce documentation. Validate authentication, Pub/Sub
error codes and limits against a real org (a free Developer Edition org is sufficient) before
production use.

## Changes from upstream

Stream collector:

- The replay ID was persisted as each event arrived, before it was exported: a crash lost events.
- Export errors were logged at debug level and the batch discarded.
- Checkpoint keys expired after `expireDays`; a missing checkpoint silently fell back to `LATEST`.
- Non-union Avro fields other than `CreatedDate`/`CreatedById` were dropped.
- Redis TLS was not supported.

Event log collector:

- Downloaded CSV files were never closed (file descriptor leak).
- The watermark jumped to the newest listed file even when downloads or exports failed, and
  results were not ordered.
- Pagination (`nextRecordsUrl`) was ignored, so rows after the first page were dropped.
- Custom query watermarks advanced when queries failed; query upper bounds had sub-second
  precision that could skip boundary records.
- Strings were truncated to 4096 characters and numeric-looking values converted.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
