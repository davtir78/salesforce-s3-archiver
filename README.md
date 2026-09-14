# salesforce-s3-archiver

Archives Salesforce Event Monitoring data to Amazon S3 without silent data loss.

It collects:

- **Real-Time Event Monitoring streams** via the Salesforce Pub/Sub API (`sf-archive-stream`)
- **EventLogFiles**, **custom SOQL query results** and **org limits** via the REST API (`sf-archive-eventlog`)

and writes them to S3 as gzip-compressed NDJSON with a manifest per object.

This project is a hard fork of the Apache-2.0 licensed
[newrelic/newrelic-salesforce-exporter](https://github.com/newrelic/newrelic-salesforce-exporter).
The Salesforce collection code is derived from that project; the New Relic export path was
replaced with an S3 archive and a number of data-loss bugs were fixed (see [below](#changes-from-upstream)).
It is not affiliated with or endorsed by New Relic or Salesforce.

## Guarantees

| Source | Delivery | How |
|---|---|---|
| Pub/Sub streams | At-least-once | The replay checkpoint only advances after events are durably written to S3. Restarts may re-archive a batch; they never skip one. |
| EventLogFiles | Idempotent | One object per file at a deterministic key (`elf-<Id>`). The watermark only moves past archived files. |
| Custom SOQL queries | At-least-once | Watermark advances only after every object is written; overlapping windows are de-duplicated on `Id` + timestamp. |

Downstream consumers should de-duplicate on `EventIdentifier` (streams), `REQUEST_ID`/file Id (EventLogFiles) or `Id` + timestamp (SOQL).

The stream collector **fails closed**: it refuses to start without a checkpoint unless
`initialReplay` is set, stops if Salesforce rejects its stored replay ID (for example it is
older than the retention window), and stops if its checkpoint lease is lost. Silently falling
back to `LATEST` would hide data loss.

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

Stream and SOQL objects are partitioned by ingestion time and use unique names; EventLogFiles
are partitioned by `LogDate` and named `elf-<Id>`. Each NDJSON line contains `eventType`,
`timestamp`, `source`, `orgId`, `instance`, `replayId` (streams) and the original `attributes`.
Manifests record the record count, sizes, SHA-256, first/last timestamp and lineage (topic and
replay ID range, or EventLogFile Id).

Manifests live under a separate prefix so Athena/Glue tables over `raw/` only see data.

## Operations

See [docs/operations.md](docs/operations.md) for metrics, alerts and recovery procedures,
including resetting a checkpoint after Salesforce rejects a replay ID.

## Deploying to AWS (optional)

[deploy/aws](deploy/aws) contains a Terraform test stack: ECS Fargate services, S3 with
SSE-KMS, ElastiCache for Valkey (Multi-AZ, TLS, IAM and password RBAC users) and the mock org.

```bash
bash scripts/aws-deploy.sh      # build, push and apply
bash scripts/chaos-aws.sh       # chaos test with reconciliation
bash scripts/aws-destroy.sh     # tear down
```

To point it at a real org set `use_mock_salesforce=false` and the `salesforce_*` variables.

## Development

```bash
go test ./...                                  # unit, mock-org, crash-matrix and chaos tests
go test -race ./...                            # requires cgo (or run in a golang container)
S3_TEST_ENDPOINT=http://localhost:9000 S3_TEST_BUCKET=archive go test ./internal/archive
REDIS_TEST_HOST=localhost REDIS_TEST_PORT=6379 go test ./internal/checkpoint
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
