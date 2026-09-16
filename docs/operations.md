# Operations

## Deployment rules

- Run **one** stream collector per org/topic set. Use `Recreate`-style deployments
  (ECS: `minimumHealthyPercent=0`, `maximumPercent=100`). The checkpoint lease makes an
  accidental second instance wait rather than race, but it still delays recovery.
- After a hard crash the replacement waits up to `leaseTtlSeconds` for the old lease to expire.
- On SIGTERM the stream collector flushes buffered events and commits the checkpoint, allowing
  `shutdownFlushTimeoutSeconds` (default 90) before giving up without advancing it. Set the
  orchestrator stop timeout above that (ECS `stopTimeout` 120, compose `stop_grace_period: 120s`)
  so the process is not killed mid-flush.
- Redis/ElastiCache: `maxmemory-policy noeviction`, TLS, Multi-AZ. Checkpoints never expire.

## Metrics

Both collectors serve Prometheus metrics on `metricsAddr` (`/metrics`, `/healthz`, `/readyz`).

| Metric | Meaning | Suggested alert |
|---|---|---|
| `sfarchive_stream_last_committed_event_timestamp_seconds{topic}` | Event time covered by the checkpoint. `time() - value` is the replay age. | Warn well inside the Pub/Sub retention window (e.g. > 12h), page before it (e.g. > 24h). |
| `sfarchive_stream_last_commit_timestamp_seconds{topic}` | Last checkpoint commit, including keepalive commits on idle topics. | No commit for > 15 minutes. |
| `sfarchive_stream_checkpoint_failures_total{topic,op}` | Checkpoint load/commit/renew failures. | Any increase. |
| `sfarchive_upload_failures_total{source}` | Failed archive write attempts. | Sustained increase. |
| `sfarchive_stream_reconnects_total{topic,reason}` | Subscription reconnects. | Reconnect loops. |
| `sfarchive_stream_buffered_events{topic}` | Events not yet archived. | Growing without commits. |
| `sfarchive_watermark_timestamp_seconds{name}` | EventLogFile and custom query watermarks. | Watermark age > 3 poll intervals + expected ELF delay. |
| `sfarchive_eventlog_last_successful_poll_timestamp_seconds` | Last poll without errors. | No success for > 1 hour. |
| `sfarchive_eventlog_failures_total{stage}` | list/download/archive/query/limits failures. | Sustained increase. |

A stream collector that exits with an error is **meant** to be restarted by the orchestrator.
Alert on restart loops: they indicate a condition that needs a human (see below).

## Recovery procedures

### Stream collector refuses to start: "no replay checkpoint exists"

First deployment, or the checkpoint was deleted. Decide deliberately:

- `initialReplay: EARLIEST` archives everything still retained by Salesforce (duplicates possible).
- `initialReplay: LATEST` starts from now (anything before is not archived by the stream).

Set it, start the collector, and remove it again once checkpoints exist.

### "stored replay ID was rejected by Salesforce; events may have been lost"

The checkpoint is older than the retention window (collector down too long) or no longer valid
(e.g. sandbox refresh). Events between the checkpoint and the oldest retained event are gone
from the stream.

1. Stop the stream collector.
2. Note the gap start: the `previousReplayId` logged by the reset below and the
   `lastTimestamp` of the newest stream manifest for the topic.
3. Reset: `sf-archive-stream -config stream.yml -reset-checkpoints <topic>|all -confirm`
4. Start with `initialReplay: EARLIEST`.
5. Backfill the gap from stored event objects (for example `LoginEvent`, `ApiEvent` via SOQL
   custom queries) or EventLogFiles where available.

### Checkpoint lease lost

Another collector took over the topic, or Redis was unreachable for longer than the lease TTL.
The collector stops without moving the checkpoint; the orchestrator restarts it. If it keeps
happening, look for a second deployment or Redis instability.

### S3 unavailable

Writes are retried (`maxAttempts`, `uploadTimeoutSeconds`), then the stream collector stops
without moving its checkpoint and the event log collector leaves its watermark in place. Both
resume once S3 is reachable. Data is re-read from Salesforce, so outages must stay shorter than
Salesforce retention.

### EventLogFile backfill

1. Stop the event log collector.
2. Delete the watermark key `<instanceName>_last_run_ts` (or set it to the epoch-millis start).
3. Optionally delete processed markers `<instanceName>_elf_*` (reprocessing is idempotent:
   each file overwrites its own object).
4. Set `initialTimeInterval` to cover the gap and start the collector.

### Custom query backfill

Delete the query watermark key `<instanceName>_query_<hash>_last_run_ts` and set
`initialTimeInterval`. Rows already archived are skipped while their de-duplication markers exist.

## Verifying an archive

`sf-archive-verify -bucket <bucket> -prefix <prefix>` checks every manifest against its object
(record count, size, SHA-256) and exits non-zero on mismatches or objects without manifests.
With `-ledger` it also reconciles against the mock org's ledger.

## Upgrading from earlier builds

These changes affect a deployment that already has data in S3 or state in
Redis. A new deployment can ignore this section.

- **Record envelope (manifest version 2).** Every line is now an envelope with
  the Salesforce record under `payload`. Objects written by earlier builds have
  `manifestVersion: 1` and the old line format. Keep them under a separate
  prefix (or delete them if they were test data) rather than mixing the two
  formats under one Athena table.
- **`eventLog.instanceName` is required.** Cache keys are namespaced by it, so a
  config without one is rejected at startup. Set it to the name the instance
  had before, or its watermarks will not be found.
- **Custom query state is re-keyed.** De-duplication markers were keyed by
  object (`<instanceName>_soql_<object>_…`) and are now keyed by a hash of the
  query, so two queries on one object no longer share them. The watermark key
  `<instanceName>_query_<hash>_last_run_ts` keeps its form, but the hash now
  treats select fields as a set, so it changes too. On the first poll after
  upgrading, each custom query starts from `initialTimeInterval` and rows in
  that window are archived once more. The duplicates carry the same `event_id`,
  so queries can de-duplicate on it; the new watermark key is shown in the
  debug-level "Custom query on …" log line.
- **`cache.redis.keyPrefix` now applies to every key.** It previously applied
  only to stream checkpoints. If you set it, event log watermarks and markers
  move under the prefix: rename the existing `<instanceName>_*` keys to
  `<keyPrefix><instanceName>_*`, or expect a re-collection over
  `initialTimeInterval`.
- **Custom queries with `endTimestamp`** now select on the end field alone, so
  records that started before the window and finished inside it are archived.
- **403 responses no longer tombstone EventLogFiles.** Only 400, 404 and 410
  count towards `unavailableFileAttempts`; 403 (including
  `REQUEST_LIMIT_EXCEEDED`) keeps blocking until it clears.
