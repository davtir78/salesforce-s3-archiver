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

The AWS stack wires this up: an EventBridge rule publishes to an SNS topic whenever a task stops
with a non-zero exit code, and CloudWatch alarms fire on the log lines `CHECKPOINT RESET`,
`QUARANTINED`, `UNAVAILABLE EventLogFile` and `stored replay ID was rejected`. Set `alarm_email`
to receive them.

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

`sf-archive-verify -bucket <bucket> -prefix <prefix>` streams every object and checks it against
its manifest: record count, compressed and uncompressed size, SHA-256 and the first/last
timestamps. It exits non-zero when a manifest does not match, when a manifest's data object is
missing, or when an object could not be read.

A data object with no manifest is reported but does not fail the run: it is normally a duplicate
left behind by a failed manifest upload, which the collector re-archived. Pass `-strict` to fail
on those too. With `-ledger` it also reconciles against the mock org ledger.

On AWS the cache and archive are only reachable from inside the VPC, so run recovery commands as
one-off tasks:

```bash
bash scripts/aws-run-task.sh stream -reset-checkpoints /event/LoginEventStream -confirm
bash scripts/aws-run-task.sh eventlog -once
bash scripts/aws-run-task.sh verify -strict
```
