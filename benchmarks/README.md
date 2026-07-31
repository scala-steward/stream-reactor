# Sink throughput benchmark: HTTP sink vs GCS sink

Local, network-free, Kafka-free comparison of the HTTP sink and GCS sink connectors'
in-process pipelines. Built to answer a specific customer question: at the same input rate,
why is the HTTP sink slower and more CPU/memory-hungry than the GCS sink, and why did raising
`connect.http.batch.count` (previously recommended) not fix it?

This module is **deliberately excluded** from the root aggregate and from `fullTest`/CI: it is
a local investigation tool, not a shipped connector or a correctness test suite.

**For methodology, results, and the objective conclusion, see
[`EXECUTIVE_SUMMARY.md`](EXECUTIVE_SUMMARY.md).** This README only covers what the module is and
how to run it.

## What "network removed" and "Kafka removed" mean here

- **Kafka removed**: both harnesses hand `SinkRecord`s directly to the real `put()` code path
  (`HttpSinkTask`'s render+enqueue logic for HTTP; the real `GCPStorageSinkTask.put()` for GCS).
  No consumer, broker, or serialization-over-the-wire is involved.
- **Network removed**: the only thing swapped out in each sink is the bottom-most egress seam:
  - HTTP: the `org.http4s.client.Client[IO]` used by `HttpRequestSender` is replaced with a stub
    that returns `200 OK` instantly, or after a configurable simulated latency.
  - GCS: `GCPStorageSinkTask.createStorageInterface` is overridden to return
    [`NoOpGCPStorage`](src/test/scala/io/lenses/streamreactor/connect/benchmarks/gcs/NoOpGCPStorage.scala),
    which succeeds instantly (or after a configurable simulated latency) and never touches disk
    bytes or the network.

Everything above those seams (template rendering, the per-topic `RecordsQueue` and batch policy
evaluation, `HttpSinkMetrics`, `WriterManager`, `IndexManagerV2`, and `JsonFormatWriter`'s
per-record JSON serialisation) is unmodified production code, wrapped by the same decorators
(`Retry`, `RetryingStorageInterface`, `StorageInterfaceWithMetrics`) production uses.

See the scaladoc on
[`HttpSinkThroughputHarness`](src/test/scala/io/lenses/streamreactor/connect/benchmarks/http/HttpSinkThroughputHarness.scala)
and
[`GcsSinkThroughputHarness`](src/test/scala/io/lenses/streamreactor/connect/benchmarks/gcs/GcsSinkThroughputHarness.scala)
for the exact seams, and `EXECUTIVE_SUMMARY.md` for the full methodology (warm-up, iteration
counts, logging suppression, and why).

## Running it

```bash
sbt "project benchmarks" "testOnly *SinkThroughputComparisonTest"
```

This runs four scenarios (pure CPU ceiling at 0ms; egress latency sweep at 100/400/900ms, which
includes a GCS `exactly.once.enable` on/off contrast; record size at 0ms; and a logging-cost
delta) and prints two plain-text tables at the end: measured results (median of several
iterations per scenario) and a GCS-vs-HTTP deviation table. The results table reports both `Ops`
(every latency-charged storage call) and `Flushes` (uploaded data files) so GCS's per-round-trip
amortisation is shown honestly. Expect it to take a few minutes -- most of that time is the
benchmark itself doing real work, not sbt overhead.

To reproduce or extend the analysis, edit
[`SinkThroughputComparisonTest`](src/test/scala/io/lenses/streamreactor/connect/benchmarks/SinkThroughputComparisonTest.scala)
directly; it is the single entry point for all scenarios.
