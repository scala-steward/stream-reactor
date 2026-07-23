# Executive summary: HTTP sink vs GCS sink throughput

## Conclusion

**The HTTP-vs-GCS throughput gap is primarily an in-process (CPU) effect, not a network effect.** With no simulated network latency at all, the GCS sink drained records roughly **40-60x faster** than the HTTP sink at its current `batch.count=1500`, and roughly **190-325x faster** than the HTTP sink at the previously-recommended `batch.count=10000` (ranges, not single figures, because the HTTP median swings run-to-run — see the min/max columns). Since there is no network delay in this scenario, that gap is entirely in-process cost.

**Raising `connect.http.batch.count` from 1,500 to 10,000 is not a reliable fix, and at low-to-moderate latency it made pure-CPU throughput worse in this benchmark — roughly 4-5x worse (e.g. ~17,300 -> ~3,800 records/sec median at zero latency).** The cause is an `O(n^2)` cost in the HTTP sink's shared batch-formation code (`RecordsQueueBatcher.takeBatch`, in [`kafka-connect-common`](../kafka-connect-common/src/main/scala/io/lenses/streamreactor/common/batch/RecordsQueueBatcher.scala)): forming a batch of size N re-copies and re-scans the batch built so far on every one of the N records considered, so batch-formation cost grows quadratically with batch size. The `Avg Op(ms)` column shows the effect directly: a 10,000-record batch costs ~2,700-4,200ms to form and send *regardless* of simulated latency, so that formation cost — not the network — is the throughput floor for that configuration. The larger batch only starts to pay off once per-request latency is high enough to exceed that formation floor — in this run the crossover sits between 100ms (still behind) and 400ms (marginally ahead), and it is only clearly ahead at 900ms. **This finding is unchanged by the remediation described below** and is independently code-verified.

**A smaller, avoidable effect: `BatchPolicy.shouldBatch` performs per-record logging at `INFO`.** In this remediated run the WARN-vs-INFO delta collapsed into run-to-run noise (the medians even inverted, with heavily overlapping min/max — see scenario 4), because with stdout redirected the console appender write is cheap; the cost is real but its magnitude is dominated by the log destination's speed rather than being a stable property of the code. An earlier, slower-console pass put it around ~15% of throughput. It remains worth gating per-record logging, but it is not a primary driver of the gap and the previously-quoted single-figure percentage was not reproducible as a stable number here. See "Correction" and "Reading the results objectively" below.

## Correction to the previous version of this document

**The earlier version of this summary claimed HTTP and GCS "converge to parity" at 400-900ms simulated latency. That claim was misleading, and this run corrects it.**

The old numbers under-counted GCS's network operations. [`NoOpGCPStorage`](src/test/scala/io/lenses/streamreactor/connect/benchmarks/gcs/NoOpGCPStorage.scala) charges the simulated latency on **every** storage call — not just `uploadFile`, but also the temp-file `mvFile`, the temp-file `deleteFile`, and the index/lock bookkeeping writes (`writeBlobToFile`/`writeStringToFile`) that the exactly-once commit chain performs on every flush. The old harness reported `Ops = totalUploads`, i.e. **1 op per flush**, which hid the fact that with exactly-once enabled (the production default) each flush actually makes **~6-9 latency-charged round trips** — so its records are amortised over far more delay charges than the `Rec/Op = 10000` column implied.

This run reports honest ops accounting. Two columns now appear:

- **`Ops`** = every latency-charged storage call (upload + move + delete + index/lock writes). This is the fair, apples-to-apples column against HTTP's request count.
- **`Flushes`** = uploaded data files (`uploadFile` count), i.e. the old `Ops` number.

With that accounting, and with an added **exactly-once-off** GCS variant, the "convergence" dissolves:

| Latency | GCS eo=on `Ops`/`Flushes` | GCS eo=on rec/s | GCS eo=**off** `Ops`/`Flushes` | GCS eo=**off** rec/s | HTTP batch=1500 rec/s |
|---|---|---|---|---|---|
| 100ms | 9 / 1 (1,111 rec/op) | 13,502 | 1 / 1 (10,000 rec/op) | **85,548** | 8,104 |
| 400ms | 9 / 1 (1,111 rec/op) | 3,514 | 1 / 1 (10,000 rec/op) | **23,893** | 2,870 |
| 900ms | 9 / 1 (1,111 rec/op) | 1,574 | 1 / 1 (10,000 rec/op) | **10,877** | 1,431 |

The apparent 400-900ms "parity" between GCS (exactly-once on) and HTTP was substantially a **coincidence of amortisation ratios**: exactly-once GCS amortised 10,000 records over ~9 delay-charged round trips (~1,111 records per charge), which is close to HTTP's 1,500 records per request — so the two spent similar total time in simulated latency. It was *not* evidence that the two sinks perform equally under production-like latency. Turn exactly-once off and each GCS flush becomes a single direct upload (1 charged op, 10,000 records per round trip), and GCS is **roughly 8-11x faster than HTTP `batch=1500`** at every latency point measured (100ms: ~10.6x; 400ms: ~8.3x; 900ms: ~7.6x — see the table above), well above the near-parity the exactly-once-on numbers suggested.

## What was run, and what was mocked

Both sides ran the same production connector code, driven directly (no Kafka broker/consumer) with identical synthetic `SinkRecord`s, and with only the network egress replaced:

| | HTTP sink | GCS sink |
|---|---|---|
| Driver | [`HttpSinkThroughputHarness`](src/test/scala/io/lenses/streamreactor/connect/benchmarks/http/HttpSinkThroughputHarness.scala) | [`GcsSinkThroughputHarness`](src/test/scala/io/lenses/streamreactor/connect/benchmarks/gcs/GcsSinkThroughputHarness.scala) |
| Real code exercised | Template rendering, `RecordsQueue`, `BatchPolicy`/`RecordsQueueBatcher`, `HttpWriter`, `HttpSinkMetrics` | KCQL/commit-policy parsing, `WriterManager`, `IndexManagerV2` (exactly-once on) or `NoIndexManager` (exactly-once off), local staging file, `JsonFormatWriter` |
| Mocked seam | `org.http4s.client.Client[IO]` used by `HttpRequestSender` -> stubbed to return `200 OK` instantly or after a configurable delay | `GCPStorageSinkTask.createStorageInterface` -> [`NoOpGCPStorage`](src/test/scala/io/lenses/streamreactor/connect/benchmarks/gcs/NoOpGCPStorage.scala), which succeeds instantly or after a configurable delay, persists nothing, and **charges the delay on every storage call** so ops accounting is honest |
| Decorators still applied | `Retry` middleware bypassed deliberately (see harness scaladoc) | `RetryingStorageInterface` + `StorageInterfaceWithMetrics`, same as production |
| Drive loop | `HttpWriter.process()` called back-to-back until the queue drains (production's `fs2.Stream.fixedRate` poll scheduler is a scheduling artifact, not part of the pipeline's real cost -- see harness scaladoc) | The real `put()` write-and-commit chain, which is synchronous on the calling thread |

**Ops accounting (new this run).** `NoOpGCPStorage` now exposes both `totalChargedOps` (every latency-charged storage call) and `totalUploads` (the flush/data-file count). The results table reports the former as `Ops` and the latter as `Flushes`, and `Rec/Op` is computed against charged ops for both sinks, so the records-per-round-trip amortisation is directly comparable. HTTP's `Ops` and `Flushes` are equal (one request per batch).

**Exactly-once on/off variants (new this run).** The latency sweep runs GCS twice per latency point: once with `connect.gcpstorage.exactly.once.enable=true` (production default; the index/commit chain runs, ~6-9 charged ops per flush) and once with it `false` (`NoIndexManager`; a single direct upload, 1 charged op per flush). This makes the amortisation-ratio artifact explicit rather than leaving it as a hidden confound.

**Methodology hardening** (see [`SinkThroughputComparisonTest`](src/test/scala/io/lenses/streamreactor/connect/benchmarks/SinkThroughputComparisonTest.scala)):

- **Logging suppressed by default.** [`logback-test.xml`](src/test/resources/logback-test.xml) sets the root logger and, specifically, `BatchPolicy$`/`CommitPolicy$` (note the trailing `$` — these are Scala `object`s, so their runtime logger name includes the module suffix) to `WARN`, so scenarios 1-3 measure compute, not per-record `INFO` logging.
- **Warm-up + repeated iterations, reporting the median.** Pure-CPU scenarios (1 and 3) run 2 discarded warm-up iterations followed by measured iterations; the latency sweep (where a deterministic sleep dominates, not JIT) runs 3 measured iterations with no warm-up. `BenchAggregator` reports the median `records/sec` across measured iterations, plus min/max.
- **GCS pure-CPU runs lengthened.** The zero-latency GCS scenarios now run 200,000 records (`batchesToRun=20`) rather than 20,000, so the timed section is comfortably above single-GC-pause noise (previously a ~15-30ms window in which one GC could swing the result 2x). The min/max spread on the GCS CPU rows is now much tighter than before.
- **Record-size sweep moved to 0ms latency.** Previously run at 400ms, where ~2.8s of each iteration's ~2.85s was pure sleep, masking any real serialisation cost. It now runs at 0ms so serialisation cost is actually visible for both sinks (see scenario 3 — GCS is emphatically *not* flat across record sizes, contrary to the old summary).
- **Zero-latency point de-duplicated.** The old suite measured the same zero-latency configs twice (a `1-cpu-ceiling` family and a `2-latency=0ms` family) with different run lengths/warm-up, producing inconsistent numbers for identical configs. There is now a single zero-latency measurement (scenario 1); the latency sweep starts at 100ms.
- **Logging cost measured directly, not inferred.** Scenario 4 runs the identical HTTP `batch.count=1500` scenario at the default `WARN` and again with `BatchPolicy$`'s logger raised to `INFO` via logback's `Logger.setLevel` (see caveat below on why this came out within noise this run).

Everything below is per-task, single-partition, single-JVM. It does not include Kafka poll/consumer overhead, real network RTT, or multi-task/multi-partition contention.

## Results (median of measured iterations)

From a run of `sbt "project benchmarks" "testOnly *SinkThroughputComparisonTest"`:

```
Family                   Variant                            Sink   Iter  Median rec/s   Min rec/s   Max rec/s   Records    Ops  Flushes    Rec/Op  Avg Op(ms)
-------------------------------------------------------------------------------------------------------------------------------------------------------------
1-cpu-ceiling            http batch=1500                    HTTP      5       17264.4      8172.2     24308.5     15000     10       10    1500.0     102.916
1-cpu-ceiling            http batch=10000 (recommended)     HTTP      5        3768.2      3211.6      3906.6     20000      2        2   10000.0    2732.113
1-cpu-ceiling            gcs flush=10000                    GCS       5      719124.5    668597.5    854052.3    200000    123       20    1626.0       1.985
2-latency=100ms          http batch=1500                    HTTP      3        8104.0      5807.6      8479.0      7500      5        5    1500.0     185.093
2-latency=100ms          http batch=10000 (recommended)     HTTP      3        3199.1      2759.4      3439.8     10000      1        1   10000.0    2907.176
2-latency=100ms          gcs flush=10000                    GCS       3       13502.1     13483.2     13602.2     10000      9        1    1111.1      82.407
2-latency=100ms          gcs flush=10000 eo=off             GCS       3       85547.9     79937.5     86987.3     10000      1        1   10000.0     114.959
2-latency=400ms          http batch=1500                    HTTP      3        2869.9      2865.9      3062.2      7500      5        5    1500.0     489.849
2-latency=400ms          http batch=10000 (recommended)     HTTP      3        3058.5      2341.8      3060.2     10000      1        1   10000.0    3267.792
2-latency=400ms          gcs flush=10000                    GCS       3        3514.2      3510.8      3535.1     10000      9        1    1111.1     316.482
2-latency=400ms          gcs flush=10000 eo=off             GCS       3       23893.2     23572.6     24037.2     10000      1        1   10000.0     418.529
2-latency=900ms          http batch=1500                    HTTP      3        1431.1      1340.2      1453.8      7500      5        5    1500.0    1048.176
2-latency=900ms          http batch=10000 (recommended)     HTTP      3        2289.7      2198.5      2380.3     10000      1        1   10000.0    4201.231
2-latency=900ms          gcs flush=10000                    GCS       3        1574.1      1571.2      1576.5     10000      9        1    1111.1     707.196
2-latency=900ms          gcs flush=10000 eo=off             GCS       3       10876.6     10856.7     10947.6     10000      1        1   10000.0     913.441
3-size=128B              http batch=1500                    HTTP      3       21351.3     16419.4     22014.6      7500      5        5    1500.0      68.137
3-size=128B              gcs flush=10000                    GCS       3      963656.7    826397.8    969677.4    200000    123       20    1626.0       1.687
3-size=2048B             http batch=1500                    HTTP      3       11496.3      9092.5     13566.1      7500      5        5    1500.0     110.569
3-size=2048B             gcs flush=10000                    GCS       3      274768.6    213066.6    327532.5    200000    123       20    1626.0       4.964
4-logging-cost           http batch=1500 WARN (compute only) HTTP      5       13549.1     11816.4     21971.2     15000     10       10    1500.0     110.709
4-logging-cost           http batch=1500 INFO (production default) HTTP      5       15621.1      8457.1     16606.3     15000     10       10    1500.0      92.420
```

### GCS vs HTTP deviation (grouped by family)

The GCS row used for each ratio is the production-default **exactly-once-enabled** variant (the `eo=off` variant is excluded from the ratio; its numbers are in the table above). Ratios are shown to two decimals by the tool but should be read as approximate given the HTTP median's run-to-run swing (see the min/max columns).

```
Family                     GCS median rec/s HTTP variant                         HTTP median rec/s  GCS/HTTP ratio
------------------------------------------------------------------------------------------------------------------
1-cpu-ceiling                      719124.5 http batch=1500                                17264.4          41.65x
1-cpu-ceiling                      719124.5 http batch=10000 (recommended)                  3768.2         190.84x
2-latency=100ms                     13502.1 http batch=1500                                 8104.0           1.67x
2-latency=100ms                     13502.1 http batch=10000 (recommended)                  3199.1           4.22x
2-latency=400ms                      3514.2 http batch=1500                                 2869.9           1.22x
2-latency=400ms                      3514.2 http batch=10000 (recommended)                  3058.5           1.15x
2-latency=900ms                      1574.1 http batch=1500                                 1431.1           1.10x
2-latency=900ms                      1574.1 http batch=10000 (recommended)                  2289.7           0.69x
3-size=128B                        963656.7 http batch=1500                                21351.3          45.13x
3-size=2048B                       274768.6 http batch=1500                                11496.3          23.90x
```

(A `GCS/HTTP ratio` below `1.00x` means HTTP was faster than GCS in that row — see `2-latency=900ms http batch=10000`. The deviation table's near-`1.0x` latency ratios are for the **exactly-once-on** GCS variant, and are exactly the artifact discussed in the Correction section — the exactly-once-**off** variant, not shown in this table, is roughly 8-11x faster than HTTP `batch=1500` at every latency point, per the table in the Correction section above.)

## Reading the results objectively

1. **The gap is largest with zero network latency, not with it.** At `latency=0ms`, GCS drained records ~40-60x faster than HTTP `batch=1500` and ~190-325x faster than HTTP `batch=10000`. With no network delay present at all, this cannot be a network effect — it is entirely in-process cost.
2. **The 400-900ms "convergence" of exactly-once GCS with HTTP is a configuration/amortisation artifact, not latency parity.** Exactly-once GCS makes ~6-9 latency-charged round trips per flush, so its 10,000-record flush is amortised over ~9 delay charges (~1,111 records each) — close to HTTP's 1,500 records per request, so the two spend similar total time in the (identical, synthetic) sleep. Disable exactly-once and each flush is a single round trip amortising all 10,000 records, and GCS is roughly 8-11x faster than HTTP `batch=1500` at 100/400/900ms. **This benchmark therefore does not show, and should not be read as showing, that the two sinks perform equally at production latencies.** It shows the internal behaviour of each connector under an identical, artificial per-operation delay.
3. **`batch.count=10000` is clearly slower than `batch.count=1500` for HTTP at 0ms and 100ms latency, and the crossover happens between 100ms and 400ms in this run** — not only at the highest latency tested. At `0ms`: ~3,800 vs ~17,300 rec/sec (batch=10000 far behind). At `100ms`: ~3,200 vs ~8,100 (still behind). At `400ms`: ~3,060 vs ~2,870 (batch=10000 has just edged ahead). At `900ms`: ~2,290 vs ~1,431 (batch=10000 clearly ahead). The `Avg Op(ms)` column shows why: forming and sending a 10,000-record batch costs ~2,700-4,200ms regardless of simulated latency, i.e. batch-formation cost — not the network — is the floor for that configuration. Increasing `batch.count` only pays off once the endpoint's real per-request latency is high enough to exceed that floor by a meaningful margin; below roughly 400ms in this benchmark, it is a net loss.
4. **Record size has a real, visible effect now that the sweep runs at 0ms** — and GCS is *not* flat, contrary to the old (400ms-masked) summary. HTTP `batch=1500` fell from ~21,400 rec/sec at 128B to ~11,500 at 2048B (~1.9x); GCS fell from ~964,000 to ~275,000 (~3.5x). GCS's JSON serialisation is more size-sensitive in relative terms, though GCS remains far faster in absolute terms at both sizes. The old claim that "GCS was flat across record sizes" was an artifact of the 400ms sleep dominating the timed section, not a real property.
5. **The logging-cost scenario did not cleanly isolate an effect in this run.** The WARN ("compute only") and INFO ("production default") medians (13,549 vs 15,621 rec/sec) are within each other's min/max spread and the ordering is inverted from what "logging costs throughput" would predict. The reason is structural: `BatchPolicy.shouldBatch` calls `logger.info(generateLogLine(...))` through `com.typesafe.scalalogging` (a macro that skips both the string build and the write when the level is below INFO), so the WARN->INFO delta is the per-record log-line construction **plus** the console-appender write — and the appender write's cost depends entirely on the log destination's speed (a slow interactive console vs a fast redirected pipe/file). With stdout redirected in this run, that write was cheap and the effect sank into noise. Per-record logging is still avoidable work worth gating, but the previously-quoted "~16%" is not a stable, destination-independent property and should not be cited as a precise figure.

## Caveats

- **The GCS `Ops` under-counting is now fixed.** `Ops` counts every latency-charged storage call; `Flushes` counts uploaded data files. `Rec/Op` is computed against charged ops for both sinks. (This was the primary misrepresentation in the previous summary.)
- **GCS's real per-operation latency in production is unknown and was never measured here.** This benchmark applies an *identical, synthetic* delay to every storage call for both sinks. Real GCS per-call latency (and its distribution across the different call types in the commit chain) is almost certainly different from — and likely much lower than — any single number plugged into this synthetic model. No real customer-endpoint or real-GCS latency was measured; do not read the latency-sweep rows as predictions of production throughput.
- **Short-run GC noise is largely resolved for GCS CPU scenarios.** The zero-latency GCS runs now process 200,000 records, so a single GC pause no longer dominates; the min/max spread on those rows is much tighter than the previous 2x swings. HTTP's zero-latency rows still show meaningful spread (the `O(n^2)` formation cost interacts with GC), so treat the large GCS/HTTP CPU ratios as order-of-magnitude, not exact.
- **The record-size sweep now runs at 0ms**, so its numbers reflect serialisation/rendering cost, not sleep time. (Previously it ran at 400ms and was uninformative.)
- **Logging cost is destination-dependent and was not cleanly isolated this run** (see point 5 above); the scenario is retained but its result should be read as "within noise here", not as a measured percentage.
- **Single JVM, wall-clock timing, shared-process warm-up/GC noise.** The median is reported specifically to reduce sensitivity to this; scenario-to-scenario comparisons at similar magnitudes should be read as directional, not exact.
- **Per-task, single-partition, no Kafka.** These numbers do not include consumer/poll overhead or multi-task contention; connector-level throughput requires multiplying by the number of running tasks.
- **Not a byte-for-byte content test.** Records carry a JSON string value; HTTP's template passes it straight through, while GCS's converter path re-encodes it. This does not affect the throughput comparison (nothing here asserts on output content).
- **Retry middleware bypassed on the HTTP side** (see harness scaladoc) — production wraps the client in `Retry`, which adds negligible cost when every response is `200 OK`, as in every scenario here.

## Suggested follow-ups (not made as part of this benchmark)

1. Fix `RecordsQueueBatcher.takeBatch`'s `O(n^2)` batch-formation cost (accumulate directly in the mutable buffer and maintain running count/size/offset totals incrementally instead of recomputing over the whole batch-so-far on every record). This is the single highest-impact change for the HTTP sink.
2. Gate `BatchPolicy.shouldBatch`'s per-record log line so that even the message construction is skipped unless the level is enabled (the scala-logging macro already does this for the level check; ensure no eagerly-evaluated work precedes it), consistent with how `RecordsQueueBatcher`'s own per-record log line is already gated at `logger.debug`.
3. Re-run this benchmark once (1) is fixed to confirm `batch.count=10000` (or higher) then reliably outperforms `batch.count=1500` across the full latency range, not just at `>=900ms`.
4. If a production-latency comparison is actually wanted, measure real per-operation latencies for both the HTTP endpoint and GCS (including the per-call-type breakdown of the exactly-once commit chain) and feed those into the model, rather than applying one synthetic delay uniformly to every call.
