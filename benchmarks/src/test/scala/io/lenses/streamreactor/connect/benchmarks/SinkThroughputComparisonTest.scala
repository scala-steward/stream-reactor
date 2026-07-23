/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.benchmarks

import cats.effect.unsafe.IORuntime
import ch.qos.logback.classic.{ Logger => LogbackLogger }
import ch.qos.logback.classic.Level
import io.lenses.streamreactor.connect.benchmarks.gcs.GcsSinkThroughputHarness
import io.lenses.streamreactor.connect.benchmarks.http.HttpSinkThroughputHarness
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

/**
 * Deep performance analysis: HTTP sink vs GCS sink, with Kafka and the network removed.
 *
 * Runs both sinks' real production pipelines (rendering/serialisation, batching/commit-policy
 * evaluation, and either the HTTP request path or the GCS write-and-commit path) against a stub
 * egress that returns success instantly or after a configurable simulated latency. See
 * [[HttpSinkThroughputHarness]] and [[GcsSinkThroughputHarness]] for what "removed" means
 * precisely, and `benchmarks/EXECUTIVE_SUMMARY.md` for the methodology and findings.
 *
 * This is a local comparison tool, not a correctness or CI test: it is intentionally excluded
 * from the root aggregate/`fullTest`. Run it directly with:
 *
 * {{{
 *   sbt "project benchmarks" "testOnly *SinkThroughputComparisonTest"
 * }}}
 *
 * Methodology notes (see also `benchmarks/EXECUTIVE_SUMMARY.md`):
 *  - Every scenario runs `warmup` discarded iterations (JIT/class-loading warm-up) followed by
 *    `measured` iterations that are aggregated into a median (plus min/max) via
 *    [[BenchAggregator]], to absorb JIT and GC noise from running many scenarios in one shared
 *    JVM process.
 *  - `benchmarks/src/test/resources/logback-test.xml` sets the root logger, and specifically
 *    `BatchPolicy`/`CommitPolicy`, to WARN, so these numbers measure compute, not per-record
 *    logging I/O. Scenario 4 below measures that logging cost directly, by temporarily raising
 *    `BatchPolicy` back to its production-default INFO level.
 *
 * Sweep design:
 *  1. Pure CPU ceiling (egress latency = 0) -- isolates in-process cost from network latency; the
 *     sole zero-latency measurement point (GCS runs 200,000 records so its timed section is above
 *     single-GC-pause noise).
 *  2. Egress latency sweep (100/400/900ms) -- reproduces the real-world gap, and includes the
 *     "recommended settings" (`batch.count=10000`) HTTP configuration the customer reported
 *     still showing the same issue, plus a GCS `exactly.once.enable=false` variant that flushes
 *     with ~1 latency-charged op per flush (vs ~6-8 with exactly-once on) to expose the
 *     amortisation-ratio artifact behind the old "convergence" narrative.
 *  3. Record size sweep at 0ms latency -- isolates the cost of rendering/serialising larger
 *     payloads (run at 0ms, not 400ms, so the serialisation cost is not swamped by sleep time).
 *  4. Logging-cost delta -- quantifies how much of the HTTP pure-CPU cost is
 *     `BatchPolicy`'s per-record INFO logging vs genuine batch-formation compute.
 *
 * Record counts are always an exact multiple of the batch/flush count for the scenario, so every
 * record is included in a full batch/flush -- see the harness scaladocs for why a remainder
 * would otherwise never flush.
 */
class SinkThroughputComparisonTest extends AnyFunSuite {

  private implicit val ioRuntime: IORuntime = IORuntime.global

  private val httpHarness = new HttpSinkThroughputHarness()
  private val gcsHarness  = new GcsSinkThroughputHarness()

  private val results: ListBuffer[AggregatedResult] = ListBuffer.empty

  private val bucket = "bench-bucket"

  // Batch/flush sizes mirror the customer's actual configuration (HTTP batch.count=1500) and
  // GCS's flush.count=10000, plus the "recommended settings" HTTP batch.count=10000 the customer
  // says produced the same symptoms.
  private val HttpBatchDefault     = 1500L
  private val HttpBatchRecommended = 10000L
  private val GcsFlushCount        = 10000L

  private def httpProps(batchCount: Long): Map[String, String] =
    Map("connect.http.batch.count" -> batchCount.toString)

  private def httpIteration(
    scenarioLabel: String,
    batchCount:    Long,
    batchesToRun:  Int,
    egressLatency: FiniteDuration,
    payloadBytes:  Int,
  ): BenchResult = {
    val records = RecordGenerator.sinkRecords("bench-http-topic", (batchCount * batchesToRun).toInt, payloadBytes = payloadBytes)
    // Single interpreter boundary: the harness is pure (IO[BenchResult]); we run it here.
    httpHarness.run(scenarioLabel, httpProps(batchCount), records, egressLatency).unsafeRunSync()
  }

  private def gcsIteration(
    scenarioLabel:      String,
    flushCount:         Long,
    batchesToRun:       Int,
    egressLatency:      FiniteDuration,
    payloadBytes:       Int,
    exactlyOnceEnabled: Boolean,
  ): BenchResult = {
    val records = RecordGenerator.sinkRecords("bench-gcs-topic", (flushCount * batchesToRun).toInt, payloadBytes = payloadBytes)
    gcsHarness.run(scenarioLabel,
                   "bench-gcs-topic",
                   bucket,
                   records,
                   flushCount,
                   egressLatency,
                   exactlyOnceEnabled = exactlyOnceEnabled,
    )
  }

  /** Runs `warmup` discarded iterations then `measured` iterations, aggregated to a median. */
  private def repeatHttp(
    family:        String,
    variant:       String,
    batchCount:    Long,
    batchesToRun:  Int,
    warmup:        Int            = 2,
    measured:      Int            = 5,
    egressLatency: FiniteDuration = 0.millis,
    payloadBytes:  Int            = 128,
  ): AggregatedResult = {
    val label = s"$family $variant"
    (0 until warmup).foreach(_ => httpIteration(label, batchCount, batchesToRun, egressLatency, payloadBytes))
    val runs = (0 until measured).map(_ => httpIteration(label, batchCount, batchesToRun, egressLatency, payloadBytes))
    val aggregated = BenchAggregator.aggregate("HTTP", family, variant, runs)
    results += aggregated
    aggregated
  }

  private def repeatGcs(
    family:             String,
    variant:            String,
    flushCount:         Long,
    batchesToRun:       Int,
    warmup:             Int            = 2,
    measured:           Int            = 5,
    egressLatency:      FiniteDuration = 0.millis,
    payloadBytes:       Int            = 128,
    exactlyOnceEnabled: Boolean        = true,
  ): AggregatedResult = {
    val label = s"$family $variant"
    (0 until warmup).foreach(_ => gcsIteration(label, flushCount, batchesToRun, egressLatency, payloadBytes, exactlyOnceEnabled))
    val runs =
      (0 until measured).map(_ => gcsIteration(label, flushCount, batchesToRun, egressLatency, payloadBytes, exactlyOnceEnabled))
    val aggregated = BenchAggregator.aggregate("GCS", family, variant, runs)
    results += aggregated
    aggregated
  }

  test("1. pure CPU ceiling (no simulated egress latency)") {
    repeatHttp("1-cpu-ceiling", "http batch=1500", HttpBatchDefault, batchesToRun = 10)
    repeatHttp("1-cpu-ceiling", "http batch=10000 (recommended)", HttpBatchRecommended, batchesToRun = 2)
    // GCS's CPU-bound throughput is high enough (~hundreds of thousands rec/s) that a 20k-record
    // timed section completes in ~15-30ms, small enough that a single GC pause can swing the
    // median 2x. Run 200,000 records (batchesToRun=20) so the timed section is comfortably above
    // single-GC-pause noise and the median is reproducible.
    repeatGcs("1-cpu-ceiling", "gcs flush=10000", GcsFlushCount, batchesToRun = 20)
  }

  test("2. egress latency sweep") {
    // No warm-up here: the dominant cost is the deterministic simulated-latency sleep, not JIT.
    // The zero-latency point is measured once, by scenario 1 (pure CPU ceiling), so it is not
    // repeated here -- this sweep starts at 100ms.
    for (latencyMs <- Seq(100, 400, 900)) {
      val family = s"2-latency=${latencyMs}ms"
      val d      = latencyMs.millis
      repeatHttp(family, "http batch=1500", HttpBatchDefault, batchesToRun = 5, warmup = 0, measured = 3, egressLatency = d)
      repeatHttp(family, "http batch=10000 (recommended)", HttpBatchRecommended, batchesToRun = 1, warmup = 0, measured = 3, egressLatency = d)
      // GCS with exactly-once ON (production default): the commit chain makes ~6-8 latency-charged
      // storage calls per flush, so records are amortised over many more round-trips than the
      // single upload the old benchmark counted.
      repeatGcs(family, "gcs flush=10000", GcsFlushCount, batchesToRun = 1, warmup = 0, measured = 3, egressLatency = d)
      // GCS with exactly-once OFF: each flush is a single direct upload (~1 charged op per flush),
      // so all 10,000 records are amortised over one round-trip. Included to expose the
      // amortisation-ratio artifact that made exactly-once GCS appear to "converge" with HTTP.
      repeatGcs(family,
                "gcs flush=10000 eo=off",
                GcsFlushCount,
                batchesToRun       = 1,
                warmup             = 0,
                measured           = 3,
                egressLatency      = d,
                exactlyOnceEnabled = false,
      )
    }
  }

  test("3. record size sweep at 0ms latency (isolates serialisation cost)") {
    // Run at 0ms simulated latency, NOT 400ms. At 400ms almost all of each iteration's wall clock
    // is the sleep, which swamps any real serialisation/rendering-cost difference between a 128B
    // and a 2048B record. At 0ms the timed section is pure in-process cost, so any real
    // serialisation-cost difference between record sizes is actually visible for both sinks.
    for (payloadBytes <- Seq(128, 2048)) {
      val family = s"3-size=${payloadBytes}B"
      repeatHttp(family, "http batch=1500", HttpBatchDefault, batchesToRun = 5, warmup = 2, measured = 3, egressLatency = 0.millis, payloadBytes = payloadBytes)
      repeatGcs(family, "gcs flush=10000", GcsFlushCount, batchesToRun = 20, warmup = 2, measured = 3, egressLatency = 0.millis, payloadBytes = payloadBytes)
    }
  }

  test("4. logging-cost delta (BatchPolicy WARN vs production-default INFO)") {
    // Trailing "$": BatchPolicy is a Scala `object`, so its runtime (and SLF4J/logback) logger
    // name includes the module suffix -- see the comment in logback-test.xml for why the
    // non-"$" name would silently target an unrelated, unconfigured logger instead.
    val loggerName = "io.lenses.streamreactor.common.batch.BatchPolicy$"
    val family      = "4-logging-cost"

    // logback-test.xml already sets this to WARN; this scenario is the baseline (compute only).
    repeatHttp(family, "http batch=1500 WARN (compute only)", HttpBatchDefault, batchesToRun = 10)

    // Temporarily raise BatchPolicy back to its production-default INFO level to measure the
    // per-record logging cost that scenario 1 above deliberately excludes.
    withLoggerLevel(loggerName, Level.INFO) {
      repeatHttp(family, "http batch=1500 INFO (production default)", HttpBatchDefault, batchesToRun = 10)
    }
  }

  test("5. print comparison tables") {
    val resultsTable   = ResultsTablePrinter.printAggregated(results.toSeq)
    val deviationTable = ResultsTablePrinter.printDeviation(results.toSeq)
    // scalastyle:off regex
    println("\n=== Measured results (median of measured iterations) ===\n" + resultsTable)
    println("\n=== GCS vs HTTP deviation (grouped by family) ===\n" + deviationTable + "\n")
    // scalastyle:on regex
    assert(results.nonEmpty)
  }

  private def withLoggerLevel[A](loggerName: String, level: Level)(block: => A): A = {
    val logger   = LoggerFactory.getLogger(loggerName).asInstanceOf[LogbackLogger]
    val original = logger.getLevel
    logger.setLevel(level)
    try block
    finally logger.setLevel(original)
  }
}
