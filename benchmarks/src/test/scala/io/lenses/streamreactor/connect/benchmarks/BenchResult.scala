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

/**
 * Outcome of a single throughput run (one sink, one iteration of one scenario).
 *
 * @param sink            label identifying which sink/config this run measured, e.g. "HTTP" or "GCS".
 * @param scenario         short human-readable description of the sweep point, e.g. "latency=400ms".
 * @param records          total number of records successfully drained.
 * @param elapsedNanos     wall-clock time from the first `put()` call to full drain (all records
 *                         acknowledged/committed).
 * @param networkOps       number of latency-charged network round-trips performed. For HTTP this
 *                         is the number of HTTP requests; for GCS this is every latency-charged
 *                         storage call (upload + temp-file move/delete + index/lock bookkeeping),
 *                         NOT just the upload count -- see [[flushes]].
 * @param flushes          number of flushes/uploaded files. For HTTP this equals [[networkOps]]
 *                         (one request per batch). For GCS this is the `uploadFile` count (one
 *                         data file per flush), which with exactly-once enabled is far smaller
 *                         than [[networkOps]] because the commit chain makes several extra
 *                         latency-charged calls per flush.
 * @param heapUsedDeltaBytes approximate heap growth attributable to the run (best-effort; see
 *                           [[HeapSampler]]).
 */
final case class BenchResult(
  sink:                String,
  scenario:             String,
  records:              Long,
  elapsedNanos:         Long,
  networkOps:           Long,
  flushes:              Long,
  heapUsedDeltaBytes:   Long,
) {
  def elapsedMillis: Double = elapsedNanos / 1e6

  def recordsPerSec: Double = if (elapsedNanos <= 0) 0.0 else records * 1e9 / elapsedNanos.toDouble

  /** Records per latency-charged network round-trip -- the fair, apples-to-apples amortisation. */
  def avgRecordsPerOp: Double = if (networkOps <= 0) 0.0 else records.toDouble / networkOps.toDouble

  def avgOpLatencyMillis: Double = if (networkOps <= 0) 0.0 else elapsedMillis / networkOps.toDouble
}

/**
 * A scenario point measured across `iterations` repeats (after any discarded warm-up runs),
 * summarised as median/min/max `recordsPerSec` to absorb JIT warm-up and GC noise from a single
 * shared JVM process. Non-timing columns (records, ops, rec/op) are deterministic given fixed
 * inputs, so `sample` (the last measured iteration) is used to report those directly rather than
 * aggregating them.
 *
 * @param family  groups scenario points that should be compared against each other across sinks,
 *                e.g. "cpu-ceiling", "latency=400ms", "size=2048B". Used by
 *                [[ResultsTablePrinter.printDeviation]] to line up the GCS and HTTP rows for the
 *                same sweep point.
 * @param variant distinguishes multiple configurations run within the same sink/family, e.g.
 *                "http batch=1500" vs "http batch=10000".
 */
final case class AggregatedResult(
  sink:                 String,
  family:                String,
  variant:               String,
  iterations:            Int,
  medianRecordsPerSec:   Double,
  minRecordsPerSec:      Double,
  maxRecordsPerSec:      Double,
  sample:                BenchResult,
)

object BenchAggregator {

  /** `runs` must be non-empty measured iterations (warm-up iterations must already be excluded). */
  def aggregate(sink: String, family: String, variant: String, runs: Seq[BenchResult]): AggregatedResult = {
    require(runs.nonEmpty, "aggregate() requires at least one measured iteration")
    val sortedRates = runs.map(_.recordsPerSec).sorted
    AggregatedResult(
      sink                = sink,
      family              = family,
      variant             = variant,
      iterations          = runs.size,
      medianRecordsPerSec = median(sortedRates),
      minRecordsPerSec    = sortedRates.head,
      maxRecordsPerSec    = sortedRates.last,
      sample              = runs.last,
    )
  }

  private def median(sorted: Seq[Double]): Double = {
    val n = sorted.size
    if (n % 2 == 1) sorted(n / 2) else (sorted(n / 2 - 1) + sorted(n / 2)) / 2.0
  }
}

/**
 * Renders [[AggregatedResult]]s as plain-text tables: the per-scenario measurements, and a
 * separate GCS-vs-HTTP deviation table computed by grouping on `family`.
 */
object ResultsTablePrinter {

  def printAggregated(results: Seq[AggregatedResult]): String = {
    val header =
      f"${"Family"}%-24s ${"Variant"}%-34s ${"Sink"}%-5s ${"Iter"}%5s ${"Median rec/s"}%13s ${"Min rec/s"}%11s ${"Max rec/s"}%11s ${"Records"}%9s ${"Ops"}%6s ${"Flushes"}%8s ${"Rec/Op"}%9s ${"Avg Op(ms)"}%11s"
    val separator = "-" * header.length
    val rows = results.map { a =>
      f"${a.family}%-24s ${a.variant}%-34s ${a.sink}%-5s ${a.iterations}%5d ${a.medianRecordsPerSec}%13.1f ${a.minRecordsPerSec}%11.1f ${a.maxRecordsPerSec}%11.1f ${a.sample.records}%9d ${a.sample.networkOps}%6d ${a.sample.flushes}%8d ${a.sample.avgRecordsPerOp}%9.1f ${a.sample.avgOpLatencyMillis}%11.3f"
    }
    (Seq(header, separator) ++ rows).mkString("\n")
  }

  /**
   * For every `family` that has a canonical GCS row, prints how each non-GCS (HTTP) row in that
   * same family compares to it, as a throughput ratio. The canonical GCS row is the default,
   * exactly-once-enabled configuration (the production default); families may additionally carry
   * an `eo=off` GCS variant, which is intentionally excluded from the ratio here so the comparison
   * stays against the production-default GCS behaviour (the `eo=off` numbers are shown in the main
   * results table). Families with no GCS row (e.g. the logging-cost HTTP-only comparison) are
   * skipped -- that comparison is HTTP-vs-HTTP, not GCS-vs-HTTP.
   */
  def printDeviation(results: Seq[AggregatedResult]): String = {
    val byFamily = results.groupBy(_.family)
    val header   = f"${"Family"}%-24s ${"GCS median rec/s"}%18s ${"HTTP variant"}%-34s ${"HTTP median rec/s"}%19s ${"GCS/HTTP ratio"}%15s"
    val separator = "-" * header.length
    val rows = byFamily.toSeq.sortBy(_._1).flatMap {
      case (family, rows) =>
        val httpRows = rows.filter(_.sink == "HTTP")
        // Canonical GCS row = the exactly-once-enabled (production default) variant, i.e. not the
        // "eo=off" contrast variant.
        val gcsRows = rows.filter(row => row.sink == "GCS" && !row.variant.contains("eo=off"))
        if (gcsRows.size != 1 || httpRows.isEmpty) {
          Seq.empty
        } else {
          val gcs = gcsRows.head
          httpRows.map { http =>
            val ratio = if (http.medianRecordsPerSec <= 0) Double.PositiveInfinity else gcs.medianRecordsPerSec / http.medianRecordsPerSec
            f"${family}%-24s ${gcs.medianRecordsPerSec}%18.1f ${http.variant}%-34s ${http.medianRecordsPerSec}%19.1f ${ratio}%14.2fx"
          }
        }
    }
    (Seq(header, separator) ++ rows).mkString("\n")
  }
}

/**
 * Best-effort heap usage sampling around a benchmark run. Not a substitute for a real profiler:
 * intended only to corroborate large, order-of-magnitude differences (e.g. the customer's "2x
 * memory" observation), not to make precise per-record allocation claims.
 */
object HeapSampler {

  /** Requests a full GC and reads used heap. Best-effort: JVMs are not required to honour System.gc(). */
  def usedHeapBytes(): Long = {
    val runtime = Runtime.getRuntime
    System.gc()
    Thread.sleep(50)
    System.gc()
    runtime.totalMemory() - runtime.freeMemory()
  }

  def measure[A](block: => A): (A, Long, Long) = {
    val before = usedHeapBytes()
    val result = block
    val after  = usedHeapBytes()
    (result, before, after)
  }
}
