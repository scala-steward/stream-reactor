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
package io.lenses.streamreactor.connect.benchmarks.gcs

import io.lenses.streamreactor.connect.benchmarks.BenchResult
import io.lenses.streamreactor.connect.benchmarks.HeapSampler
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito

import java.util
import java.util.Collections
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

/**
 * Drives the real `GCPStorageSinkTask` (via [[BenchmarkGCPStorageSinkTask]]) for a single
 * (single-partition) topic, with Kafka and the network removed:
 *
 *  - "Kafka removed": records are handed directly to `put()` as `SinkRecord`s, exactly as a
 *    Connect worker would; there is no consumer/broker involved.
 *  - "Network removed": `createStorageInterface` returns [[NoOpGCPStorage]], so no bytes ever
 *    reach GCS. Everything above that seam -- KCQL/commit-policy parsing, `WriterManager`,
 *    `IndexManagerV2`, the local staging file, and `JsonFormatWriter`'s per-record JSON
 *    serialisation -- is unmodified production code, wrapped by the same
 *    `RetryingStorageInterface`/`StorageInterfaceWithMetrics` decorators production uses.
 *
 * Unlike the HTTP sink, the GCS sink's write-and-commit chain (including the "upload") runs
 * synchronously on the calling thread inside `put()` (see `CloudSinkTask.put` ->
 * `WriterManager.write` -> `Writer.commit` -> `StorageInterface.uploadFile`), so no separate
 * drain loop is required: once the last `put()` call returns, every record that was going to be
 * flushed by then already has been.
 */
class GcsSinkThroughputHarness {

  private val prefix = GCPConfigSettings.CONNECTOR_PREFIX // "connect.gcpstorage"

  /**
   * @param records          input records; `records.length` MUST be an exact multiple of
   *                         `flushCount` so the final partial batch is not left stranded,
   *                         unflushed, in the writer's staging file (mirrors the same
   *                         constraint on the HTTP harness).
   * @param flushCount       KCQL `flush.count` -- records per uploaded file.
   * @param egressLatency    simulated GCS upload latency, applied inside [[NoOpGCPStorage]].
   * @param exactlyOnceEnabled sets `connect.gcpstorage.exactly.once.enable`. When `true` (the
   *                         production default) every flush drives the index/commit chain, which
   *                         makes several latency-charged storage calls per flush; when `false`
   *                         each flush is a single direct upload (~1 charged op per flush).
   */
  def run(
    scenario:     String,
    topic:        String,
    bucket:       String,
    records:      IndexedSeq[SinkRecord],
    flushCount:   Long,
    egressLatency: FiniteDuration = Duration.Zero,
    pollChunkSize: Int            = 500,
    exactlyOnceEnabled: Boolean   = true,
  ): BenchResult = {
    val storage = new NoOpGCPStorage(egressLatency.toMillis)
    val task    = new BenchmarkGCPStorageSinkTask(storage)
    val context = Mockito.mock(classOf[SinkTaskContext])

    val props: util.Map[String, String] = Map(
      "name"                          -> "bench-gcs",
      s"$prefix.task.index"           -> "1:1",
      s"$prefix.gcp.auth.mode"        -> "none",
      s"$prefix.gcp.project.id"       -> "bench-project",
      s"$prefix.indexes.gc.sweep.enabled" -> "false",
      s"$prefix.exactly.once.enable"      -> exactlyOnceEnabled.toString,
      s"$prefix.kcql" ->
        s"insert into $bucket:bench select * from $topic PROPERTIES('flush.count'=$flushCount,'flush.size'=500000000,'flush.interval'=3600)",
    ).asJava

    task.initialize(context)
    task.start(props)
    task.open(Seq(new TopicPartition(topic, 0)).asJava)

    val heapBefore = HeapSampler.usedHeapBytes()
    val start      = System.nanoTime()

    records.grouped(math.max(1, pollChunkSize)).foreach { chunk =>
      task.put(chunk.asJava)
    }
    // Mirrors the production "empty put()" hook that forces a time-based flush of any partial
    // batch; a no-op here as long as `records.length` is an exact multiple of `flushCount`.
    task.put(Collections.emptyList[SinkRecord]())

    val elapsedNanos = System.nanoTime() - start
    val heapAfter    = HeapSampler.usedHeapBytes()

    task.close(Seq(new TopicPartition(topic, 0)).asJava)
    task.stop()

    BenchResult(
      sink               = "GCS",
      scenario           = scenario,
      records            = records.length.toLong,
      elapsedNanos       = elapsedNanos,
      networkOps         = storage.totalChargedOps,
      flushes            = storage.totalUploads,
      heapUsedDeltaBytes = math.max(0L, heapAfter - heapBefore),
    )
  }
}
