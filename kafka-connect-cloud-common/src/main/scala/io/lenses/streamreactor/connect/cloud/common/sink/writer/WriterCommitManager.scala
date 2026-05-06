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
package io.lenses.streamreactor.connect.cloud.common.sink.writer

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata

/**
 * Manages the commit operations for writers.
 *
 * Selective-commit contract (see the "Selective commit fan-out" subsection in
 * `docs/datalake-exactly-once-partitionby.md`):
 *
 *   - `commitFlushableWriters` / `commitFlushableWritersForTopicPartition` commit ONLY
 *     writers where `shouldFlush == true`. Sibling writers on the same `TopicPartition`
 *     stay open. The previous fan-out-on-every-flush behaviour is gone.
 *   - `commitPending` commits ONLY writers in `Uploading` state (`hasPendingUpload == true`).
 *     Writing-state siblings are not opportunistically force-flushed by the pending-retry
 *     path; this avoids dragging unrelated writers into a commit cycle that they did not
 *     trigger and is the deliberate amendment to the prior plan version.
 *   - `commitForTopicPartition` is the only path that still fans out across every writer
 *     for a `TopicPartition`. It is reserved for schema rollover, where every sibling must
 *     flush together to preserve format-boundary semantics. Do NOT weaken this method to
 *     selective; the regression is pinned by `WriterCommitManagerTest "commitForTopicPartition
 *     should commit all writers for the given topic partition if one is committed"`.
 *
 * Safety: `WriterManager.getOffsetAndMeta` continues to scan EVERY active writer on the
 * topic-partition when computing `globalSafeOffset`, so the consumer-committable offset
 * is bounded by the slowest active writer's `firstBufferedOffset` even when only a subset
 * of writers commit. Selective commit reduces the *CommitSet* but never the *BarrierSet*.
 *
 * @param fnGetWriters    Function to retrieve the current map of writers.
 * @param metrics         Metrics sink for selective-commit observability. Defaults to a
 *                        fresh instance so test helpers compile without wiring; production
 *                        callers thread the WriterManager-level metrics through so the JMX
 *                        bean reflects real activity.
 * @param connectorTaskId Implicit task ID for logging purposes.
 * @tparam SM Type parameter for file metadata.
 */
class WriterCommitManager[SM <: FileMetadata](
  fnGetWriters: () => Map[MapKey, Writer[SM]],
  metrics:      CloudSinkMetrics = new CloudSinkMetrics(),
)(
  implicit
  connectorTaskId: ConnectorTaskId,
) extends LazyLogging {

  /**
   * Commits writers that have pending uploads (state == `Uploading`).
   *
   * Selective: only the writers that match the predicate are committed. Sibling Writing
   * writers on the same `TopicPartition` are left open — they will commit when their own
   * flush trigger fires. Every Uploading writer still retries on every call, so there is
   * no starvation.
   */
  def commitPending(): Either[SinkError, Unit] =
    commitWritersWithFilter {
      case (_, writer) => writer.hasPendingUpload
    }

  /**
   * Commits every writer for `topicPartition`, irrespective of state or flush threshold.
   *
   * This is the schema-rollover full-fan-out path: when a record with an incompatible
   * schema arrives, every sibling writer on the topic-partition must flush together so
   * the format boundary is consistent. Do NOT replace this with a selective filter.
   */
  def commitForTopicPartition(topicPartition: TopicPartition): Either[BatchCloudSinkError, Unit] =
    commitWritersWithFilter {
      case (mapKey, _) =>
        mapKey.topicPartition == topicPartition
    }

  /**
   * Commits writers that should be flushed, across all topic-partitions. Selective:
   * non-flushable siblings keep buffering.
   */
  def commitFlushableWriters(): Either[BatchCloudSinkError, Unit] =
    commitWritersWithFilter {
      case (_, writer) => writer.shouldFlush
    }

  /**
   * Commits writers that should be flushed for a specific topic partition. Selective:
   * non-flushable siblings on the same topic-partition keep buffering.
   */
  def commitFlushableWritersForTopicPartition(topicPartition: TopicPartition): Either[BatchCloudSinkError, Unit] =
    commitWritersWithFilter {
      case (MapKey(tp, _), writer) => tp == topicPartition && writer.shouldFlush
    }

  /**
   * Selective commit: commit exactly the writers matching `keyValueFilterFn`.
   *
   * The previous implementation expanded the selected set to every sibling on the same
   * `TopicPartition`. That step has been removed: each cycle observes a single snapshot
   * of the writer map, picks the matching entries, commits them, and reports metrics
   * describing how much fan-out was avoided versus the legacy behaviour.
   *
   * Errors are aggregated as `BatchCloudSinkError` over the selected set, preserving the
   * pre-existing classification semantics (`fatal` vs `nonFatal`, `rollBack()`,
   * `topicPartitions()`).
   */
  private def commitWritersWithFilter(
    keyValueFilterFn: ((MapKey, Writer[SM])) => Boolean,
  ): Either[BatchCloudSinkError, Unit] = {

    val snapshot = fnGetWriters()
    val selected = snapshot.filter(keyValueFilterFn)

    // Benefit metric: count of writers that the legacy "expand to every sibling on the
    // matching TPs" step would have committed. With selective commit, anything in this
    // matching-TP set that is NOT in `selected` is a sibling we successfully avoided.
    val matchingTopicPartitions = selected.keysIterator.map(_.topicPartition).toSet
    val matchingTpWriters =
      if (matchingTopicPartitions.isEmpty) 0
      else snapshot.count { case (MapKey(tp, _), _) => matchingTopicPartitions.contains(tp) }

    metrics.recordSelectiveCommit(selectedSize = selected.size, matchingTpWriters = matchingTpWriters)

    logger.debug(
      s"[{}] Selective commit: selected={} matchingTpWriters={} avoided={}",
      connectorTaskId.show,
      selected.size,
      matchingTpWriters,
      math.max(0, matchingTpWriters - selected.size),
    )

    val errors = selected.iterator
      .map { case (_, w) => w.commit }
      .collect { case Left(err) => err }
      .toSet

    Either.cond(errors.isEmpty, (), BatchCloudSinkError(errors))
  }
}
