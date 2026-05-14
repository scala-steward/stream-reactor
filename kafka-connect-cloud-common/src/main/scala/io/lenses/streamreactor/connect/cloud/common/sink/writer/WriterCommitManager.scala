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

import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata

/**
 * Abstraction over the writer map that lets [[WriterCommitManager]] iterate writers without
 * ever materialising a full immutable snapshot.  The two methods return live iterators backed
 * directly by the mutable data structures held in [[WriterManager]]:
 *
 *  - `iterator`                       — all writers (used by the TP-agnostic paths).
 *  - `iteratorForTopicPartition(tp)`  — only writers keyed to `tp` (used by the per-TP paths,
 *                                       O(siblings on TP) rather than O(all writers)).
 *
 * Callers must not mutate the underlying structures while consuming an iterator.  This is safe
 * because [[WriterManager]] is documented as non-thread-safe and all mutations happen on the
 * same thread as commit calls.
 */
private[writer] trait WriterSource[SM <: FileMetadata] {
  def iterator: Iterator[(MapKey, Writer[SM])]
  def iteratorForTopicPartition(tp: TopicPartition): Iterator[(MapKey, Writer[SM])]
}

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
 *     flush together to preserve format-boundary semantics. Do NOT replace this with a selective filter.
 *
 * Safety: `WriterManager.getOffsetAndMeta` continues to scan EVERY active writer on the
 * topic-partition when computing `globalSafeOffset`, so the consumer-committable offset
 * is bounded by the slowest active writer's `firstBufferedOffset` even when only a subset
 * of writers commit. Selective commit reduces the *CommitSet* but never the *BarrierSet*.
 *
 * @param source          Provides live iterators over the writer map without snapshot allocation.
 * @param connectorTaskId Implicit task ID for logging purposes.
 * @tparam SM Type parameter for file metadata.
 */
class WriterCommitManager[SM <: FileMetadata](
  source: WriterSource[SM],
) {

  /**
   * Commits writers that have pending uploads (state == `Uploading`).
   *
   * Selective: only the writers that match the predicate are committed. Sibling Writing
   * writers on the same `TopicPartition` are left open — they will commit when their own
   * flush trigger fires. Every Uploading writer still retries on every call, so there is
   * no starvation.
   */
  def commitPending(): Either[SinkError, Unit] =
    commitFromIterator(source.iterator.filter { case (_, w) => w.hasPendingUpload })

  /**
   * Commits every writer for `topicPartition`, irrespective of state or flush threshold.
   *
   * This is the schema-rollover full-fan-out path: when a record with an incompatible
   * schema arrives, every sibling writer on the topic-partition must flush together so
   * the format boundary is consistent. Do NOT replace this with a selective filter.
   */
  def commitForTopicPartition(topicPartition: TopicPartition): Either[BatchCloudSinkError, Unit] =
    commitFromIterator(source.iteratorForTopicPartition(topicPartition))

  /**
   * Commits writers that should be flushed, across all topic-partitions. Selective:
   * non-flushable siblings keep buffering.
   */
  def commitFlushableWriters(): Either[BatchCloudSinkError, Unit] =
    commitFromIterator(source.iterator.filter { case (_, w) => w.shouldFlush })

  /**
   * Commits writers that should be flushed for a specific topic partition. Selective:
   * non-flushable siblings on the same topic-partition keep buffering.
   *
   * Uses `iteratorForTopicPartition` so only the O(siblings on TP) entries are visited;
   * no full-map scan or snapshot allocation occurs.
   */
  def commitFlushableWritersForTopicPartition(topicPartition: TopicPartition): Either[BatchCloudSinkError, Unit] =
    commitFromIterator(
      source.iteratorForTopicPartition(topicPartition).filter { case (_, w) => w.shouldFlush },
    )

  /**
   * Drives commits for every entry in `iter`, collects errors, and returns a single
   * `BatchCloudSinkError` if any writer failed (preserving the pre-existing classification
   * semantics: `fatal` vs `nonFatal`, `rollBack()`, `topicPartitions()`).
   * A failure in one writer does not prevent the remaining writers from being committed.
   */
  private def commitFromIterator(
    iter: Iterator[(MapKey, Writer[SM])],
  ): Either[BatchCloudSinkError, Unit] = {
    val errors = iter
      .map { case (_, w) => w.commit }
      .collect { case Left(err) => err }
      .toSet
    Either.cond(errors.isEmpty, (), BatchCloudSinkError(errors))
  }
}
