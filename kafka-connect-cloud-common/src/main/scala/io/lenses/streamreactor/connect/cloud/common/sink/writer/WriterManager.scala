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
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.ForcedWriteReason
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.data.Schema

import java.io.File
import java.net.URLEncoder
import scala.collection.immutable
import scala.collection.mutable
import scala.util.control.NonFatal

case class MapKey(topicPartition: TopicPartition, partitionValues: immutable.Map[PartitionField, String])

/**
 * Classification of a `preCommit` cycle for a single PARTITIONBY topic-partition, used by
 * the master-lock write-decision helper. The classification is computed inside
 * `WriterManager.getOffsetAndMeta` from `globalSafeOffset`, `lastWrittenMasterSafeOffset(tp)`,
 * and the per-cycle force flag.
 *
 *  - `Skip` : the dirty flag is clean (`globalSafeOffset == lastWritten`) and no force point
 *             is active for this cycle. `updateMasterLock` is NOT invoked. The cycle still
 *             returns the safe offset to Kafka Connect, advancing the in-memory HWM and
 *             `lastReturnedSafeOffset(tp)` (no-op in value terms on this branch).
 *  - `WriteRoutine` : the dirty flag is true (`globalSafeOffset > lastWritten`) and no
 *             force point is active. `updateMasterLock` is invoked once with
 *             `globalSafeOffset`; GC runs on success, no-op on failure.
 *  - `WriteForced` : a lifecycle force point is active (revoke / stop / post-`cleanUp`).
 *             Structurally identical to `WriteRoutine` — same eTag-conditional write, same
 *             same-value GC threshold, same fail-closed semantics. Carries the reason tag
 *             solely so the corresponding `masterLockWriteForced*` counter can be
 *             incremented at the call site.
 */
private[writer] sealed trait WriteDecision
private[writer] object WriteDecision {
  case object Skip         extends WriteDecision
  case object WriteRoutine extends WriteDecision
  final case class WriteForced(reason: ForcedWriteReason) extends WriteDecision
}

/**
 * Manages the lifecycle of [[Writer]] instances.
 *
 * A given sink may be writing to multiple locations (partitions), and therefore
 * it is convenient to extract this to another class.
 *
 * This class is not thread safe as it is not designed to be shared between concurrent
 * sinks, since file handles cannot be safely shared without considerable overhead.
 */
class WriterManager[SM <: FileMetadata](
  commitPolicyFn:              TopicPartition => Either[SinkError, CommitPolicy],
  bucketAndPrefixFn:           TopicPartition => Either[SinkError, CloudLocation],
  keyNamerFn:                  TopicPartition => Either[SinkError, KeyNamer],
  stagingFilenameFn:           (TopicPartition, Map[PartitionField, String]) => Either[SinkError, File],
  objKeyBuilderFn:             (TopicPartition, Map[PartitionField, String]) => ObjectKeyBuilder,
  formatWriterFn:              (TopicPartition, File) => Either[SinkError, FormatWriter],
  indexManager:                IndexManager,
  transformerF:                MessageDetail => Either[RuntimeException, MessageDetail],
  schemaChangeDetector:        SchemaChangeDetector,
  skipNullValues:              Boolean,
  pendingOperationsProcessors: PendingOperationsProcessors,
  metrics:                     CloudSinkMetrics = new CloudSinkMetrics(),
)(
  implicit
  connectorTaskId: ConnectorTaskId,
) extends StrictLogging {

  private val writers = mutable.LinkedHashMap.empty[MapKey, Writer[SM]]
  // Thread the same `metrics` instance through so JMX-visible selective-commit counters
  // reflect the real production cycle, not a throwaway sink. Tests that build a
  // WriterManager via the public constructor inherit this wiring automatically; tests
  // that build a WriterCommitManager standalone fall back to the defaulted metrics.
  private val writerCommitManager = new WriterCommitManager[SM](() => writers.toMap, metrics)

  // High-watermark per TopicPartition: the highest globalSafeOffset ever reported to Kafka Connect.
  // Prevents regression when idle-writer eviction removes the writer that defined the previous
  // high watermark (see "globalSafeOffset regression" in docs/datalake-exactly-once-partitionby.md).
  private val safeOffsetHighWatermarks = mutable.Map.empty[TopicPartition, Long]

  // Highest `globalSafeOffset` successfully persisted by `updateMasterLock` for this TP within
  // the current task instance's ownership episode. Mutated ONLY inside the success branch of `updateMasterLock`.
  // Cleared by both `close(tp)` (rebalance / shutdown) and `cleanUp(tp)` (in-place rollback).
  // Lazy-seeded on first access from the durable master-lock floor
  // (`IndexManager.getSeekedOffsetForTopicPartition(tp) + 1`, or 0 if absent), mirroring the
  // seeding rule used by `safeOffsetHighWatermarks`.
  //
  // `dirtyMasterLock(tp)` is a *computed* flag: `globalSafeOffset > lastWrittenMasterSafeOffset(tp)`.
  // It is not stored as a separate field — that would create two sources of truth for the same
  // boolean (the comparison vs. a separately set flag) and a future refactor that updates one
  // without the other would silently break the gate.
  private val lastWrittenMasterSafeOffset = mutable.Map.empty[TopicPartition, Long]

  // Highest `globalSafeOffset` returned to Kafka Connect for this TP within the current task
  // instance's ownership episode.
  // Advances on BOTH the skipped-write and successful-write paths (every cycle that returns
  // an offset). NEVER advances on cycles where `preCommit` returns `None` for the TP.
  //
  // Cleared by `close(tp)` only. PRESERVED across `cleanUp(tp)`.
  //
  // Role differs by mode:
  //
  //  - Non-PARTITIONBY mode (load-bearing): `preCommit` advances `lastReturnedSafeOffset(tp)`
  //    independently of `Writer.commit()`, which is the only code path that updates
  //    `seekedOffsets(tp)` (via `IndexManager.update`). Between flushes, `lastReturned` can
  //    sit strictly ahead of `getSeekedOffsetForTopicPartition(tp).value + 1`. After a
  //    `cleanUp(tp)`, the HWM re-seed expression `max(durableFloor + 1, lastReturned)` needs
  //    the `lastReturned` term to avoid regressing below what Kafka Connect was already told.
  //
  //  - PARTITIONBY mode (defence-in-depth): `IndexManagerV2.updateMasterLock` updates
  //    `seekedOffsets(tp)` atomically with each successful cloud write, so in production the
  //    two terms in `max(durableFloor + 1, lastReturned)` are equal after every successful
  //    cycle. The `IndexManager` trait does NOT enforce that contract, however — a future
  //    implementation that decouples the two would silently break post-`cleanUp` monotonicity
  //    without this field. Preserving it is the safety net against that drift.
  private val lastReturnedSafeOffset = mutable.Map.empty[TopicPartition, Long]

  // Force-after-`cleanUp(tp)` flag: when set, the next `preCommit` cycle for this TP MUST be
  // classified `WriteForced(PostCleanUp)` regardless of whether the routine dirty-flag gate
  // would also have admitted a write. Cleared on the next master-lock write attempt (success
  // or failure) — if the forced write fails the dirty bit alone re-admits the next routine
  // cycle, which is the design's correctness fallback.
  private val forceWriteAfterCleanUp = mutable.Set.empty[TopicPartition]

  def recommitPending(): Either[SinkError, Unit] = {
    logger.debug(s"[{}] Retry Pending", connectorTaskId.show)
    val result = writerCommitManager.commitPending()
    logger.debug(s"[{}] Retry Pending Complete", connectorTaskId.show)
    result
  }

  def commitFlushableWriters(): Either[BatchCloudSinkError, Unit] = {
    logger.debug(s"[{}] Received call to WriterManager.commitFlushableWriters", connectorTaskId.show)
    writerCommitManager.commitFlushableWriters()
  }

  /**
   * Per-TP close routine, used by both `close()` (rebalance) and the stop path.
   * Implements the locked-in step ordering for force-on-revoke:
   *
   *   1. Compute `globalSafeOffset` from this TP's writers and the current HWM.
   *   2. Attempt `updateMasterLock` if `dirtyMasterLock(tp)` is true; otherwise skip.
   *   3. On success: update `lastWrittenMasterSafeOffset(tp)`; run GC enqueue against the
   *      just-persisted value. On failure: leave all state untouched (no eTag refresh, no
   *      GC, no fatal escalation).
   *   4. Clear `safeOffsetHighWatermarks(tp)`, `lastWrittenMasterSafeOffset(tp)`,
   *      `lastReturnedSafeOffset(tp)`, and `forceWriteAfterCleanUp(tp)` for this TP.
   *   5. Close writers for this TP and remove them from the writers map.
   *   6. Evict granular-lock cache entries for this TP.
   *
   * The reordering invariant (compute → write → clear → close) is mandatory: closing
   * writers first would compute `globalSafeOffset` against an empty writers map and the
   * force write would either no-op or persist a value disconnected from the durable
   * floor at the moment of revocation.
   */
  private def closePartition(topicPartition: TopicPartition, reason: ForcedWriteReason): Unit = {
    val tpWriters = writersForTopicPartition(topicPartition)
    // Step 1 + 2 + 3: attempt force write if dirty AND we still have writers to compute
    // a meaningful `globalSafeOffset` from. If the TP has no writers (e.g. close() called
    // on an instance where this TP was never written to), there is no advancement to
    // persist and the force-on-revoke path is a no-op for this TP.
    //
    // The force-write helper only returns Eithers from `updateMasterLock` / `cleanUpObsoleteLocks`,
    // so a throw can only come from a programmer error or a JVM-level fault. Wrap defensively
    // so that on the close path the per-TP state clear, writer close, and granular-cache eviction
    // (steps 4–6 below) always run — leaving stale writers or HWM entries behind would compound
    // a fault into an operational outage on the next ownership episode.
    if (tpWriters.nonEmpty) {
      try {
        attemptForceMasterLockWrite(topicPartition, tpWriters, reason)
      } catch {
        case NonFatal(t) =>
          logger.warn(
            s"[${connectorTaskId.show}] Force-${reason} master-lock write threw unexpectedly for " +
              s"$topicPartition; proceeding with state cleanup. The next owner / restart will replay " +
              s"from the durable master floor (deduplicated by granular locks).",
            t,
          )
      }
    }
    // Step 4: clear all per-TP state. Use `remove` (not `clear()`) to avoid wiping state
    // for other partitions that share this WriterManager instance. The lifecycle-table
    // rule for `close(tp)` is: clear every new field for this TP.
    safeOffsetHighWatermarks.remove(topicPartition)
    lastWrittenMasterSafeOffset.remove(topicPartition)
    lastReturnedSafeOffset.remove(topicPartition)
    forceWriteAfterCleanUp.remove(topicPartition)
    // Step 5: close writers for this TP and remove them. The iteration is materialised to
    // a list first so the mutation does not invalidate the view.
    val keysToRemove = writers
      .view.filterKeys(_.topicPartition == topicPartition)
      .keys
      .toList
    keysToRemove.foreach { key =>
      writers.get(key).foreach(_.close())
      writers.remove(key)
    }
    // Step 6: evict granular lock cache entries for this TP. Writer.close() deliberately
    // does NOT evict cache entries (they are left for cleanUpObsoleteLocks to GC), so this
    // bulk eviction is the sole memory cleanup path on shutdown.
    indexManager.evictAllGranularLocks(topicPartition)
  }

  /**
   * Force-on-revoke / force-on-stop helper. Computes `globalSafeOffset` from the current
   * writers + HWM, attempts `updateMasterLock` once if `dirtyMasterLock(tp)` is true, and
   * applies the post-write state updates per the force-on-revoke ordering steps 1–4
   * documented on `closePartition` above.
   *
   * Best-effort, non-blocking: a single synchronous attempt with no in-process retry. A
   * transient or fencing failure here is acceptable — force-on-revoke failure is not a fatal
   * path; the next owner / restart replays from the older durable master floor, deduplicated
   * by granular locks. The forced-write metric counter is incremented on every attempt
   * regardless of outcome.
   */
  private def attemptForceMasterLockWrite(
    topicPartition: TopicPartition,
    tpWriters:      Seq[Writer[SM]],
    reason:         ForcedWriteReason,
  ): Unit = {
    val firstBufferedOffsets = tpWriters.flatMap(_.getFirstBufferedOffset)
    val committedOffsets     = tpWriters.flatMap(_.getCommittedOffset)
    if (committedOffsets.isEmpty) {
      // Nothing has been durably committed by these writers yet; there is no
      // `globalSafeOffset` to persist. Skip the force attempt — the durable master lock
      // floor (which the next owner will read in `open(tp)`) is already authoritative.
      logger.debug(
        s"[${connectorTaskId.show}] Force-${reason} skipped for $topicPartition: " +
          s"no committed offsets (writers have not yet flushed); durable master-lock floor is authoritative.",
      )
    } else {
      val calculatedSafeOffset =
        if (firstBufferedOffsets.nonEmpty) firstBufferedOffsets.map(_.value).min
        else committedOffsets.map(_.value).max + 1
      val previousHighWatermark = currentHighWatermark(topicPartition)
      val globalSafeOffset      = math.max(calculatedSafeOffset, previousHighWatermark)
      val lastWritten           = currentLastWrittenMaster(topicPartition)
      val dirty                 = globalSafeOffset > lastWritten

      val hasPartitionByWriters =
        writers.keys.exists(k => k.topicPartition == topicPartition && k.partitionValues.nonEmpty)

      if (hasPartitionByWriters && dirty) {
        metrics.incrementMasterLockWriteForced(reason)
        indexManager.updateMasterLock(topicPartition, Offset(globalSafeOffset)) match {
          case Left(err) =>
            metrics.incrementMasterLockFailures()
            logger.warn(
              s"[${connectorTaskId.show}] Force-${reason} updateMasterLock failed for $topicPartition: " +
                s"${err.message()} — proceeding without forced persistence. The next owner / restart will " +
                s"replay from the older durable master floor (deduplicated by granular locks).",
            )
          case Right(_) =>
            metrics.incrementMasterLockUpdates()
            lastWrittenMasterSafeOffset.put(topicPartition, globalSafeOffset)
            // Compute the active partition-key set from the *current* writers map (the force
            // path runs before writers are closed, so the set is still meaningful).
            val activePartitionKeys: Set[String] = writers
              .filter { case (key, _) => key.topicPartition == topicPartition }
              .keys
              .flatMap(key => WriterManager.derivePartitionKey(key.partitionValues))
              .toSet
            indexManager.cleanUpObsoleteLocks(topicPartition, Offset(globalSafeOffset), activePartitionKeys) match {
              case Left(gcErr) =>
                logger.warn(
                  s"[${connectorTaskId.show}] Best-effort GC after force-${reason} failed for $topicPartition: " +
                    s"${gcErr.message()}",
                )
              case Right(_) =>
            }
        }
      } else if (hasPartitionByWriters) {
        // Nothing to persist — the durable floor already matches the in-memory value.
        logger.debug(
          s"[${connectorTaskId.show}] Force-${reason} skipped for $topicPartition: " +
            s"globalSafeOffset=$globalSafeOffset already persisted (lastWritten=$lastWritten).",
        )
      }
      // else: non-PARTITIONBY mode — master lock is maintained inside `Writer.commit()` via
      // `IndexManager.update`. There is nothing to force from the WriterManager layer.
    }
  }

  /** Reads the current HWM for the TP without seeding (returns 0 if absent). */
  private def currentHighWatermark(topicPartition: TopicPartition): Long =
    safeOffsetHighWatermarks.getOrElse(topicPartition, 0L)

  /**
   * Reads `lastWrittenMasterSafeOffset(tp)` if already seeded; otherwise lazy-seeds from the
   * durable master-lock floor (`getSeekedOffsetForTopicPartition + 1`, or 0 if absent).
   * Mirrors the lazy-seed used by `safeOffsetHighWatermarks` so the dirty-flag gate cannot
   * fire on a fresh ownership episode before the master lock has been read.
   */
  private def currentLastWrittenMaster(topicPartition: TopicPartition): Long =
    lastWrittenMasterSafeOffset.getOrElseUpdate(
      topicPartition,
      indexManager.getSeekedOffsetForTopicPartition(topicPartition)
        .map(_.value + 1)
        .getOrElse(0L),
    )

  /**
   * `close()` (rebalance / shutdown) — bulk per-TP iteration that runs the force-on-revoke
   * routine for every owned partition before closing writers. The per-TP routine is the
   * same regardless of revoke vs. stop; only the reason tag differs for metrics. The
   * default `Revoke` tag matches the call site `CloudSinkTask.close(partitions)`.
   */
  def close(reason: ForcedWriteReason = ForcedWriteReason.Revoke): Unit = {
    logger.debug(s"[{}] Received call to WriterManager.close", connectorTaskId.show)
    val topicPartitions = writers.keys.map(_.topicPartition).toSet
    topicPartitions.foreach(closePartition(_, reason))
    // Defensive bulk clear: a `cleanUp(tp)` that runs immediately before close with no
    // intervening `put()` to re-create writers for that TP leaves the TP absent from
    // `writers.keys`, so `closePartition` above does not visit it. The preserved
    // `lastReturnedSafeOffset(tp)` and the set `forceWriteAfterCleanUp(tp)` entry would
    // otherwise survive into the next ownership episode of this task instance.
    //
    // Safety argument (PARTITIONBY mode): `lastReturned(tp) <= durableFloor(tp) + 1` is
    // an invariant provable at the two PARTITIONBY `advanceLastReturned` call sites (Skip
    // branch and `attemptMasterLockWrite` success). Therefore the HWM re-seed expression
    // `max(durableFloor+1, lastReturned)` always equals `durableFloor+1` after `open()`
    // refreshes the seeked offsets — the leaked `lastReturned` entry cannot inflate the
    // HWM beyond the durable floor.
    //
    // Safety argument (non-PARTITIONBY mode): the non-PARTITIONBY `advanceLastReturned`
    // call site (line 638) CAN sit ahead of the durable floor, because `Writer.commit` is
    // the master-lock authority in that mode and `preCommit` does not write the master
    // lock. However, `closePartition`'s `attemptForceMasterLockWrite` short-circuits via
    // `!hasPartitionByWriters`, so no stale eTag is persisted, and the next ownership
    // episode reads the authoritative durable floor through `open()`. The bulk clear here
    // ensures the leaked `lastReturned` does not inject a false HWM on the first
    // post-reopen `preCommit` for that TP.
    //
    // The asymmetry is a hygiene smell and a trap for future refactors that relax either
    // invariant above — the bulk clear mirrors the pre-Option-A `safeOffsetHighWatermarks
    // .clear()` semantics and makes the lifecycle table self-consistent: `close()` is a
    // hard reset of all per-TP state.
    //
    // Safe because `closePartition` already drained state for every TP that had writers;
    // the bulk clear is a no-op for those TPs and the defensive fix for leaked-state TPs.
    safeOffsetHighWatermarks.clear()
    lastWrittenMasterSafeOffset.clear()
    lastReturnedSafeOffset.clear()
    forceWriteAfterCleanUp.clear()
    // After every per-TP closePartition has run, refresh the writer-count gauge for the
    // final value (in the steady-state path writers are now empty).
    metrics.setWriterCount(writers.size)
  }

  /**
   * Equivalent to `close(Stop)` — surfaced as a distinct method so `CloudSinkTask.stop()`
   * can opt in to the `Stop` reason tag without making the default API
   * change-controlled. Mirrors the per-TP routine used by `close(Revoke)`; the difference
   * is purely in which metric counter is incremented.
   */
  def closeForStop(): Unit = close(ForcedWriteReason.Stop)

  def write(topicPartitionOffset: TopicPartitionOffset, messageDetail: MessageDetail): Either[SinkError, Unit] = {

    logger.debug(
      s"[${connectorTaskId.show}] Received call to WriterManager.write for ${topicPartitionOffset.topic}-${topicPartitionOffset.partition}:${topicPartitionOffset.offset}",
    )
    for {
      writer    <- writer(topicPartitionOffset.toTopicPartition, messageDetail)
      shouldSkip = writer.shouldSkip(topicPartitionOffset.offset)
      resultIfNotSkipped <-
        if (!shouldSkip) {
          transformerF(messageDetail).leftMap(ex =>
            new FatalCloudSinkError(ex.getMessage, ex.some, topicPartitionOffset.toTopicPartition),
          ).flatMap { transformed =>
            writeAndCommit(topicPartitionOffset, transformed, writer)
          }
        } else {
          ().asRight
        }
    } yield resultIfNotSkipped
  }

  private def writeAndCommit(
    topicPartitionOffset: TopicPartitionOffset,
    messageDetail:        MessageDetail,
    writer:               Writer[SM],
  ): Either[SinkError, Unit] =
    for {
      // commitException can not be recovered from
      _ <- rollOverTopicPartitionWriters(writer, topicPartitionOffset.toTopicPartition, messageDetail)
      // a processErr can potentially be recovered from in the next iteration.  Can be due to network problems
      _         <- writer.write(messageDetail)
      commitRes <- writerCommitManager.commitFlushableWritersForTopicPartition(topicPartitionOffset.toTopicPartition)
    } yield commitRes

  private def rollOverTopicPartitionWriters(
    writer:         Writer[SM],
    topicPartition: TopicPartition,
    message:        MessageDetail,
  ): Either[BatchCloudSinkError, Unit] =
    //TODO: fix this; it cannot always be VALUE and it depends on writer requiring a roll over to new file
    message.value.schema() match {
      case Some(value: Schema) if writer.shouldRollover(value) =>
        // Schema rollover is the explicit full-fan-out path: every writer on the
        // `TopicPartition` must flush together so the format boundary stays consistent
        // (a sibling that keeps buffering past a schema change would mix incompatible
        // records into a single output file). Flush-trigger and pending-retry paths use
        // the selective commit methods on `WriterCommitManager`; this one deliberately
        // does not. Do NOT weaken to selective without a separate format-compatibility
        // review — the regression is pinned by `WriterCommitManagerTest "commitForTopicPartition
        // should commit all writers for the given topic partition if one is committed"`.
        writerCommitManager.commitForTopicPartition(topicPartition)
      case _ => ().asRight
    }

  private def processPartitionValues(
    messageDetail:  MessageDetail,
    keyNamer:       KeyNamer,
    topicPartition: TopicPartition,
  ): Either[SinkError, immutable.Map[PartitionField, String]] =
    keyNamer.processPartitionValues(messageDetail, topicPartition)

  /**
   * Returns a writer that can write records for a particular topic and partition.
   * The writer will create a file inside the given directory if there is no open writer.
   */
  private def writer(topicPartition: TopicPartition, messageDetail: MessageDetail): Either[SinkError, Writer[SM]] =
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      keyNamer        <- keyNamerFn(topicPartition)
      partitionValues <- processPartitionValues(messageDetail, keyNamer, topicPartition)
      key              = MapKey(topicPartition, partitionValues)
      maybeWriter      = writers.get(key)
      writer <- maybeWriter match {
        case Some(w) => w.asRight
        case None =>
          createWriter(bucketAndPrefix, topicPartition, partitionValues)
            .map { w =>
              writers.put(key, w)
              evictIdleWriters(key.topicPartition, Some(key))
              metrics.setWriterCount(writers.size)
              w
            }
      }
    } yield writer

  private def createWriter(
    bucketAndPrefix: CloudLocation,
    topicPartition:  TopicPartition,
    partitionValues: Map[PartitionField, String],
  ): Either[SinkError, Writer[SM]] = {
    logger.debug(s"[${connectorTaskId.show}] Creating new writer for bucketAndPrefix:$bucketAndPrefix")
    val partitionKey = WriterManager.derivePartitionKey(partitionValues)
    for {
      commitPolicy <- commitPolicyFn(topicPartition)
      _            <- partitionKey.fold(().asRight[SinkError])(pk => indexManager.ensureGranularLock(topicPartition, pk))
      lastSeekedOffset <- partitionKey match {
        case Some(pk) =>
          // Granular-lock-first, master-lock-fallback: load the per-writer granular lock offset.
          // If no granular lock exists (new partition key, or lock GC'd/swept), fall back to the
          // master lock offset as a deduplication floor. This prevents data duplication when:
          //  (a) a lock is legitimately deleted by GC/sweep and a new writer is later created
          //      for the same partition key (e.g., the same date reappears in incoming data),
          //  (b) an operator manually rewinds the consumer to reprocess historical data, or
          //  (c) the GC threshold is accidentally loosened in a future change.
          //
          // When globalSafeOffset == 0, updateMasterLock stores None (not Some(Offset(0))), so
          // getSeekedOffsetForTopicPartition returns None and the fallback produces
          // None.orElse(None) = None -- no false skip of offset 0.
          //
          // History: this fallback was removed in f2e6906ad to work around a since-fixed bug
          // where updateMasterLock stored Some(Offset(0)) when globalSafeOffset == 0, causing
          // false skips. Commit 319b7be6f fixed the root cause (storing None instead), making
          // the fallback safe again.
          indexManager.getSeekedOffsetForPartitionKey(topicPartition, pk).map {
            granularOffset =>
              granularOffset.orElse(indexManager.getSeekedOffsetForTopicPartition(topicPartition))
          }
        case None =>
          indexManager.getSeekedOffsetForTopicPartition(topicPartition).asRight[SinkError]
      }
    } yield {
      new Writer(
        topicPartition,
        commitPolicy,
        indexManager,
        () => stagingFilenameFn(topicPartition, partitionValues),
        objKeyBuilderFn(topicPartition, partitionValues),
        formatWriterFn.curried(topicPartition),
        schemaChangeDetector,
        pendingOperationsProcessors,
        partitionKey,
        lastSeekedOffset,
      )
    }
  }

  def preCommit(
    currentOffsets: immutable.Map[TopicPartition, OffsetAndMetadata],
  ): immutable.Map[TopicPartition, OffsetAndMetadata] =
    currentOffsets
      .flatMap { case (tp, offAndMeta) =>
        getOffsetAndMeta(tp, offAndMeta).map(tp -> _)
      }

  private def writersForTopicPartition(topicPartition: TopicPartition): Seq[Writer[SM]] =
    writers
      .collect {
        case (key, writer) if key.topicPartition == topicPartition => writer
      }
      .toSeq

  private def getOffsetAndMeta(
    topicPartition:    TopicPartition,
    offsetAndMetadata: OffsetAndMetadata,
  ): Option[OffsetAndMetadata] = {
    val tpWriters = writersForTopicPartition(topicPartition)
    if (tpWriters.isEmpty) {
      None
    } else {
      val firstBufferedOffsets = tpWriters.flatMap(_.getFirstBufferedOffset)
      val committedOffsets     = tpWriters.flatMap(_.getCommittedOffset)

      // Operator-facing signal: how many writers are currently holding the safe-offset
      // barrier (i.e. have buffered but uncommitted data). Under selective commit the
      // BarrierSet is unchanged from before — every active writer on the TP still bounds
      // `globalSafeOffset` — but the CommitSet is narrower, so the gauge tells operators
      // which workloads are pinning consumer-lag growth and need `flush.interval` tuning.
      metrics.setSafeOffsetBarrierWriters(firstBufferedOffsets.size)

      if (committedOffsets.isEmpty) {
        None
      } else {
        val calculatedSafeOffset =
          if (firstBufferedOffsets.nonEmpty) {
            firstBufferedOffsets.map(_.value).min
          } else {
            committedOffsets.map(_.value).max + 1
          }

        // HWM re-seed after `cleanUp(tp)`: the lazy-seed expression combines the durable
        // master-lock floor with `lastReturnedSafeOffset(tp)`.
        //
        // The first term anchors recovery across the ownership-episode boundary.
        //
        // The second term's role differs by mode:
        //  - Non-PARTITIONBY (load-bearing): `preCommit` and `Writer.commit()` advance
        //    `lastReturned` and `seekedOffsets(tp)` on independent schedules. After a
        //    `cleanUp(tp)`, `lastReturned` can sit ahead of `durableFloor + 1`, so this
        //    term is what prevents returning a lower offset than Kafka Connect was already told.
        //  - PARTITIONBY (defensive): `IndexManagerV2.updateMasterLock` updates `seekedOffsets(tp)`
        //    atomically on each successful write, so `durableFloor + 1 == lastReturned` in
        //    production. The `lastReturned` term is redundant in value today but is the safety
        //    net if a future `IndexManager` implementation decouples the two.
        //
        // Both terms must be evaluated together regardless; branching on mode or on whether
        // `cleanUp(tp)` recently ran would let a future refactor silently drop the guard.
        val previousHighWatermark = safeOffsetHighWatermarks.getOrElseUpdate(
          topicPartition,
          math.max(
            indexManager.getSeekedOffsetForTopicPartition(topicPartition).map(_.value + 1).getOrElse(0L),
            lastReturnedSafeOffset.getOrElse(topicPartition, 0L),
          ),
        )
        val globalSafeOffset = math.max(calculatedSafeOffset, previousHighWatermark)

        // Defensive invariants: today these are guaranteed by the math.max above, but a require()
        // here means any future refactor that drops or reorders the max will fail fast in CI
        // rather than silently regressing the globalSafeOffset and risking data loss.
        require(
          globalSafeOffset >= previousHighWatermark,
          s"globalSafeOffset regression for $topicPartition: $globalSafeOffset < previousHWM $previousHighWatermark",
        )
        require(
          globalSafeOffset >= calculatedSafeOffset,
          s"globalSafeOffset below calculated floor for $topicPartition: $globalSafeOffset < $calculatedSafeOffset",
        )

        val hasPartitionByWriters =
          writers.keys.exists(k => k.topicPartition == topicPartition && k.partitionValues.nonEmpty)

        val shouldReturnOffset: Boolean =
          if (hasPartitionByWriters) {
            // PARTITIONBY mode: classify the cycle (Skip / WriteRoutine / WriteForced) and let the
            // helper drive the master-lock write decision. The helper reads the just-computed
            // `globalSafeOffset` from a single local and reuses it for both the `updateMasterLock`
            // call and the post-success GC call (same-value GC threshold invariant).
            val lastWritten = currentLastWrittenMaster(topicPartition)
            val dirty       = globalSafeOffset > lastWritten

            // Dirty-window-cycle counter: incremented every cycle that observes a dirty TP on
            // entry (whether or not the cycle's write succeeds). Under the dirty-flag gate's
            // success path this fires at most once per advance; sustained non-zero values
            // indicate `updateMasterLock` is failing across multiple retries.
            if (dirty) metrics.incrementMasterLockDirtyWindowCycle()

            val decision: WriteDecision =
              if (forceWriteAfterCleanUp.contains(topicPartition))
                WriteDecision.WriteForced(ForcedWriteReason.PostCleanUp)
              else if (dirty) WriteDecision.WriteRoutine
              else WriteDecision.Skip

            decision match {
              case WriteDecision.Skip =>
                // Structural invariant: on the Skip branch under the dirty-flag-only gate,
                // `globalSafeOffset == lastWritten` (no advance to persist). The `require` pins this
                // so a future refactor that loosens the gate (e.g. adds a delta threshold) fails
                // fast in CI before it can silently change HWM-advance, GC-threshold, or
                // dirty-window semantics elsewhere in the design.
                require(
                  globalSafeOffset == lastWritten,
                  s"Skip branch invariant violated for $topicPartition: globalSafeOffset=$globalSafeOffset, " +
                    s"lastWritten=$lastWritten — the dirty-flag gate requires equality here.",
                )
                metrics.incrementMasterLockWriteSkipped()
                // HWM and lastReturned still advance (no-op in value terms on this branch, but
                // structurally identical to the success branch so the code paths cannot drift).
                safeOffsetHighWatermarks.put(topicPartition, globalSafeOffset)
                advanceLastReturned(topicPartition, globalSafeOffset)
                logger.debug(
                  s"[${connectorTaskId.show}] Master-lock write skipped for $topicPartition: " +
                    s"globalSafeOffset=$globalSafeOffset already persisted.",
                )
                true

              case WriteDecision.WriteRoutine =>
                attemptMasterLockWrite(topicPartition, globalSafeOffset, forcedReason = None)

              case WriteDecision.WriteForced(reason) =>
                // Clear the post-cleanUp force flag *before* attempting the write so a failure
                // still leaves the routine dirty-flag gate as the next-cycle re-entry path
                // — if the forced write fails the dirty bit stays true, no GC runs, and the
                // next eligible cycle re-attempts.
                if (reason == ForcedWriteReason.PostCleanUp) forceWriteAfterCleanUp.remove(topicPartition)
                attemptMasterLockWrite(topicPartition, globalSafeOffset, forcedReason = Some(reason))
            }
          } else {
            // Non-PARTITIONBY mode: Writer.commit() already maintains the master lock via
            // indexManager.update(), so skip the redundant cloud write. HWM and lastReturned
            // still advance — this is the equivalent of the Skip branch for non-PARTITIONBY
            // mode (Writer.commit() is the master-lock authority, so preCommit only advances
            // in-memory state here).
            safeOffsetHighWatermarks.put(topicPartition, globalSafeOffset)
            advanceLastReturned(topicPartition, globalSafeOffset)
            // Defensive: `cleanUp(tp)` sets `forceWriteAfterCleanUp(tp)` unconditionally, but only
            // the PARTITIONBY branch above consumes it. Connector mode is fixed at runtime, so a
            // mode flip within a single ownership episode is not expected — clearing the flag
            // here is a belt-and-braces guard so a future refactor that introduces a mixed mode
            // cannot leave a stale flag pinned across cycles. In non-PARTITIONBY mode the force
            // point is a no-op (master lock is maintained inside Writer.commit()), so clearing
            // the flag here is safe.
            forceWriteAfterCleanUp.remove(topicPartition)
            true
          }

        if (shouldReturnOffset)
          Some(new OffsetAndMetadata(
            globalSafeOffset,
            offsetAndMetadata.leaderEpoch(),
            offsetAndMetadata.metadata(),
          ))
        else
          None
      }
    }
  }

  /**
   * Attempt a single `updateMasterLock` write (routine or forced) for the given TP and
   * `globalSafeOffset`. Returns `true` if the write succeeded (and the caller may return an
   * advancing offset to Kafka Connect), `false` if the write failed (caller must return
   * `None` for the TP).
   *
   * On success: increment the appropriate counter (routine `masterLockUpdates`, or
   * `masterLockWriteForced(reason)`), advance `safeOffsetHighWatermarks(tp)`,
   * `lastReturnedSafeOffset(tp)`, and `lastWrittenMasterSafeOffset(tp)`, then run
   * `cleanUpObsoleteLocks` against the *same* just-persisted value (same-value GC threshold
   * invariant — `globalSafeOffset` is read into a single local and reused for both calls).
   *
   * On failure: increment `masterLockFailures`, log, and return `false`. Do not touch any
   * of the new state fields — the dirty bit stays true (computed) for the next retry, and
   * the orphan-sweep threshold (`seekedOffsets(tp)`) stays pinned to the durable floor.
   */
  private def attemptMasterLockWrite(
    topicPartition:   TopicPartition,
    globalSafeOffset: Long,
    forcedReason:     Option[ForcedWriteReason],
  ): Boolean = {
    // Defensive entry invariant — matches the monotonic-max math in `getOffsetAndMeta`
    // (lines 526–533 and 565–569). Every call site classifies the cycle via the WriteDecision
    // helper, which only routes to `WriteRoutine` / `WriteForced` when the value can validly
    // be written. A future refactor that introduces a non-monotonic call site (e.g. forcing a
    // write with a stale local) would fail loudly here rather than driving
    // `cleanUpObsoleteLocks` past still-needed granular locks and silently regressing the
    // exactly-once dedup floor.
    val priorLastWritten = currentLastWrittenMaster(topicPartition)
    require(
      globalSafeOffset >= priorLastWritten,
      s"attemptMasterLockWrite invariant violated for $topicPartition: " +
        s"globalSafeOffset=$globalSafeOffset < lastWritten=$priorLastWritten — " +
        s"writing a regression would drive same-value GC past locks the next owner needs.",
    )
    forcedReason.foreach(metrics.incrementMasterLockWriteForced)
    indexManager.updateMasterLock(topicPartition, Offset(globalSafeOffset)) match {
      case Left(err) =>
        metrics.incrementMasterLockFailures()
        logger.error(
          s"[${connectorTaskId.show}] Master lock update failed for $topicPartition " +
            s"(forced=${forcedReason.map(_.toString).getOrElse("no")}): ${err.message()}. " +
            s"Returning no offset to prevent consumer advance.",
        )
        false
      case Right(_) =>
        // `masterLockUpdates` is the universal "writes that succeeded" counter — incremented on
        // every successful eTag-conditional persistence regardless of routine vs. forced. Forced
        // writes additionally incremented `masterLockWriteForced(reason)` at the top of this
        // method so operators can break the success rate down by reason.
        metrics.incrementMasterLockUpdates()
        safeOffsetHighWatermarks.put(topicPartition, globalSafeOffset)
        advanceLastReturned(topicPartition, globalSafeOffset)
        lastWrittenMasterSafeOffset.put(topicPartition, globalSafeOffset)
        logger.debug(
          s"[${connectorTaskId.show}] Updated master lock for $topicPartition with globalSafeOffset=$globalSafeOffset" +
            forcedReason.map(r => s" (forced=$r)").getOrElse(""),
        )
        val activePartitionKeys: Set[String] = writers
          .filter { case (key, _) => key.topicPartition == topicPartition }
          .keys
          .flatMap(key => WriterManager.derivePartitionKey(key.partitionValues))
          .toSet
        // Same-value GC threshold: pass the *just-persisted* `globalSafeOffset` local.
        indexManager.cleanUpObsoleteLocks(topicPartition, Offset(globalSafeOffset), activePartitionKeys) match {
          case Left(err) =>
            logger.warn(s"[${connectorTaskId.show}] Best-effort GC failed for $topicPartition: ${err.message()}")
          case Right(_) =>
        }
        true
    }
  }

  /**
   * Monotonic-max update of `lastReturnedSafeOffset(tp)`. The map stores the highest
   * `globalSafeOffset` ever returned to Kafka Connect for the TP within the current
   * ownership episode. Lifecycle table invariant: this field NEVER moves backwards.
   *
   * Role: load-bearing in non-PARTITIONBY mode (where `preCommit` and `Writer.commit()`
   * advance `lastReturned` and `seekedOffsets(tp)` on independent schedules, so `lastReturned`
   * can sit ahead of `durableFloor + 1` between flushes); defensive in PARTITIONBY mode
   * (where `IndexManagerV2.updateMasterLock` keeps `seekedOffsets(tp)` in lockstep with
   * successful writes, making the two terms in `max(durableFloor + 1, lastReturned)` equal
   * in production — the field is the safety net against a future `IndexManager` implementation
   * that decouples the two).
   *
   * Structural call-ordering invariant (pinned by `require`): every advance MUST be
   * immediately preceded by a `safeOffsetHighWatermarks.put(tp, globalSafeOffset)` for
   * the same value. The two fields are intended to advance in lockstep — `HWM(tp)` is
   * the in-memory monotonicity defence within the current ownership episode,
   * `lastReturnedSafeOffset(tp)` is the cross-`cleanUp(tp)` survivor used to re-seed
   * the HWM after rollback. The `close()` bulk-clear safety argument depends on
   * `lastReturned(tp) == HWM(tp)` at every advance — a future refactor that introduces
   * a call site that advances `lastReturned` without first advancing `HWM` would
   * silently break the invariant the bulk-clear comment relies on.
   */
  private def advanceLastReturned(topicPartition: TopicPartition, globalSafeOffset: Long): Unit = {
    require(
      safeOffsetHighWatermarks.get(topicPartition).contains(globalSafeOffset),
      s"advanceLastReturned must follow safeOffsetHighWatermarks.put for $topicPartition: " +
        s"globalSafeOffset=$globalSafeOffset, HWM=${safeOffsetHighWatermarks.get(topicPartition)} — " +
        s"the two fields must advance in lockstep so the close() bulk-clear safety argument " +
        s"(lastReturned(tp) <= durableFloor(tp) + 1) survives any future refactor.",
    )
    val previous = lastReturnedSafeOffset.getOrElse(topicPartition, 0L)
    if (globalSafeOffset > previous) {
      val _ = lastReturnedSafeOffset.put(topicPartition, globalSafeOffset)
    }
  }

  def cleanUp(topicPartition: TopicPartition): Unit = {
    val keysToRemove = writers
      .view.filterKeys(_.topicPartition == topicPartition)
      .keys
      .toList
    keysToRemove.foreach { key =>
      writers.get(key).foreach(_.close())
      writers.remove(key)
    }
    safeOffsetHighWatermarks.remove(topicPartition)
    // `cleanUp(tp)` must preserve `lastReturnedSafeOffset(tp)`.
    //
    // PARTITIONBY mode (defensive): `IndexManagerV2.updateMasterLock` updates `seekedOffsets(tp)`
    // atomically with each successful write, so `durableFloor + 1 == lastReturned` in production
    // after every successful cycle. In that steady state the `lastReturned` term in the HWM
    // re-seed `max(durableFloor + 1, lastReturned)` is redundant in value. It is kept because
    // the `IndexManager` trait does NOT enforce the lockstep contract — a future implementation
    // that decouples `updateMasterLock` from `seekedOffsets` would silently break post-`cleanUp`
    // monotonicity without it. Preserving the field is the safety net against that drift.
    //
    // Non-PARTITIONBY mode (load-bearing): `Writer.commit()` (via `indexManager.update(...)`)
    // and `preCommit` advance `seekedOffsets(tp)` and `lastReturned` on independent schedules.
    // Between flushes `lastReturned` sits ahead of `durableFloor + 1`, so the HWM re-seed
    // `max(durableFloor + 1, lastReturned)` genuinely needs the `lastReturned` term to prevent
    // returning a lower offset to Kafka Connect than this task already returned. The
    // post-cleanup `preCommit` does NOT invoke a forced master-lock write in this mode (the
    // `getOffsetAndMeta` non-PARTITIONBY branch clears `forceWriteAfterCleanUp(tp)` and
    // advances HWM only); `lastReturnedSafeOffset(tp)` is the sole surviving record of what
    // was previously told to Kafka and is consumed directly by the monotonic HWM seed.
    lastWrittenMasterSafeOffset.remove(topicPartition)
    forceWriteAfterCleanUp.add(topicPartition)
    // Evict the granular lock cache so a fresh writer for this partition reloads its
    // dedup floor from storage rather than reading stale cached offsets.
    indexManager.evictAllGranularLocks(topicPartition)
    // NOTE: clearTopicPartitionState is deliberately NOT called here, mirroring the same
    // decision in close(). cleanUp is invoked from CloudSinkTask.rollback when a put()
    // surfaced a FatalCloudSinkError -- the partition is still owned by this task instance
    // and the master-lock state in storage has not changed. Clearing seekedOffsets and the
    // master eTag would leave the next updateMasterLock with no eTag to fence on, and
    // PARTITIONBY preCommit would permanently fail with "Master index not found" until the
    // next rebalance/restart. Partition revocation goes through IndexManagerV2.open()
    // (its stalePartitions and failure-rollback branches), which are the only legitimate
    // callers of clearTopicPartitionState.
  }

  private def evictIdleWriters(topicPartition: TopicPartition, exclude: Option[MapKey]): Unit = {
    val idleEntries = writers.iterator
      .filter { case (k, writer) => k.topicPartition == topicPartition && !exclude.contains(k) && writer.isIdle }
      .toList
    if (idleEntries.nonEmpty) {
      logger.debug(
        s"[${connectorTaskId.show}] Evicting ${idleEntries.size} idle writer(s) (map size ${writers.size})",
      )
    }
    idleEntries.foreach { case (key, writer) =>
      writer.close()
      writers.remove(key)
      WriterManager.derivePartitionKey(key.partitionValues).foreach { pk =>
        indexManager.evictGranularLock(key.topicPartition, pk)
      }
      metrics.incrementIdleWriterEvictions()
    }
    metrics.setWriterCount(writers.size)
  }

  private[writer] def writerCount: Int = writers.size

  private[writer] def putWriter(key: MapKey, writer: Writer[SM]): Unit = { val _ = writers.put(key, writer) }

  private[writer] def evictIdleWritersNow(topicPartition: TopicPartition): Unit =
    evictIdleWriters(topicPartition, None)

  def shouldSkipNullValues(): Boolean = skipNullValues

}

object WriterManager {

  def sanitize(value: String): String =
    URLEncoder.encode(value, "UTF-8").replace("_", "%5F")

  def derivePartitionKey(partitionValues: Map[PartitionField, String]): Option[String] =
    if (partitionValues.isEmpty) None
    else {
      val key = partitionValues.toSeq
        .sortBy(_._1.name())
        .map { case (field, value) => s"${sanitize(field.name())}=${sanitize(value)}" }
        .mkString("_")
      Some(key)
    }
}
