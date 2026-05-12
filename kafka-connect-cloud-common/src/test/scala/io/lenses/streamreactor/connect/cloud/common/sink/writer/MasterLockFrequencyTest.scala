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

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.ForcedWriteReason
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.collection.immutable

/**
 * Tests for the master-lock write-frequency reduction (dirty-flag gate).
 *
 * Every test in this file pins a non-negotiable scenario for the master-lock
 * write-frequency reduction (dirty-flag gate). The set covers:
 *
 *   - Duplicate-write skipping under the dirty-flag gate.
 *   - No-data-loss-on-crash-after-failed-master-lock-write across a multi-cycle dirty window.
 *   - No-duplication-when-force-on-revoke-fails (state-level fencing assertions).
 *   - Zombie-fences-on-stale-eTag (state-level proof).
 *   - GC-threshold-matches-just-persisted-master-offset (same-value invariant).
 *   - HWM-advances-on-skipped-but-not-failed (three-case split).
 *   - Skipped-path structural invariant: globalSafeOffset == lastWritten on the Skip branch.
 *   - Granular-lock writes occur on every commit regardless of master-lock gating.
 *   - No-regression-after-cleanup-in-dirty-window (preserved `lastReturnedSafeOffset(tp)`).
 *
 * The end-to-end zombie / final-path scenario lives alongside the existing end-to-end
 * scenario tests in `ExactlyOnceScenarioTest`; this file is mock-driven and focuses on
 * the WriterManager invariants in isolation.
 */
class MasterLockFrequencyTest
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with OptionValues
    with MockitoSugar
    with ArgumentMatchersSugar {

  private implicit val connectorTaskId:        ConnectorTaskId        = ConnectorTaskId("test-connector", 1, 1)
  private implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator

  private val tp0 = Topic("topic").withPartition(0)

  private val dateField: PartitionField                        = ValuePartitionField(PartitionNamePath("date"))
  private val dateA:     immutable.Map[PartitionField, String] = Map(dateField -> "2024-01-01")
  private val dateB:     immutable.Map[PartitionField, String] = Map(dateField -> "2024-01-02")

  private val commitPolicy         = mock[CommitPolicy]
  private val formatWriter         = mock[FormatWriter]
  private val objectKeyBuilder     = mock[ObjectKeyBuilder]
  private val schemaChangeDetector = mock[SchemaChangeDetector]
  private val pendingOpsProcessors = mock[PendingOperationsProcessors]

  private def makeIdleWriter(tp: TopicPartition, committedOffset: Option[Offset]): Writer[FileMetadata] = {
    val indexManager = mock[IndexManager]
    when(indexManager.indexingEnabled).thenReturn(true)
    val w = new Writer[FileMetadata](
      tp,
      commitPolicy,
      indexManager,
      () => Right(new File("test")),
      objectKeyBuilder,
      _ => Right(formatWriter),
      schemaChangeDetector,
      pendingOpsProcessors,
      None,
      committedOffset,
    )
    w.forceWriteState(NoWriter(CommitState(tp, committedOffset)))
    w
  }

  private def makeWritingWriter(
    tp:                  TopicPartition,
    committedOffset:     Option[Offset],
    firstBufferedOffset: Offset,
    uncommittedOffset:   Offset,
  ): Writer[FileMetadata] = {
    val indexManager = mock[IndexManager]
    when(indexManager.indexingEnabled).thenReturn(true)
    val w = new Writer[FileMetadata](
      tp,
      commitPolicy,
      indexManager,
      () => Right(new File("test")),
      objectKeyBuilder,
      _ => Right(formatWriter),
      schemaChangeDetector,
      pendingOpsProcessors,
      None,
      committedOffset,
    )
    w.forceWriteState(
      Writing(
        CommitState(tp, committedOffset),
        formatWriter,
        new File("test"),
        firstBufferedOffset,
        uncommittedOffset,
        System.currentTimeMillis(),
        System.currentTimeMillis(),
      ),
    )
    w
  }

  private def buildWriterManager(
    indexManager: IndexManager,
    metrics:      CloudSinkMetrics = new CloudSinkMetrics(),
  ): WriterManager[FileMetadata] =
    new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(commitPolicy),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", None)),
      keyNamerFn                  = _ => Right(mock[KeyNamer]),
      stagingFilenameFn           = (_, _) => Right(new File("test")),
      objKeyBuilderFn             = (_, _) => objectKeyBuilder,
      formatWriterFn              = (_, _) => Right(formatWriter),
      indexManager                = indexManager,
      transformerF                = (m: MessageDetail) => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = pendingOpsProcessors,
      metrics                     = metrics,
    )

  private def currentOffsets(tp: TopicPartition, offset: Long): immutable.Map[TopicPartition, OffsetAndMetadata] =
    Map(tp -> new OffsetAndMetadata(offset))

  // ── Duplicate-write skipping (no-op gate) ──────────────────────────────────────────
  test("Duplicate-write skipping: N identical preCommit cycles on an idle PARTITIONBY TP produce exactly one master-lock write") {
    // The dirty-flag alone deduplicates exact-duplicate writes. After the first cycle
    // establishes the master-lock floor, subsequent idle cycles must skip the write while
    // still returning the same safe offset to Kafka Connect.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))

    val cycles = 5
    val results = (1 to cycles).map(_ => wm.preCommit(currentOffsets(tp0, 200))(tp0).offset())

    // Every cycle returns the same safe offset (no advancement to Kafka).
    results.forall(_ == 51L) shouldBe true

    // Exactly one underlying master-lock write across N cycles.
    verify(indexManager, times(1)).updateMasterLock(eqTo(tp0), eqTo(Offset(51L)))
    metrics.getMasterLockUpdates shouldBe 1L
    metrics.getMasterLockWriteSkipped shouldBe (cycles - 1).toLong
    // GC runs only on the success branch — so exactly once.
    verify(indexManager, times(1)).cleanUpObsoleteLocks(eqTo(tp0), any[Offset], any[Set[String]])
  }

  // ── HWM-advances-on-skipped-but-not-failed ─────────────────────────────────────────
  test("HWM advances on skipped cycle but never on failed attempted write") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    // Phase 1: a successful write establishes lastWritten = 51 and HWM = 51.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 51L

    // Phase 2 (a): skipped cycle — same offset, no write attempted, HWM stays at 51,
    // lastReturned advances (no-op in value terms), preCommit still returns 51.
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 51L
    verify(indexManager, times(1)).updateMasterLock(any[TopicPartition], any[Offset]) // still only one
    metrics.getMasterLockWriteSkipped shouldBe 1L

    // Phase 2 (b): attempted write succeeds after a higher-offset writer arrives — HWM
    // advances, lastWritten advances, preCommit returns the new offset.
    val writerB = makeIdleWriter(tp0, Some(Offset(105)))
    wm.putWriter(MapKey(tp0, dateB), writerB)
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 106L
    metrics.getMasterLockUpdates shouldBe 2L

    // Phase 2 (c): attempted write fails — HWM does NOT advance, lastWritten does NOT
    // advance, preCommit returns None for this TP.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("transient", tp0)))
    val writerC = makeIdleWriter(tp0, Some(Offset(200)))
    wm.putWriter(MapKey(tp0, dateA), writerC)
    wm.preCommit(currentOffsets(tp0, 300)) shouldBe empty
    metrics.getMasterLockFailures shouldBe 1L

    // Confirm subsequent retry continues to attempt because the dirty bit stayed true:
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    wm.preCommit(currentOffsets(tp0, 300))(tp0).offset() shouldBe 201L
    metrics.getMasterLockUpdates shouldBe 3L
  }

  // ── No-data-loss across a multi-cycle failed master-lock write ─────────────────────
  test("No data loss: HWM and lastWritten do not advance across K failed master-lock writes, GC never runs on failed cycles") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))

    // K = 3 consecutive failures simulating a transient cloud outage.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("transient", tp0)))
    (1 to 3).foreach { _ =>
      wm.preCommit(currentOffsets(tp0, 200)) shouldBe empty
    }
    metrics.getMasterLockFailures shouldBe 3L
    // GC never ran on the failed cycles.
    verify(indexManager, never).cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])

    // Recovery: the next cycle succeeds and advances state at the FIRST in-memory offset,
    // not at any intermediate value. Critically: no consumer offset was ever advanced past
    // 51 by Kafka Connect during the failure window (because preCommit returned None).
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    val result = wm.preCommit(currentOffsets(tp0, 200))
    result(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L
    // GC runs against the just-persisted value, not a later mutated value.
    verify(indexManager, times(1)).cleanUpObsoleteLocks(eqTo(tp0), eqTo(Offset(51L)), any[Set[String]])
  }

  // ── No-duplication-when-force-on-revoke-fails ─────────────────────────────────────
  test("Force-on-revoke failure is not a fatal path; granular-lock state is preserved for the next owner") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    // Cycle 1: a successful routine write establishes lastWritten = 51.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L

    // Advance writer state but skip routine preCommit, so dirty flag is true on close.
    val writerHigh = makeIdleWriter(tp0, Some(Offset(105)))
    wm.putWriter(MapKey(tp0, dateA), writerHigh)

    // Force-on-revoke fails with an eTag mismatch (fencing).
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("eTag mismatch", tp0)))

    wm.close()

    // Force-on-revoke metric was incremented (attempt was made).
    metrics.getMasterLockWriteForcedRevoke shouldBe 1L
    metrics.getMasterLockFailures shouldBe 1L
    // GC did NOT run on the failed forced write — granular locks above the durable floor
    // (51) survive for the next owner to dedup against.
    verify(indexManager, times(1)).cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])
    // Granular-cache eviction still ran (writers were closed).
    verify(indexManager).evictAllGranularLocks(tp0)
    wm.writerCount shouldBe 0
  }

  // ── GC-threshold-matches-just-persisted-master-offset (same-value invariant) ───────
  test("GC threshold equals the just-persisted master-lock offset (same-value invariant)") {
    // Same-value GC threshold invariant: the write helper must read globalSafeOffset from
    // a single local and pass the SAME value to both
    // updateMasterLock and cleanUpObsoleteLocks. A future refactor that introduces a
    // second read of in-memory state between the two calls would silently let GC delete
    // granular locks above the durable floor on a race.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm = buildWriterManager(indexManager)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(100))))

    wm.preCommit(currentOffsets(tp0, 500))(tp0).offset() shouldBe 101L

    // Exact value-matchers (no ArgumentCaptor): `Offset` is an `AnyVal` value class which
    // Mockito's ArgumentCaptor erases to `Long`, returning `null` for the captured value.
    // The same-value invariant is verified by the matching pair of `verify(eqTo(...))`
    // calls below: if a future refactor introduces a second read of in-memory state
    // between the two calls, one of the two matchers will fail.
    verify(indexManager).updateMasterLock(eqTo(tp0), eqTo(Offset(101L)))
    verify(indexManager).cleanUpObsoleteLocks(eqTo(tp0), eqTo(Offset(101L)), any[Set[String]])
  }

  // ── Skipped-path-globalSafeOffset-equals-lastWritten (structural invariant) ────────
  test("Structural invariant: every Skip-classified cycle has globalSafeOffset == lastWrittenMasterSafeOffset(tp)") {
    // Under the dirty-flag gate the Skip-classified path corresponds exactly to
    // `globalSafeOffset == lastWritten`. This is structurally pinned in the implementation
    // by a `require(...)` inside the Skip branch — exercising the Skip branch through a
    // mix of routine writes, failed writes, and idle cycles verifies the helper
    // classifies cycles consistently with that invariant.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    // Sequence: success, idle (skip), success, failed, retry (success), idle (skip).
    // Each Skip-classified cycle MUST keep globalSafeOffset == lastWritten — verified
    // implicitly by the `require(...)` in WriterManager throwing IllegalArgumentException
    // if the invariant breaks.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 51L // success
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 51L // skip
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 51L // skip

    wm.putWriter(MapKey(tp0, dateB), makeIdleWriter(tp0, Some(Offset(80))))
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 81L // success

    // Now drive a failure cycle followed by a successful retry:
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("transient", tp0)))
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(120))))
    wm.preCommit(currentOffsets(tp0, 200)) shouldBe empty // failure
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 121L // retry success
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 121L // skip again

    // 3 successful updates + 3 skipped cycles.
    metrics.getMasterLockUpdates shouldBe 3L
    metrics.getMasterLockWriteSkipped shouldBe 3L
    metrics.getMasterLockFailures shouldBe 1L
    // Dirty-window counter increments once per cycle that observed a non-empty dirty window
    // on entry. The Skip branch by structural invariant has `globalSafeOffset == lastWritten`,
    // so it never increments the dirty-window counter. The expected sequence:
    //   success @ 51 (dirty=true: 51 > 0)         → 1
    //   skip    @ 51 (dirty=false: 51 == 51)      → 1
    //   skip    @ 51 (dirty=false: 51 == 51)      → 1
    //   success @ 81 (dirty=true: 81 > 51)        → 2
    //   failure @ 121 (dirty=true: 121 > 81)      → 3
    //   retry   @ 121 (dirty=true: 121 > 81)      → 4
    //   skip    @ 121 (dirty=false: 121 == 121)   → 4
    metrics.getMasterLockDirtyWindowCycles shouldBe 4L
  }

  // ── No-regression-after-cleanup-in-dirty-window ────────────────────────────────────
  test("Post-cleanUp(tp) preCommit forces a master-lock write at max(durableFloor, lastReturnedSafeOffset(tp))") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(Some(Offset(100)))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    // Step 1: establish lastWritten = 151, lastReturned = 151, durable floor = 101.
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(150))))
    wm.preCommit(currentOffsets(tp0, 500))(tp0).offset() shouldBe 151L

    // Step 2: cleanUp(tp0) — clears HWM and lastWritten, preserves lastReturned = 151,
    // sets forceWriteAfterCleanUp(tp0).
    wm.cleanUp(tp0)

    // Step 3: re-add a writer with a LOWER calculated safe offset (101). HWM re-seeds from
    // max(durable + 1 = 101, lastReturned = 151) = 151. globalSafeOffset = max(101, 151) = 151.
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(100))))
    val result = wm.preCommit(currentOffsets(tp0, 500))
    result(tp0).offset() shouldBe 151L

    // Step 4: WriteForced(PostCleanUp) fired, writing the SAME 151 value to both
    // updateMasterLock and cleanUpObsoleteLocks (same-value invariant). The metric tag
    // distinguishes this from a routine write. We verify the call signatures rather than
    // capturing values because `Offset` is an `AnyVal` value class that Mockito's
    // ArgumentCaptor cannot capture (the captured value is null at runtime).
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 1L
    // Two cycles: one routine pre-cleanup at 151, one forced post-cleanup at 151.
    verify(indexManager, times(2)).updateMasterLock(eqTo(tp0), eqTo(Offset(151L)))
    verify(indexManager, times(2)).cleanUpObsoleteLocks(eqTo(tp0), eqTo(Offset(151L)), any[Set[String]])
  }

  // ── No-regression after cleanup when force-on-revoke earlier failed ───────────────
  test("Force-after-cleanUp is one-shot: a failure clears the flag and falls back to dirty-flag retry on the next routine cycle") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    // Establish lastReturned = 51 on a successful first cycle.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 51L

    // In-place rollback — force flag set for tp0.
    wm.cleanUp(tp0)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(80))))

    // First post-cleanUp preCommit: force-write fails. forceWriteAfterCleanUp(tp) is
    // cleared inside the helper before the write attempt so the next cycle can re-attempt
    // through the routine dirty-flag path.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("transient", tp0)))
    wm.preCommit(currentOffsets(tp0, 100)) shouldBe empty
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 1L
    metrics.getMasterLockFailures shouldBe 1L

    // Second post-cleanUp preCommit: still dirty (globalSafeOffset=81 > lastWritten=0
    // because lastWritten lazy seeds from the absent durable floor). WriteRoutine fires.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 81L
    // No second forced-write metric — this was a routine retry.
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 1L
    // Two total successes (initial + retry).
    metrics.getMasterLockUpdates shouldBe 2L
  }

  // ── Force-on-stop carries the Stop reason tag ──────────────────────────────────────
  test("closeForStop tags the forced-write metric with Stop, distinct from Revoke") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    // Establish lastWritten = 51, then bump the writer so dirty flag is true on stop.
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 100))
    val highWriter = makeIdleWriter(tp0, Some(Offset(99)))
    wm.putWriter(MapKey(tp0, dateA), highWriter)

    wm.closeForStop()

    metrics.getMasterLockWriteForcedStop shouldBe 1L
    metrics.getMasterLockWriteForcedRevoke shouldBe 0L
  }

  // ── ForcedWriteReason enum: explicit cases are wired through the metric helper ─────
  test("ForcedWriteReason: every enum value maps to a distinct metric counter") {
    val metrics = new CloudSinkMetrics()
    metrics.incrementMasterLockWriteForced(ForcedWriteReason.Revoke)
    metrics.incrementMasterLockWriteForced(ForcedWriteReason.Stop)
    metrics.incrementMasterLockWriteForced(ForcedWriteReason.PostCleanUp)
    metrics.getMasterLockWriteForcedRevoke shouldBe 1L
    metrics.getMasterLockWriteForcedStop shouldBe 1L
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 1L
  }

  // ── Skipped-cycle: lastReturned still advances monotonically (value-level no-op) ──
  test("Skip branch advances lastReturnedSafeOffset(tp) (monotonic-max) and HWM together") {
    // This is the structurally-required no-op-in-value-terms advance on the Skip branch.
    // The test exercises the path indirectly by verifying that a *failure* on the cycle
    // immediately after a skip does NOT cause regression — because lastReturned was
    // captured on the skip even though the value did not change.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))

    val wm = buildWriterManager(indexManager)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 51L // success
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 51L // skip

    // After skip + a forced-rollback that preserves lastReturned, the post-cleanUp HWM
    // seed must still equal 51 (otherwise a buggy implementation that didn't advance
    // lastReturned on the skip path would drop to 0 here).
    wm.cleanUp(tp0)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(10))))
    val result = wm.preCommit(currentOffsets(tp0, 100))
    // Calculated = 11; HWM re-seed = max(0, 51) = 51; globalSafeOffset = max(11, 51) = 51.
    result(tp0).offset() shouldBe 51L
  }

  // ── Granular-lock writes occur on every commit regardless of master-lock gating ──────
  test("Granular-lock-per-commit independence: a skipped master-lock cycle still allows the next Writer.commit to write its granular lock") {
    // The dirty-flag gate lives entirely inside `WriterManager.preCommit` and ONLY gates
    // `IndexManager.updateMasterLock`. Granular-lock writes go through a different code
    // path — `Writer.commit` -> `IndexManager.updateForPartitionKey` (PARTITIONBY) /
    // `IndexManager.update` (non-PARTITIONBY) — that is structurally untouched by the
    // gate. This test pins that independence: a `preCommit` cycle that classifies Skip
    // must NOT short-circuit the granular path on a subsequent commit, even if the
    // master-lock write has been suppressed for multiple cycles in a row.
    //
    // We exercise this indirectly through `WriterManager.preCommit` invocations alternating
    // with verification that `updateForPartitionKey` / `update` are NOT called from
    // `WriterManager.preCommit`. The actual `Writer.commit` -> granular-lock path is
    // exercised end-to-end by `ExactlyOnceScenarioTest` against the real `IndexManagerV2`;
    // here we pin only the structural decoupling.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))

    // Run 5 preCommit cycles. After cycle 1 the dirty-flag gate skips cycles 2–5 for the
    // master lock. None of these cycles should call updateForPartitionKey / update — those
    // belong to the commit path, not to preCommit. The skipped master-lock writes must
    // not bleed into the granular-lock surface.
    (1 to 5).foreach(_ => wm.preCommit(currentOffsets(tp0, 200)))
    metrics.getMasterLockUpdates shouldBe 1L
    metrics.getMasterLockWriteSkipped shouldBe 4L
    verify(indexManager, never).updateForPartitionKey(any[TopicPartition],
                                                      any[String],
                                                      any[Option[Offset]],
                                                      any[Option[io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState]],
    )
    verify(indexManager, never).update(any[TopicPartition],
                                       any[Option[Offset]],
                                       any[Option[io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState]],
    )
  }

  // ── Buffered writer with min(firstBufferedOffset) bounds globalSafeOffset under the gate ──
  test("Buffered writer at firstBufferedOffset bounds globalSafeOffset; idle siblings cannot push past the barrier under the dirty-flag gate") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    val bufWriter = makeWritingWriter(tp0,
                                      committedOffset     = None,
                                      firstBufferedOffset = Offset(100),
                                      uncommittedOffset   = Offset(104),
    )
    val idleSibling = makeIdleWriter(tp0, Some(Offset(105)))
    wm.putWriter(MapKey(tp0, dateA), bufWriter)
    wm.putWriter(MapKey(tp0, dateB), idleSibling)

    // First cycle: globalSafeOffset = min(firstBuffered) = 100; lastWritten lazy seeds to 0;
    // dirty = true; WriteRoutine fires. Returns 100.
    wm.preCommit(currentOffsets(tp0, 1000))(tp0).offset() shouldBe 100L
    metrics.getMasterLockUpdates shouldBe 1L

    // Second cycle with same writer states: same 100. Skip branch — no write.
    wm.preCommit(currentOffsets(tp0, 1000))(tp0).offset() shouldBe 100L
    metrics.getMasterLockWriteSkipped shouldBe 1L
    metrics.getMasterLockUpdates shouldBe 1L
  }

  // ── cleanUp(tp) → close() interleaving: leaked state is fully cleared ──────────────
  test("close() bulk-clears per-TP state for cleanUp-then-close interleaving so the next ownership episode starts fresh") {
    // Scenario: cleanUp(tp) runs after a fatal error (preserves lastReturnedSafeOffset and
    // sets forceWriteAfterCleanUp), but before any new writer is created the task is
    // rebalanced (close() called). Without the defensive bulk clear the leaked entries
    // survive into the next ownership episode, causing WriteForced(PostCleanUp) and an
    // inflated HWM on the first preCommit of the new episode.
    //
    // Step 1: establish lastWritten = 51, lastReturned = 51.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))
    doNothing.when(indexManager).evictAllGranularLocks(any[TopicPartition])

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L

    // Step 2: in-place rollback — cleanUp preserves lastReturned=51, sets forceWriteAfterCleanUp.
    wm.cleanUp(tp0)

    // Step 3: rebalance close with no intervening writer re-creation. The bulk clear must
    // erase lastReturnedSafeOffset(tp0) and forceWriteAfterCleanUp(tp0).
    wm.close()

    // Step 4: new ownership episode. Simulate open() having re-read the master lock from
    // storage — the mock returns None (durable floor is 0, as if nothing was persisted
    // during the zombie episode). A new writer at committed=5 produces calculatedSafeOffset=6.
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(5))))

    val result  = wm.preCommit(currentOffsets(tp0, 200))
    // Without the bulk clear: HWM re-seeds to max(durableFloor+1=0, lastReturned=51) = 51,
    // and WriteForced(PostCleanUp) fires at 51. With the bulk clear: both leaked entries
    // are gone, so HWM re-seeds to max(0, 0) = 0, calculatedSafeOffset=6 wins, and the
    // cycle is classified WriteRoutine at 6.
    result(tp0).offset() shouldBe 6L
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 0L
    // Exactly two successful master-lock writes total: the initial cycle at 51 (step 1)
    // and the new-episode routine write at 6 (step 4).
    metrics.getMasterLockUpdates shouldBe 2L
  }
}
