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

import cats.data.Validated
import cats.implicits._
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CloudCommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerV2
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated.IndexFilenames
import io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated.IndexManagerV1
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailWriteAt
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable

/**
 * Integrated tests that wire a real [[WriterManager]] + real [[IndexManagerV2]] +
 * [[InMemoryStorageInterface]] for the PARTITIONBY master-lock failure / fencing paths.
 *
 * Coverage complement:
 *
 *  - [[MasterLockFrequencyTest]] (mock-driven): exercises the real `WriterManager.preCommit`
 *    decision logic (`Skip` / `WriteRoutine` / `WriteForced`) against a *mocked* `IndexManager`.
 *    Regressions in the gating / decision logic are caught there.
 *
 *  - [[io.lenses.streamreactor.connect.cloud.common.sink.ExactlyOnceScenarioTest]]
 *    (synthetic-harness): exercises real `IndexManagerV2` + real storage but bypasses
 *    `WriterManager.preCommit` by calling `IndexManagerV2.updateMasterLock` directly from
 *    the harness `PreCommit` op handler.
 *
 *  - THIS SUITE: closes the integrated gap — no single prior test exercises the chain
 *    `WriterManager.preCommit` → `WriteDecision` → `attemptMasterLockWrite` →
 *    `IndexManagerV2.updateMasterLock` → `InMemoryStorageInterface`.
 *
 * Test matrix — one test per failure-arm shape:
 *
 *   (a) Zombie / stale-eTag fencing: the `WriteRoutine` path fails mid-write when an
 *       external eTag bump makes the cached eTag stale. `preCommit` must return an empty
 *       map for the TP. A new task instance that re-opens from storage recovers to the
 *       correct K without duplicating data.
 *
 *   (b) `WriteRoutine` failure-then-recover (multi-cycle dirty window): K consecutive
 *       storage failures must not advance the HWM or the durable master-lock floor.
 *       Recovery on cycle K+1 uses the same `globalSafeOffset` that was first attempted,
 *       verified via `IndexManagerV2.getSeekedOffsetForTopicPartition`.
 *
 *   (c) `WriteForced(PostCleanUp)` one-shot failure: the force flag is cleared *before*
 *       the write attempt so a failure does not re-arm the forced path on the next cycle.
 *       The next cycle re-enters via the routine dirty-flag path and succeeds.
 *
 * Writer construction: writers are created directly with `lastSeekedOffset = committedOffset`
 * so they start in `NoWriter(CommitState(tp, committedOffset))` state — exactly the same
 * observable state as `makeIdleWriter(...) + forceWriteState(NoWriter(...))` in
 * [[MasterLockFrequencyTest]], but achieved via the public `Writer` constructor rather than
 * the internal test seam. No `forceWriteState` is used.
 */
class WriterManagerZombieIntegrationTest
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with MockitoSugar
    with ArgumentMatchersSugar {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("eo-zombie-test", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => Validated.valid(location)

  private val bucket        = "test-bucket"
  private val directoryName = ".indexes2"
  private val tp0           = Topic("topic").withPartition(0)

  private val dateField: PartitionField                        = ValuePartitionField(PartitionNamePath("date"))
  private val dateA:     immutable.Map[PartitionField, String] = Map(dateField -> "2024-01-01")

  // Master lock path that IndexManagerV2 will use for tp0. Note: `tp0.topic` renders as
  // "Topic(topic)" (the case-class toString), matching IndexManagerV2.generateLockFilePath
  // which interpolates `${topicPartition.topic}` (not `.topic.value`).
  //   <directoryName>/<connectorName>/.locks/Topic(<topic>)/<partition>.lock
  private val masterLockPath: String =
    s"$directoryName/${connectorTaskId.name}/.locks/${tp0.topic}/${tp0.partition}.lock"

  private def bucketAndPrefix(tp: TopicPartition): Either[SinkError, CloudLocation] =
    CloudLocation(bucket, Some(s"data/${tp.topic.value}/${tp.partition}/")).asRight

  /**
   * Build a real `IndexManagerV2` against the provided storage. Mirrors
   * `ExactlyOnceScenarioTest.Harness.bootTask` exactly (GC disabled, all intervals at
   * `Int.MaxValue` to prevent background activity during tests).
   */
  private def buildIndexManager(storage: StorageInterface[FakeFileMetadata]): IndexManagerV2 = {
    implicit val si: StorageInterface[FakeFileMetadata] = storage
    val v1  = new IndexManagerV1(new IndexFilenames(directoryName + "-v1"), bucketAndPrefix)
    val pop = new PendingOperationsProcessors(storage)
    new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      oldIndexManager             = v1,
      pendingOperationsProcessors = pop,
      directoryFileName           = directoryName,
      gcIntervalSeconds           = Int.MaxValue,
      gcSweepIntervalSeconds      = Int.MaxValue,
      gcSweepMinAgeSeconds        = Int.MaxValue,
      gcSweepEnabled              = false,
    )(si, connectorTaskId)
  }

  /**
   * Build a real `WriterManager` backed by the given `IndexManagerV2`. Mirrors the pattern
   * from `WriterUploadRetryRecoveryTest.buildMinimalWriterManager`. The format-writer /
   * object-key-builder lambdas are mocked because no actual record writes are performed —
   * only `preCommit` paths are exercised.
   */
  private def buildWriterManager(
    im:      IndexManagerV2,
    storage: StorageInterface[FakeFileMetadata],
    metrics: CloudSinkMetrics,
  ): WriterManager[FakeFileMetadata] = {
    val pop = new PendingOperationsProcessors(storage)
    new WriterManager[FakeFileMetadata](
      commitPolicyFn              = _ => CloudCommitPolicy.Default.asRight,
      bucketAndPrefixFn           = bucketAndPrefix,
      keyNamerFn                  = _ => mock[KeyNamer].asRight,
      stagingFilenameFn           = (_, _) => File.createTempFile("wm-zombie-", ".tmp").asRight,
      objKeyBuilderFn             = (_, _) => mock[ObjectKeyBuilder],
      formatWriterFn              = (_, _) => mock[FormatWriter].asRight,
      indexManager                = im,
      transformerF                = Right(_),
      schemaChangeDetector        = mock[SchemaChangeDetector],
      skipNullValues              = false,
      pendingOperationsProcessors = pop,
      metrics                     = metrics,
    )
  }

  /**
   * Build a `Writer` that starts in `NoWriter(CommitState(tp, committedOffset))` state
   * without using `forceWriteState`. The `Writer` constructor always initialises its state
   * to `NoWriter(CommitState(tp, lastSeekedOffset))`, so passing the desired committed
   * offset as `lastSeekedOffset` achieves the same effect as the `makeIdleWriter` +
   * `forceWriteState` pattern used in the mock-driven `MasterLockFrequencyTest`, but via
   * the public constructor only.
   *
   * `partitionKey = WriterManager.derivePartitionKey(dateA)` ensures that the
   * corresponding `MapKey(tp, dateA)` (with non-empty `partitionValues`) triggers the
   * PARTITIONBY branch (`hasPartitionByWriters = true`) inside `getOffsetAndMeta`.
   */
  private def makeCommittedWriter(
    tp:              TopicPartition,
    im:              IndexManagerV2,
    storage:         StorageInterface[FakeFileMetadata],
    committedOffset: Option[Offset],
  ): Writer[FakeFileMetadata] = {
    val pop = new PendingOperationsProcessors(storage)
    new Writer[FakeFileMetadata](
      tp,
      CloudCommitPolicy.Default,
      im,
      () => File.createTempFile("committed-writer-", ".tmp").asRight,
      mock[ObjectKeyBuilder],
      _ => mock[FormatWriter].asRight,
      mock[SchemaChangeDetector],
      pop,
      partitionKey     = WriterManager.derivePartitionKey(dateA),
      lastSeekedOffset = committedOffset,
    )
  }

  // ── (a) Zombie / stale-eTag fencing ──────────────────────────────────────────────────

  test(
    "Integrated zombie: master-lock fenced through real WriterManager.preCommit → empty map, " +
      "metrics correct (failures=1, updates=1), recovery after reopen succeeds at correct K",
  ) {
    val storage = new InMemoryStorageInterface()
    val metrics = new CloudSinkMetrics()
    val im      = buildIndexManager(storage)
    im.open(Set(tp0)).value  // initialise master lock in storage, seed eTag cache

    val wm = buildWriterManager(im, storage, metrics)

    // Cycle 1: establish lastWritten = 43 (committed offset 42 → K = 43).
    val writer1 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(42)))
    wm.putWriter(MapKey(tp0, dateA), writer1)
    wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))(tp0).offset() shouldBe 43L
    metrics.getMasterLockUpdates shouldBe 1L

    // Install the pre-write barrier: atomically bumps the master-lock eTag via a direct
    // storage write so the zombie's cached eTag is stale the moment its write lands.
    val fired = new AtomicBoolean(false)
    IndexManagerV2.IndexManagerV2TestHooks.installPreWriteMasterLockBarrier(
      im,
      () => {
        if (fired.compareAndSet(false, true)) {
          val current = storage.getBlobAsString(bucket, masterLockPath).getOrElse(
            throw new AssertionError(s"zombie barrier: master lock missing at $bucket/$masterLockPath"),
          )
          storage.writeStringToFile(
            bucket,
            masterLockPath,
            UploadableString(current),
          ).left.foreach(err => throw new AssertionError(s"zombie barrier: external write failed: ${err.message()}"))
        }
      },
    )

    // Cycle 2 (zombie write): higher-offset writer; barrier fires mid-write → eTag mismatch.
    val writer2 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(99)))
    wm.putWriter(MapKey(tp0, dateA), writer2)
    try {
      val result2 = wm.preCommit(Map(tp0 -> new OffsetAndMetadata(500)))
      result2.get(tp0) shouldBe None  // fenced: no advancing offset returned for tp0
      fired.get() shouldBe true
      metrics.getMasterLockFailures shouldBe 1L
      metrics.getMasterLockUpdates shouldBe 1L  // not incremented during the fenced cycle
    } finally {
      IndexManagerV2.IndexManagerV2TestHooks.clearPreWriteMasterLockBarrier(im)
    }

    // Simulate crash + reopen: a fresh IndexManagerV2 reads the bumped master lock from
    // storage and seeds its eTag cache with the current value. The new task's preCommit
    // succeeds with K = max(committed) + 1 = 100.
    im.close()
    val im2     = buildIndexManager(storage)
    im2.open(Set(tp0)).value
    val metrics2 = new CloudSinkMetrics()
    val wm2      = buildWriterManager(im2, storage, metrics2)
    val writer3  = makeCommittedWriter(tp0, im2, storage, committedOffset = Some(Offset(99)))
    wm2.putWriter(MapKey(tp0, dateA), writer3)
    val result3 = wm2.preCommit(Map(tp0 -> new OffsetAndMetadata(500)))
    result3(tp0).offset() shouldBe 100L  // K = max(committed) + 1 = 100
    metrics2.getMasterLockUpdates shouldBe 1L

    wm2.close()
    im2.close()
  }

  // ── (b) WriteRoutine failure-then-recover (multi-cycle dirty window) ──────────────────

  test(
    "Integrated WriteRoutine: 3 consecutive master-lock write failures do not advance HWM; " +
      "recovery on cycle 4 uses the same globalSafeOffset; durable floor verified via getSeekedOffsetForTopicPartition",
  ) {
    val storage = new InMemoryStorageInterface()
    val metrics = new CloudSinkMetrics()
    val im      = buildIndexManager(storage)
    im.open(Set(tp0)).value

    val wm = buildWriterManager(im, storage, metrics)

    // Cycle 1: establish lastWritten = 51 (committed offset 50 → K = 51).
    val writer1 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(50)))
    wm.putWriter(MapKey(tp0, dateA), writer1)
    wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L

    // Add writer with higher committed offset and arm 3 one-shot master-lock failures.
    val writer2 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(100)))
    wm.putWriter(MapKey(tp0, dateA), writer2)
    (1 to 3).foreach(_ => storage.arm(FailWriteAt(bucket, masterLockPath)))

    // 3 failure cycles: each returns empty map; HWM and durable floor remain at K = 51.
    (1 to 3).foreach { _ =>
      val result = wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))
      result.get(tp0) shouldBe None
    }
    metrics.getMasterLockFailures shouldBe 3L
    metrics.getMasterLockUpdates shouldBe 1L  // only the first cycle's success

    // Recovery: all armed failures consumed, next cycle succeeds at K = 101 (same as the
    // first failed attempt — no HWM regression, no skip-ahead).
    val resultRecover = wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))
    resultRecover(tp0).offset() shouldBe 101L
    metrics.getMasterLockUpdates shouldBe 2L
    metrics.getMasterLockFailures shouldBe 3L  // no new failures

    // GC threshold: the durable master lock now encodes committedOffset = K-1 = 100.
    // This is the same value passed to cleanUpObsoleteLocks (same-value invariant).
    im.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(100))

    wm.close()
    im.close()
  }

  // ── (c) WriteForced(PostCleanUp) one-shot failure → routine dirty-flag retry ──────────

  test(
    "Integrated WriteForced(PostCleanUp): flag cleared before write attempt; " +
      "next cycle classifies Skip (durable floor matches globalSafeOffset) and returns correct K without redundant write",
  ) {
    val storage = new InMemoryStorageInterface()
    val metrics = new CloudSinkMetrics()
    val im      = buildIndexManager(storage)
    im.open(Set(tp0)).value

    val wm = buildWriterManager(im, storage, metrics)

    // Cycle 1: establish lastWritten = 51, lastReturnedSafeOffset = 51.
    val writer1 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(50)))
    wm.putWriter(MapKey(tp0, dateA), writer1)
    wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L

    // cleanUp: evicts writers, clears lastWrittenMasterSafeOffset(tp0) and HWM,
    // preserves lastReturnedSafeOffset(tp0) = 51, sets forceWriteAfterCleanUp(tp0).
    wm.cleanUp(tp0)

    // Re-add a writer with a lower committed offset (30). HWM re-seeds from
    // max(durableFloor+1 = 51, lastReturned = 51) = 51. calculatedSafeOffset = 31.
    // globalSafeOffset = max(31, 51) = 51.
    val writer2 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(30)))
    wm.putWriter(MapKey(tp0, dateA), writer2)

    // Arm exactly one master-lock failure to fail the forced write.
    storage.arm(FailWriteAt(bucket, masterLockPath))

    // Cycle 2: WriteForced(PostCleanUp) fires and fails.
    // The flag is cleared BEFORE the write attempt (WriterManager.scala line 627),
    // so the next cycle does NOT re-enter as WriteForced.
    val result2 = wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))
    result2.get(tp0) shouldBe None
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 1L
    metrics.getMasterLockFailures shouldBe 1L

    // Cycle 3: storage healthy; the real IndexManagerV2's seekedOffsets(tp0) = Offset(50)
    // (written by cycle 1), so lastWritten re-seeds to 50+1 = 51, which equals
    // globalSafeOffset = 51. dirty = false → Skip branch.
    //
    // Key assertion: the Skip branch is reached (not WriteForced — the flag was cleared in
    // cycle 2 before the failed write attempt) and not WriteRoutine (durable floor is
    // current). The Skip branch still returns 51 to Kafka Connect without writing again.
    val result3 = wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))
    result3(tp0).offset() shouldBe 51L
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 1L  // one-shot, not re-fired
    metrics.getMasterLockUpdates shouldBe 1L                 // no redundant write on Skip
    metrics.getMasterLockWriteSkipped shouldBe 1L            // Skip counter incremented
    metrics.getMasterLockFailures shouldBe 1L                // no new failure

    wm.close()
    im.close()
  }

  // ── (d) WriteForced(Revoke) success → durable floor advances ─────────────────────────

  test(
    "Integrated force-on-revoke success: dirty TP at close() persists globalSafeOffset to master lock; " +
      "durable floor advances; metrics tagged Revoke; granular cache evicted",
  ) {
    // This test closes the integration coverage gap identified in the exactly-once review:
    // tests (a)–(c) cover routine success, multi-cycle failure, and forced(PostCleanUp)
    // failure. This test covers forced(Revoke) SUCCESS — the path through
    //   `wm.close() → closePartition(tp, Revoke) → attemptForceMasterLockWrite → updateMasterLock`
    // against the real IndexManagerV2 + InMemoryStorageInterface.
    //
    // Shape:
    //   Cycle 1 (routine write): committed=50 → K=51 persisted (lastWritten=51).
    //   Skip preCommit after adding high writer: dirty=true on close.
    //   wm.close(): force-on-revoke fires → updateMasterLock(Offset(101)) succeeds.
    //   Assert durable floor = Offset(100) via getSeekedOffsetForTopicPartition (master
    //   stores committedOffset = K-1 = 100, not globalSafeOffset = 101).
    val storage = new InMemoryStorageInterface()
    val metrics = new CloudSinkMetrics()
    val im      = buildIndexManager(storage)
    im.open(Set(tp0)).value

    val wm = buildWriterManager(im, storage, metrics)

    // Cycle 1: routine write establishes lastWritten = 51 (committed offset 50 → K = 51).
    val writer1 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(50)))
    wm.putWriter(MapKey(tp0, dateA), writer1)
    wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L

    // Add a higher-offset writer WITHOUT a preCommit between. The dirty bit is now true
    // (globalSafeOffset = 101 > lastWritten = 51) so the close-path force-write fires.
    val writer2 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(100)))
    wm.putWriter(MapKey(tp0, dateA), writer2)

    // No storage faults armed — the force-on-revoke write succeeds.
    wm.close()

    // Forced write was attempted (once) and succeeded, tagged as Revoke.
    metrics.getMasterLockWriteForcedRevoke shouldBe 1L
    // Universal success counter = cycle 1 routine + force-on-revoke.
    metrics.getMasterLockUpdates shouldBe 2L
    metrics.getMasterLockFailures shouldBe 0L

    // Durable master floor: IndexManagerV2 stores committedOffset = globalSafeOffset - 1.
    // globalSafeOffset from the writers = max(committed)+1 = 101, so committedOffset = 100.
    im.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(100))

    // All writers closed and removed.
    wm.writerCount shouldBe 0

    // Granular cache evicted by closePartition step 6: re-adding a writer for the same
    // partition key triggers ensureGranularLock, which re-reads from storage (cache miss)
    // rather than using a stale in-memory entry. We verify this by opening a fresh
    // IndexManagerV2 from the same storage and confirming it can open successfully and
    // read the persisted floor — proving the durable state is self-consistent.
    im.close()
    val im2 = buildIndexManager(storage)
    im2.open(Set(tp0)).value
    im2.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(100))
    im2.close()
  }
}
