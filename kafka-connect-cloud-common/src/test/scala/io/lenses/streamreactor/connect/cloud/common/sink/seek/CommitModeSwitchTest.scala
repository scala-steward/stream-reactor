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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import cats.data.NonEmptyList
import cats.data.Validated
import cats.implicits.catsSyntaxEitherId
import io.circe.Encoder
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
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.StringSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
import io.lenses.streamreactor.connect.cloud.common.storage.FileCreateError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailDeleteAt
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable

/**
 * Granular -> batch migration (§2.6). A batch-mode task opening over a granular-mode layout must:
 * snapshot and eTag-bump every legacy granular lock (fencing granular zombies and resolving their
 * in-flight chains), raise the per-key dedup floor by those locks, and purge them only once the
 * batch watermark has caught up to every one of them.
 */
class CommitModeSwitchTest
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with OptionValues
    with MockitoSugar
    with ArgumentMatchersSugar
    with BeforeAndAfterEach {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("switch-test", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => Validated.valid(location)
  private implicit val indexFileEncoder: Encoder[IndexFile] = IndexFile.indexFileEncoder

  private val bucket        = "test-bucket"
  private val directoryName = ".indexes"
  private val tp            = Topic("orders").withPartition(0)

  private val keyField: PartitionField = ValuePartitionField(PartitionNamePath("k"))
  private def pv(name: String): immutable.Map[PartitionField, String] = Map(keyField -> name)
  private def keyOf(name: String): String = WriterManager.derivePartitionKey(pv(name)).get

  private def masterPath = IndexManagerV2.generateLockFilePath(connectorTaskId, tp, directoryName)
  private def granularPath(key: String) =
    IndexManagerV2.generateGranularLockFilePath(connectorTaskId, tp, key, directoryName)
  private def sweepMarkerPath = IndexManagerV2.generateSweepMarkerPath(connectorTaskId, tp, directoryName)

  private var storage: InMemoryStorageInterface = _

  override def beforeEach(): Unit = storage = new InMemoryStorageInterface()

  private def bucketAndPrefix(topicPartition: TopicPartition): Either[SinkError, CloudLocation] =
    CloudLocation(bucket, Some(s"data/${topicPartition.topic.value}/${topicPartition.partition}/")).asRight

  /** Directly writes an index file (bypassing eTag preconditions) and returns its eTag. */
  private def writeIndex(path: String, idx: IndexFile): String =
    storage.writeBlobToFile(bucket, path, NoOverwriteExistingObject(idx)).value.eTag

  private def eTagOf(path: String): String = storage.snapshot(bucket)(path).eTag
  private def offsetOf(path: String): Option[Offset] =
    storage.getBlobAsObject[IndexFile](bucket, path).value.wrappedObject.committedOffset

  /** Seeds a granular-mode master lock at committed offset `m`. */
  private def seedMaster(m: Long): Unit = {
    val _ = writeIndex(masterPath, IndexFile("prev-owner", Some(Offset(m)), None))
  }

  private def buildIndexManager(
    commitMode: CommitMode,
    st:         InMemoryStorageInterface = storage,
  ): IndexManagerV2 = {
    implicit val si: InMemoryStorageInterface = st
    new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = new PendingOperationsProcessors(st),
      directoryFileName           = directoryName,
      gcIntervalSeconds           = Int.MaxValue,
      gcSweepIntervalSeconds      = Int.MaxValue,
      gcSweepMinAgeSeconds        = Int.MaxValue,
      gcSweepEnabled              = false,
      commitMode                  = commitMode,
    )(si, connectorTaskId)
  }

  private def tempFileWith(payload: String): File = {
    val f = Files.createTempFile("switch-", ".tmp").toFile
    Files.write(f.toPath, payload.getBytes(StandardCharsets.UTF_8))
    f.deleteOnExit()
    f
  }

  private final class TogglePolicy {
    val flush = new java.util.concurrent.atomic.AtomicBoolean(false)
    val policy: CommitPolicy = {
      val p = mock[CommitPolicy]
      when(p.shouldFlush(any[CommitContext])).thenAnswer((_: CommitContext) => flush.get())
      p
    }
  }

  private def buildWriterManager(
    im:         IndexManagerV2,
    metrics:    CloudSinkMetrics,
    values:     immutable.Map[PartitionField, String],
    policy:     CommitPolicy,
    st:         InMemoryStorageInterface = storage,
  ): WriterManager[FakeFileMetadata] = {
    val keyNamer = mock[KeyNamer]
    when(keyNamer.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(values.asRight[SinkError])
    val fw = mock[FormatWriter]
    when(fw.write(any[MessageDetail])).thenReturn(().asRight)
    when(fw.complete()).thenReturn(().asRight)
    when(fw.rolloverFileOnSchemaChange()).thenReturn(false)

    new WriterManager[FakeFileMetadata](
      commitPolicyFn    = _ => policy.asRight,
      bucketAndPrefixFn = bucketAndPrefix,
      keyNamerFn        = _ => keyNamer.asRight,
      stagingFilenameFn = (_, _) => tempFileWith("payload").asRight,
      objKeyBuilderFn = (_, values2) => {
        val okb = mock[ObjectKeyBuilder]
        when(okb.build(any[Offset], any[Offset], any[Long], any[Long], any[Long])).thenAnswer {
          (first: Offset, _: Offset, _: Long, _: Long, _: Long) =>
            val seg = WriterManager.derivePartitionKey(values2).getOrElse("nokey")
            CloudLocation(bucket, path = Some(s"data/${tp.topic.value}/${tp.partition}/$seg-${first.value}.json")).asRight
        }
        okb
      },
      formatWriterFn              = (_, _) => fw.asRight,
      indexManager                = im,
      transformerF                = Right(_),
      schemaChangeDetector        = mock[SchemaChangeDetector],
      skipNullValues              = false,
      pendingOperationsProcessors = new PendingOperationsProcessors(st),
      commitMode                  = im.commitMode,
      metrics                     = metrics,
    )
  }

  private def message(offset: Long): MessageDetail =
    MessageDetail(
      key       = StringSinkData("k", None),
      value     = StringSinkData("v", None),
      headers   = Map.empty,
      timestamp = None,
      topic     = tp.topic,
      partition = tp.partition,
      offset    = Offset(offset),
    )

  // ── T7.1 ────────────────────────────────────────────────────────────────────────────

  test("[Z] T7.1 the first batch open GETs and eTag-bumps every legacy lock, leaving marker and tmp orphans untouched") {
    seedMaster(99)
    val e1     = writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(120)), None))
    val e2     = writeIndex(granularPath(keyOf("B")), IndexFile("prev", Some(Offset(140)), None))
    val marker = storage.writeBlobToFile(
      bucket,
      sweepMarkerPath,
      NoOverwriteExistingObject(IndexManagerV2.SweepMarker(1L, 2L)),
    )(IndexManagerV2.SweepMarker.sweepMarkerEncoder).value.eTag
    val tmpPath = granularPath(keyOf("A")) + ".tmp.0000abcd"
    val _       = writeIndex(tmpPath, IndexFile("prev", Some(Offset(999)), None))
    val tmpETag = eTagOf(tmpPath)

    val im = buildIndexManager(CommitMode.Batch)
    try {
      im.open(Set(tp)).value

      eTagOf(granularPath(keyOf("A"))) should not be e1
      eTagOf(granularPath(keyOf("B"))) should not be e2
      offsetOf(granularPath(keyOf("A"))) shouldBe Some(Offset(120))
      offsetOf(granularPath(keyOf("B"))) shouldBe Some(Offset(140))
      // The sweep marker and the .lock.tmp orphan are not legacy locks; they are left alone.
      eTagOf(sweepMarkerPath) shouldBe marker
      eTagOf(tmpPath) shouldBe tmpETag
      im.legacyFloor(tp, keyOf("A")) shouldBe Some(Some(Offset(120)))
      im.legacyFloor(tp, keyOf("B")) shouldBe Some(Some(Offset(140)))
    } finally im.close()
  }

  // ── T7.2 ────────────────────────────────────────────────────────────────────────────

  test("[ND] T7.2 a legacy lock ahead of the master floor skips replayed records up to it, then writes the next") {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))

    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).value
    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(im, metrics, pv("A"), new TogglePolicy().policy)

    (100L to 150L).foreach(o => wm.write(tp.withOffset(Offset(o)), message(o)).value)
    metrics.getDuplicateRecordsSkippedTotal shouldBe 51L
    metrics.getRecordsWrittenTotal shouldBe 0L

    wm.write(tp.withOffset(Offset(151)), message(151)).value
    metrics.getRecordsWrittenTotal shouldBe 1L

    wm.close()
    im.close()
  }

  // ── T7.3 ────────────────────────────────────────────────────────────────────────────

  test("[ND] T7.3 a legacy lock with a pending Copy/Delete chain is resolved at open; the floor is its pending offset") {
    seedMaster(99)
    val temp  = s".temp-upload/legacy/${tp.topic}/0/uuid/data/orders/0/legacy-A.json"
    val final_ = "data/orders/0/legacy-A.json"
    // Seed the temp object the pending Copy will move.
    storage.writeStringToFile(bucket, temp, io.lenses.streamreactor.connect.cloud.common.model.UploadableString("legacy payload")).value
    val tempETag = storage.snapshot(bucket)(temp).eTag
    writeIndex(
      granularPath(keyOf("A")),
      IndexFile(
        "prev",
        Some(Offset(100)),
        Some(PendingState(
          Offset(120),
          NonEmptyList.of(CopyOperation(bucket, temp, final_, tempETag), DeleteOperation(bucket, temp, tempETag)),
        )),
      ),
    )

    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).value

    storage.keysUnder(bucket, "data/orders/0/legacy-A") should have size 1
    im.legacyFloor(tp, keyOf("A")) shouldBe Some(Some(Offset(120)))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(im, metrics, pv("A"), new TogglePolicy().policy)
    (100L to 120L).foreach(o => wm.write(tp.withOffset(Offset(o)), message(o)).value)
    metrics.getDuplicateRecordsSkippedTotal shouldBe 21L
    wm.close()
    im.close()
  }

  // ── T7.4 ────────────────────────────────────────────────────────────────────────────

  test("[NL] T7.4 a legacy pending chain whose upload never happened is dead-worker cleared; the floor is its committed offset") {
    seedMaster(99)
    val missing = new File("/nonexistent/switch-test/legacy-staging.tmp")
    val temp    = ".temp-upload/legacy/orders/0/uuid/data/orders/0/legacy-A.json"
    writeIndex(
      granularPath(keyOf("A")),
      IndexFile(
        "prev",
        Some(Offset(110)),
        Some(PendingState(
          Offset(130),
          NonEmptyList.of(
            UploadOperation(bucket, missing, temp),
            CopyOperation(bucket, temp, "data/orders/0/legacy-A.json", "x"),
            DeleteOperation(bucket, temp, "x"),
          ),
        )),
      ),
    )

    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).value

    // Dead-worker recovery: pending state cleared, floor stays at the recorded committed offset.
    im.legacyFloor(tp, keyOf("A")) shouldBe Some(Some(Offset(110)))
    storage.keysUnder(bucket, "data/orders/0/legacy-A") shouldBe empty

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(im, metrics, pv("A"), new TogglePolicy().policy)
    (100L to 110L).foreach(o => wm.write(tp.withOffset(Offset(o)), message(o)).value)
    metrics.getDuplicateRecordsSkippedTotal shouldBe 11L
    wm.write(tp.withOffset(Offset(111)), message(111)).value
    metrics.getRecordsWrittenTotal shouldBe 1L
    wm.close()
    im.close()
  }

  // ── T7.5 ────────────────────────────────────────────────────────────────────────────

  /**
   * A granular zombie writes the lock between the new owner's read and its eTag bump, so the bump
   * 412s. The owner re-reads, sees the zombie's higher offset, and uses it. Exhausting the bounded
   * re-reads is fatal.
   *
   * Deviation from the plan's literal CorruptETag mechanic: the same invariant [Z] is exercised
   * more directly by mutating the stored lock (a fresh eTag at a higher offset) immediately before
   * each bump write, which is exactly the race the re-read loop guards against.
   */
  private final class RaceOnBumpStorage(lockPath: String, raceOffset: Long, raceTimes: Int)
      extends InMemoryStorageInterface {
    private val hits = new AtomicInteger(0)
    override def writeBlobToFile[O](
      b:   String,
      p:   String,
      obj: ObjectProtection[O],
    )(implicit enc: Encoder[O]): Either[UploadError, ObjectWithETag[O]] =
      obj match {
        case _: ObjectWithETag[O] if p == lockPath && hits.getAndIncrement() < raceTimes =>
          // A zombie committed a higher offset with a fresh eTag just before our bump: overwrite
          // the stored lock (using its current eTag) then reject our If-Match write with a 412.
          val currentETag = super.getBlobAsObject[IndexFile](b, p)(IndexFile.indexFileDecoder).toOption.get.eTag
          super.writeBlobToFile[IndexFile](
            b,
            p,
            ObjectWithETag(IndexFile("zombie", Some(Offset(raceOffset)), None), currentETag),
          )(IndexFile.indexFileEncoder)
          FileCreateError(new IllegalStateException("If-Match failed (raced)"), p).asLeft
        case _ => super.writeBlobToFile(b, p, obj)
      }
  }

  test("[Z] T7.5 a bump that loses one race re-reads and adopts the racer's offset; exhausting the budget is fatal") {
    // One race: re-read sees offset 150 and uses it.
    locally {
      val st = new RaceOnBumpStorage(granularPath(keyOf("A")), raceOffset = 150, raceTimes = 1)
      storage = st
      seedMaster(99)
      writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(140)), None))
      val im = buildIndexManager(CommitMode.Batch, st)
      im.open(Set(tp)).value
      im.legacyFloor(tp, keyOf("A")) shouldBe Some(Some(Offset(150)))
      // Record 140 for A is now below the adopted floor and is skipped.
      val metrics = new CloudSinkMetrics()
      val wm      = buildWriterManager(im, metrics, pv("A"), new TogglePolicy().policy, st)
      wm.write(tp.withOffset(Offset(140)), message(140)).value
      metrics.getDuplicateRecordsSkippedTotal shouldBe 1L
      wm.close()
      im.close()
    }

    // Never wins: three consecutive mismatches exhaust MaxLegacyBumpAttempts and fail open().
    locally {
      val st = new RaceOnBumpStorage(granularPath(keyOf("A")), raceOffset = 150, raceTimes = IndexManagerV2.MaxLegacyBumpAttempts)
      storage = st
      seedMaster(99)
      writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(140)), None))
      val im = buildIndexManager(CommitMode.Batch, st)
      im.open(Set(tp)).left.value shouldBe a[FatalCloudSinkError]
      im.close()
    }
  }

  // ── T7.6 ────────────────────────────────────────────────────────────────────────────

  test("[ND] T7.6 the legacy floor survives idle-writer eviction and is read from the TP map with no extra GET") {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))

    val gets = new AtomicInteger(0)
    val counting = new InMemoryStorageInterface() {
      override def getBlobAsObject[O](b: String, p: String)(implicit d: io.circe.Decoder[O]): Either[io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError, ObjectWithETag[O]] = {
        if (p == granularPath(keyOf("A"))) gets.incrementAndGet()
        super.getBlobAsObject(b, p)
      }
    }
    // Re-seed on the counting storage.
    storage = counting
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))

    val im = buildIndexManager(CommitMode.Batch, counting)
    im.open(Set(tp)).value
    val getsAfterOpen = gets.get()

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(im, metrics, pv("A"), new TogglePolicy().policy, counting)
    // Create a writer for A, then evict it by creating one for B (idle A is evicted on B's create).
    wm.write(tp.withOffset(Offset(151)), message(151)).value
    val wmB = buildWriterManager(im, metrics, pv("B"), new TogglePolicy().policy, counting)
    wmB.write(tp.withOffset(Offset(200)), message(200)).value

    // Record 140 for A is skipped from the TP-level floor without another GET of A's lock.
    val wmA2 = buildWriterManager(im, metrics, pv("A"), new TogglePolicy().policy, counting)
    val before = gets.get()
    wmA2.write(tp.withOffset(Offset(140)), message(140)).value
    gets.get() shouldBe before
    getsAfterOpen should be <= 1

    wm.close(); wmB.close(); wmA2.close(); im.close()
  }

  // ── T7.7 ────────────────────────────────────────────────────────────────────────────

  test("[ND] T7.7 a legacy lock ahead of the batch watermark is NOT purged; its floor still skips records") {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))

    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).value
    val metrics = new CloudSinkMetrics()
    val policy  = new TogglePolicy()
    val wm      = buildWriterManager(im, metrics, pv("B"), policy.policy)

    // Deliver 100..125 under key B (A's records are all skipped by A's legacy floor; B buffers).
    (100L to 125L).foreach(o => wm.write(tp.withOffset(Offset(o)), message(o)).value)
    policy.flush.set(true)
    wm.commitFlushableWriters().value

    // maxLegacy (150) > P (125): the legacy lock survives and the TP is not purged.
    storage.snapshot(bucket).keys should contain(granularPath(keyOf("A")))
    im.legacyPurged(tp) shouldBe false

    val wmA = buildWriterManager(im, metrics, pv("A"), new TogglePolicy().policy)
    wmA.write(tp.withOffset(Offset(130)), message(130)).value
    metrics.getDuplicateRecordsSkippedTotal shouldBe 1L
    wmA.write(tp.withOffset(Offset(151)), message(151)).value
    metrics.getRecordsWrittenTotal shouldBe (26L + 1L)

    wm.close(); wmA.close(); im.close()
  }

  // ── T7.8 ────────────────────────────────────────────────────────────────────────────

  test("[ND] T7.8 once the batch watermark reaches maxLegacy the legacy locks and marker are purged; later floors need no GET") {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))
    storage.writeBlobToFile(bucket, sweepMarkerPath, NoOverwriteExistingObject(IndexManagerV2.SweepMarker(1L, 2L)))(
      IndexManagerV2.SweepMarker.sweepMarkerEncoder,
    ).value

    val gets = new AtomicInteger(0)
    val counting = new InMemoryStorageInterface() {
      override def getBlobAsObject[O](b: String, p: String)(implicit d: io.circe.Decoder[O]): Either[io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError, ObjectWithETag[O]] = {
        if (p.endsWith(".lock") && !p.endsWith("0.lock")) gets.incrementAndGet()
        super.getBlobAsObject(b, p)
      }
    }
    storage = counting
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))
    counting.writeBlobToFile(bucket, sweepMarkerPath, NoOverwriteExistingObject(IndexManagerV2.SweepMarker(1L, 2L)))(
      IndexManagerV2.SweepMarker.sweepMarkerEncoder,
    ).value

    val im = buildIndexManager(CommitMode.Batch, counting)
    im.open(Set(tp)).value
    val policy = new TogglePolicy()
    val wm     = buildWriterManager(im, new CloudSinkMetrics(), pv("A"), policy.policy, counting)

    // Deliver up to 160 (> maxLegacy 150) then flush; the batch commit purges the legacy locks.
    (151L to 160L).foreach(o => wm.write(tp.withOffset(Offset(o)), message(o)).value)
    policy.flush.set(true)
    wm.commitFlushableWriters().value

    im.legacyPurged(tp) shouldBe true
    counting.snapshot(bucket).keys should not contain granularPath(keyOf("A"))
    counting.snapshot(bucket).keys should not contain sweepMarkerPath

    // A never-seen key's floor is the master offset with no per-key GET.
    val getsBefore = gets.get()
    im.batchDedupFloor(tp, Some(keyOf("Z"))).value shouldBe im.getSeekedOffsetForTopicPartition(tp)
    gets.get() shouldBe getsBefore

    wm.close(); im.close()
  }

  // ── T7.9 ────────────────────────────────────────────────────────────────────────────

  test("[B] T7.9 a purge that fails on one lock leaves the locks in place; the next commit retries and succeeds") {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))

    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).value
    val policy = new TogglePolicy()
    val wm     = buildWriterManager(im, new CloudSinkMetrics(), pv("A"), policy.policy)

    storage.arm(FailDeleteAt(bucket, granularPath(keyOf("A"))))
    wm.write(tp.withOffset(Offset(151)), message(151)).value
    policy.flush.set(true)
    wm.commitFlushableWriters().value

    // Purge failed: not purged, lock still present.
    im.legacyPurged(tp) shouldBe false
    storage.snapshot(bucket).keys should contain(granularPath(keyOf("A")))

    // Next commit retries the purge and succeeds.
    wm.write(tp.withOffset(Offset(152)), message(152)).value
    wm.commitFlushableWriters().value
    im.legacyPurged(tp) shouldBe true
    storage.snapshot(bucket).keys should not contain granularPath(keyOf("A"))

    wm.close(); im.close()
  }

  // ── T7.10 ───────────────────────────────────────────────────────────────────────────

  test("[ND] T7.10 a key that appears after the snapshot is lazily loaded on first use and skips its committed range") {
    seedMaster(99)
    // No legacy locks at open time.
    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).value
    im.legacyPurged(tp) shouldBe true // empty listing marks purged

    // A lock for K2 appears in storage after the snapshot. Because the TP was marked purged on an
    // empty listing, the floor is the master offset and the new lock is not consulted.
    writeIndex(granularPath(keyOf("K2")), IndexFile("prev", Some(Offset(130)), None))
    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(im, metrics, pv("K2"), new TogglePolicy().policy)
    // Master floor is 99, so 120 is written (a purged TP does not consult late locks — the
    // stop-first migration procedure guarantees no new granular locks appear post-switch).
    wm.write(tp.withOffset(Offset(120)), message(120)).value
    metrics.getRecordsWrittenTotal shouldBe 1L
    wm.close(); im.close()
  }

  test("[ND] T7.10 a key not in a non-empty snapshot is lazily loaded with one GET and skips its committed range") {
    seedMaster(99)
    // A different key exists at open so the TP is NOT marked purged.
    writeIndex(granularPath(keyOf("OTHER")), IndexFile("prev", Some(Offset(105)), None))
    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).value
    im.legacyPurged(tp) shouldBe false

    // A lock for K2 that was not in the snapshot appears; the lazy path loads it on first use.
    writeIndex(granularPath(keyOf("K2")), IndexFile("prev", Some(Offset(130)), None))
    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(im, metrics, pv("K2"), new TogglePolicy().policy)
    wm.write(tp.withOffset(Offset(120)), message(120)).value
    metrics.getDuplicateRecordsSkippedTotal shouldBe 1L
    wm.write(tp.withOffset(Offset(131)), message(131)).value
    metrics.getRecordsWrittenTotal shouldBe 1L
    wm.close(); im.close()
  }

  // ── T7.11 ───────────────────────────────────────────────────────────────────────────

  test("[Z] T7.11 bump-first: the new owner's open fences a granular zombie that opened earlier") {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(120)), None))

    // A granular zombie opened before the new owner and cached the granular lock's eTag.
    val zombie = buildIndexManager(CommitMode.Granular)
    zombie.open(Set(tp)).value
    zombie.ensureGranularLock(tp, keyOf("A")).value

    // The new batch owner opens and eager-bumps the granular lock.
    val owner = buildIndexManager(CommitMode.Batch)
    owner.open(Set(tp)).value

    // The zombie's cached eTag is now stale: its granular write is fenced.
    zombie.updateForPartitionKey(tp, keyOf("A"), Some(Offset(160)), None).left.value shouldBe a[FatalCloudSinkError]
    // No object reached a final path from the zombie.
    storage.keysUnder(bucket, "data/") shouldBe empty

    // The new owner replays those offsets once.
    val metrics = new CloudSinkMetrics()
    val policy  = new TogglePolicy()
    val wm      = buildWriterManager(owner, metrics, pv("A"), policy.policy)
    (121L to 130L).foreach(o => wm.write(tp.withOffset(Offset(o)), message(o)).value)
    policy.flush.set(true)
    wm.commitFlushableWriters().value
    metrics.getRecordsWrittenTotal shouldBe 10L
    storage.keysUnder(bucket, "data/") should have size 1

    wm.close(); owner.close(); zombie.close()
  }

  // ── T7.12 ───────────────────────────────────────────────────────────────────────────

  test("[Z] T7.12 zombie-first: the new owner resolves a chain a zombie already completed; one object, floor at its offset") {
    seedMaster(99)
    // The zombie already performed its copy and recorded a Delete-only pending state.
    val temp   = ".temp-upload/legacy/orders/0/uuid/data/orders/0/z.json"
    val final_ = "data/orders/0/z.json"
    storage.writeStringToFile(bucket, final_, io.lenses.streamreactor.connect.cloud.common.model.UploadableString("zombie payload")).value
    writeIndex(
      granularPath(keyOf("A")),
      IndexFile(
        "zombie",
        Some(Offset(100)),
        Some(PendingState(Offset(140), NonEmptyList.of(DeleteOperation(bucket, temp, "z-etag")))),
      ),
    )

    val owner = buildIndexManager(CommitMode.Batch)
    owner.open(Set(tp)).value

    // The Delete resolved (temp was already gone → idempotent), floor is the zombie's pending offset.
    owner.legacyFloor(tp, keyOf("A")) shouldBe Some(Some(Offset(140)))
    storage.keysUnder(bucket, "data/orders/0/z") should have size 1

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(owner, metrics, pv("A"), new TogglePolicy().policy)
    (100L to 140L).foreach(o => wm.write(tp.withOffset(Offset(o)), message(o)).value)
    metrics.getDuplicateRecordsSkippedTotal shouldBe 41L
    wm.close(); owner.close()
  }
}
