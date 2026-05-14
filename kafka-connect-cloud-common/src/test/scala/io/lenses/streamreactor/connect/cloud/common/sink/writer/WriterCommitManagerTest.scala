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
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File

class WriterCommitManagerTest
    extends AnyFunSuiteLike
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with EitherValues {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("test-connector", 1, 1)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (loc: CloudLocation) => Validated.fromEither(Right(loc))

  private val topicPartition  = Topic("topic").withPartition(0)
  private val topicPartitionB = Topic("topic").withPartition(1)
  private val partitionField  = PartitionField(Seq("_value")).value
  private val partitionFieldB = PartitionField(Seq("_value_b")).value

  private def uploadingState: Uploading =
    Uploading(CommitState(topicPartition, None), new File("nonexistent-staging"), Offset(1), Offset(1), 1L, 1L, 1L)

  private def writingState: Writing = {
    val fw = mock[FormatWriter]
    when(fw.complete()).thenReturn(Right(()))
    Writing(CommitState(topicPartition, None), fw, new File("nonexistent-staging"), Offset(1), Offset(1), 1L, 1L)
  }

  /**
   * Creates a real `Writer[FileMetadata]` with mocked cloud collaborators forced into
   * `initialState`. Assertions on post-commit writer state (`isIdle`, `hasPendingUpload`)
   * exercise the real `Writer` state machine rather than mock interactions.
   *
   * `failWith`: when set, `objectKeyBuilder.build` returns that error so the commit fails
   * and the writer stays in `Uploading` state.
   * `shouldFlushResult`: controls whether a `Writing`-state writer satisfies the
   * `shouldFlush` predicate used by `commitFlushableWriters*`.
   */
  private def makeWriter(
    initialState:      WriteState,
    failWith:          Option[FatalCloudSinkError] = None,
    shouldFlushResult: Boolean                     = false,
  ): Writer[FileMetadata] = {
    val commitPolicy   = mock[CommitPolicy]
    val indexManager   = mock[IndexManager]
    val objectKeyBld   = mock[ObjectKeyBuilder]
    val pendingOps     = mock[PendingOperationsProcessors]
    val schemaDetector = mock[SchemaChangeDetector]

    when(indexManager.indexingEnabled).thenReturn(false)
    when(commitPolicy.shouldFlush(any[CommitContext])).thenReturn(shouldFlushResult)

    type IndexUpdateFn =
      (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]

    failWith match {
      case None =>
        when(objectKeyBld.build(any[Offset], any[Offset], any[Long], any[Long], any[Long]))
          .thenReturn(Right(CloudLocation("test-bucket", path = Some("test/path"))))
        when(
          pendingOps.processPendingOperations(
            any[TopicPartition],
            any[Option[Offset]],
            any[PendingState],
            any[IndexUpdateFn],
            any[Boolean],
            any[Option[String]],
            any[Option[File]],
          ),
        ).thenReturn(Right(Some(Offset(1))))
      case Some(err) =>
        when(objectKeyBld.build(any[Offset], any[Offset], any[Long], any[Long], any[Long])).thenReturn(Left(err))
    }

    val writer = new Writer[FileMetadata](
      topicPartition,
      commitPolicy,
      indexManager,
      () => Right(new File("nonexistent-staging")),
      objectKeyBld,
      _ => Right(mock[FormatWriter]),
      schemaDetector,
      pendingOps,
    )
    writer.forceWriteState(initialState)
    writer
  }

  private def createManager(
    writers: Map[MapKey, Writer[FileMetadata]],
    metrics: CloudSinkMetrics = new CloudSinkMetrics(),
  ) =
    new WriterCommitManager(() => writers, metrics)

  // commitPending is selective: only writers in Uploading state are picked up.
  // A sibling in Writing state on the same topic-partition is left open and will
  // commit when its own flush trigger fires.

  test("commitPending commits only Uploading writers; Writing sibling on the same TP stays open") {
    val uploadingWriter = makeWriter(uploadingState)
    val writingWriter   = makeWriter(writingState)
    val manager = createManager(
      Map(
        MapKey(topicPartition, Map(partitionField -> "value")) -> uploadingWriter,
        MapKey(topicPartition, Map(partitionFieldB -> "sibling")) -> writingWriter,
      ),
    )

    manager.commitPending().value shouldBe ()
    uploadingWriter.isIdle shouldBe true
    writingWriter.isIdle shouldBe false
    writingWriter.hasPendingUpload shouldBe false
  }

  // All four public methods route through the same private commitWritersWithFilter helper,
  // so error aggregation only needs one test. Pins: (a) a failure in one writer does not
  // roll back a successful commit in the same batch; (b) BatchCloudSinkError round-trips
  // the error identity correctly.

  test("commitPending aggregates errors and successful commits proceed independently") {
    val successWriter = makeWriter(uploadingState)
    val failingWriter =
      makeWriter(uploadingState, failWith = Some(FatalCloudSinkError("commit failed", topicPartition)))
    val manager = createManager(
      Map(
        MapKey(topicPartition, Map.empty) -> successWriter,
        MapKey(topicPartition, Map(partitionField -> "value")) -> failingWriter,
      ),
    )

    manager.commitPending().left.value shouldBe BatchCloudSinkError(
      Set(FatalCloudSinkError("commit failed", topicPartition)),
    )
    successWriter.isIdle shouldBe true           // successful commit not rolled back
    failingWriter.hasPendingUpload shouldBe true // failed writer still needs retry
  }

  // INTENT PIN: schema-rollover path. commitForTopicPartition commits EVERY writer on the
  // target TP regardless of individual state. Do NOT weaken this to a selective filter
  // without a format-compatibility review (see WriterManager.rollOverTopicPartitionWriters).
  // The cross-TP writer in this fixture proves the TP boundary is honoured.

  test("commitForTopicPartition commits ALL writers on TP-A regardless of state; TP-B writer is untouched") {
    val uploadingA = makeWriter(uploadingState) // TP-A, Uploading
    val writingA   = makeWriter(writingState)   // TP-A, Writing (non-flushable) — still commits here
    val writingB   = makeWriter(writingState)   // keyed under TP-B — must NOT commit
    val manager = createManager(
      Map(
        MapKey(topicPartition, Map.empty) -> uploadingA,
        MapKey(topicPartition, Map(partitionField -> "value")) -> writingA,
        MapKey(topicPartitionB, Map.empty) -> writingB,
      ),
    )

    manager.commitForTopicPartition(topicPartition).value shouldBe ()
    uploadingA.isIdle shouldBe true
    writingA.isIdle shouldBe true
    writingB.isIdle shouldBe false // TP-B writer was never selected
    writingB.hasPendingUpload shouldBe false
  }

  // Selective: only writers whose shouldFlush predicate is true are committed.
  // A non-flushable sibling on the same topic-partition keeps buffering.

  test("commitFlushableWriters commits only flushable writers; non-flushable sibling stays open") {
    val flushable    = makeWriter(writingState, shouldFlushResult = true)
    val notFlushable = makeWriter(writingState, shouldFlushResult = false)
    val manager = createManager(
      Map(
        MapKey(topicPartition, Map(partitionField -> "v1")) -> flushable,
        MapKey(topicPartition, Map(partitionFieldB -> "v2")) -> notFlushable,
      ),
    )

    manager.commitFlushableWriters().value shouldBe ()
    flushable.isIdle shouldBe true
    notFlushable.isIdle shouldBe false
    notFlushable.hasPendingUpload shouldBe false
  }

  // Two invariants in one fixture: (a) non-flushable sibling on the target TP stays open;
  // (b) a flushable writer on a different TP is not committed.

  test(
    "commitFlushableWritersForTopicPartition commits only TP-A flushable writer; non-flushable and TP-B writer are untouched",
  ) {
    val flushableA    = makeWriter(writingState, shouldFlushResult = true)  // TP-A, flushable
    val notFlushableA = makeWriter(writingState, shouldFlushResult = false) // TP-A, non-flushable
    val flushableB    = makeWriter(writingState, shouldFlushResult = true)  // TP-B, flushable — different TP
    val manager = createManager(
      Map(
        MapKey(topicPartition, Map.empty) -> flushableA,
        MapKey(topicPartition, Map(partitionField -> "value")) -> notFlushableA,
        MapKey(topicPartitionB, Map.empty) -> flushableB,
      ),
    )

    manager.commitFlushableWritersForTopicPartition(topicPartition).value shouldBe ()
    flushableA.isIdle shouldBe true     // selected: TP-A + flushable
    notFlushableA.isIdle shouldBe false // filtered out: not flushable
    notFlushableA.hasPendingUpload shouldBe false
    flushableB.isIdle shouldBe false // filtered out: wrong TP
    flushableB.hasPendingUpload shouldBe false
  }

  // With 1-of-4 writers committed, the counters must reflect exactly how many sibling
  // commits were avoided and what fraction of the TP set was selected.

  test("selective-commit metrics: avoidedSiblingCommits and flushFanOutRatio reflect 1-of-4 fixture") {
    val metrics         = new CloudSinkMetrics()
    val flushableWriter = makeWriter(writingState, shouldFlushResult = true)
    val sibling1        = makeWriter(writingState, shouldFlushResult = false)
    val sibling2        = makeWriter(writingState, shouldFlushResult = false)
    val sibling3        = makeWriter(writingState, shouldFlushResult = false)

    val manager = createManager(
      Map(
        MapKey(topicPartition, Map(partitionField -> "v1")) -> flushableWriter,
        MapKey(topicPartition, Map(partitionField -> "v2")) -> sibling1,
        MapKey(topicPartition, Map(partitionField -> "v3")) -> sibling2,
        MapKey(topicPartition, Map(partitionField -> "v4")) -> sibling3,
      ),
      metrics,
    )

    manager.commitFlushableWritersForTopicPartition(topicPartition).value shouldBe ()

    flushableWriter.isIdle shouldBe true
    Seq(sibling1, sibling2, sibling3).foreach { s =>
      s.isIdle shouldBe false
      s.hasPendingUpload shouldBe false
    }

    metrics.getSelectiveCommitInvocations shouldBe 1L
    metrics.getSelectiveCommitWritersCommitted shouldBe 1L
    metrics.getSelectiveCommitAvoidedSiblingCommits shouldBe 3L
    metrics.getSelectedWritersPerFlush shouldBe 1
    metrics.getActiveWritersPerTopicPartition shouldBe 4
    metrics.getFlushFanOutRatio shouldBe (1.0d / 4.0d) +- 1e-9
  }

  // When the trigger matches no writers the invocation is still recorded and the fan-out
  // ratio falls back to 1.0 (empty selection, no benefit to quantify).

  test("selective-commit metrics: zero-match cycle records invocation; fan-out ratio defaults to 1.0") {
    val metrics = new CloudSinkMetrics()
    val writer  = makeWriter(writingState, shouldFlushResult = false) // Writing, not Uploading
    val manager = createManager(Map(MapKey(topicPartition, Map.empty) -> writer), metrics)

    manager.commitPending().value shouldBe ()

    writer.isIdle shouldBe false
    writer.hasPendingUpload shouldBe false

    metrics.getSelectiveCommitInvocations shouldBe 1L
    metrics.getSelectiveCommitWritersCommitted shouldBe 0L
    metrics.getSelectiveCommitAvoidedSiblingCommits shouldBe 0L
    metrics.getFlushFanOutRatio shouldBe 1.0d
  }
}
