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

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class WriterCommitManagerTest
    extends AnyFunSuiteLike
    with BeforeAndAfterEach
    with Matchers
    with MockitoSugar
    with EitherValues {

  private implicit val connectorTaskId: ConnectorTaskId = mock[ConnectorTaskId]
  private val topicPartition  = Topic("topic").withPartition(0)
  private val partitionField  = PartitionField(Seq("_value")).value
  private val partitionFieldB = PartitionField(Seq("_value_b")).value

  override protected def beforeEach(): Unit = reset(mock[ConnectorTaskId])

  private def createManager(
    writers: Map[MapKey, Writer[FileMetadata]],
    metrics: CloudSinkMetrics = new CloudSinkMetrics(),
  ) =
    new WriterCommitManager(() => writers, metrics)

  private def createMockWriter(
    hasPendingUpload: Boolean                           = false,
    shouldFlush:      Boolean                           = false,
    commitResult:     Either[FatalCloudSinkError, Unit] = ().asRight,
  ): Writer[FileMetadata] = {
    val mockWriter = mock[Writer[FileMetadata]]
    when(mockWriter.hasPendingUpload).thenReturn(hasPendingUpload)
    when(mockWriter.shouldFlush).thenReturn(shouldFlush)
    when(mockWriter.commit).thenReturn(commitResult)
    mockWriter
  }

  // ── commitPending ────────────────────────────────────────────────────────────────────

  test("commitPending should commit writers with pending uploads") {
    val mockWriter = createMockWriter(hasPendingUpload = true)
    val manager    = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitPending().value shouldBe ()
    verify(mockWriter).commit
  }

  test("commitPending should return an error if any writer fails to commit") {
    val mockWriter = createMockWriter(hasPendingUpload = true,
                                      commitResult = FatalCloudSinkError("commit failed", topicPartition).asLeft,
    )
    val manager = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitPending().left.value shouldBe BatchCloudSinkError(Set(FatalCloudSinkError("commit failed",
                                                                                            topicPartition,
    )))
  }

  // Selective-commit semantics: only writers in `Uploading` (hasPendingUpload=true) commit.
  // Sibling Writing-state writers on the same TopicPartition are NOT pulled in by the
  // pending-retry path. This test replaces the previous fan-out-pinning test that
  // expected `verify(mockWriter2).commit` for a non-pending sibling.
  test("commitPending commits only Uploading writers; sibling Writing writers are not pulled in") {
    val pendingWriter = createMockWriter(hasPendingUpload = true)
    val siblingWriter = createMockWriter(hasPendingUpload = false)
    val manager = createManager(
      Map(
        MapKey(topicPartition, Map(partitionField -> "value")) -> pendingWriter,
        MapKey(topicPartition, Map(partitionFieldB -> "sibling")) -> siblingWriter,
      ),
    )

    manager.commitPending().value shouldBe ()
    verify(pendingWriter).commit
    verify(siblingWriter, never).commit
  }

  // Rewrite of the old "commitPending should return an error if any writer fails to commit
  // for the given topic partition" — under selective commit both writers must be Uploading
  // for the multi-failure aggregation contract to kick in. With one in Writing the old
  // assertion (`verify(mockWriter2).commit`) would never hold under selective commit.
  test("commitPending aggregates errors across multiple Uploading writers on the same topic partition") {
    val mockWriter1 = createMockWriter(hasPendingUpload = true)
    val mockWriter2 =
      createMockWriter(hasPendingUpload = true,
                       commitResult     = FatalCloudSinkError("commit failed", topicPartition).asLeft,
      )
    val manager = createManager(Map(
      MapKey(topicPartition, Map.empty) -> mockWriter1,
      MapKey(topicPartition, Map(partitionField -> "value")) -> mockWriter2,
    ))

    manager.commitPending().left.value shouldBe BatchCloudSinkError(Set(FatalCloudSinkError("commit failed",
                                                                                            topicPartition,
    )))
    verify(mockWriter1).commit
    verify(mockWriter2).commit
  }

  // ── commitForTopicPartition (schema rollover only — full fan-out preserved) ──────────

  // INTENT PIN: schema-rollover path. `commitForTopicPartition` is the only commit method
  // that retains full-fan-out behaviour because every writer on the topic-partition must
  // flush together at a format boundary. Do NOT weaken this test or its underlying method
  // to selective without a separate format-compatibility review (see `WriterManager.rollOverTopicPartitionWriters`
  // and the "Selective commit fan-out" subsection in `docs/datalake-exactly-once-partitionby.md`).
  test("commitForTopicPartition should commit all writers for the given topic partition if one is committed") {
    val mockWriter1 = createMockWriter()
    val mockWriter2 = createMockWriter()
    val manager = createManager(Map(
      MapKey(topicPartition, Map.empty) -> mockWriter1,
      MapKey(topicPartition, Map(partitionField -> "value")) -> mockWriter2,
    ))

    manager.commitForTopicPartition(topicPartition).value shouldBe ()
    verify(mockWriter1).commit
    verify(mockWriter2).commit
  }

  test("commitForTopicPartition should return an error if any writer fails to commit for the given topic partition") {
    val mockWriter1 = createMockWriter()
    val mockWriter2 = createMockWriter(commitResult = FatalCloudSinkError("commit failed", topicPartition).asLeft)
    val manager = createManager(Map(
      MapKey(topicPartition, Map.empty) -> mockWriter1,
      MapKey(topicPartition, Map(partitionField -> "value")) -> mockWriter2,
    ))

    manager.commitForTopicPartition(topicPartition).left.value shouldBe BatchCloudSinkError(Set(FatalCloudSinkError(
      "commit failed",
      topicPartition,
    )))
    verify(mockWriter1).commit
    verify(mockWriter2).commit
  }

  test("commitForTopicPartition should commit writers for the given topic partition") {
    val mockWriter = createMockWriter()
    val manager    = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitForTopicPartition(topicPartition).value shouldBe ()
    verify(mockWriter).commit
  }

  test("commitForTopicPartition should return an error if any writer fails to commit") {
    val mockWriter = createMockWriter(commitResult = FatalCloudSinkError("commit failed", topicPartition).asLeft)
    val manager    = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitForTopicPartition(topicPartition).left.value shouldBe BatchCloudSinkError(Set(FatalCloudSinkError(
      "commit failed",
      topicPartition,
    )))
  }

  // ── commitFlushableWriters ───────────────────────────────────────────────────────────

  test("commitFlushableWriters should commit writers that require flush") {
    val mockWriter = createMockWriter(shouldFlush = true)
    val manager    = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitFlushableWriters().value shouldBe ()
    verify(mockWriter).commit
  }

  test("commitFlushableWriters should return an error if any writer fails to commit") {
    val mockWriter =
      createMockWriter(shouldFlush = true, commitResult = FatalCloudSinkError("commit failed", topicPartition).asLeft)
    val manager = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitFlushableWriters().left.value shouldBe BatchCloudSinkError(Set(FatalCloudSinkError("commit failed",
                                                                                                     topicPartition,
    )))
  }

  // Selective: only the flushable writer commits; the non-flushable sibling on the same
  // topic-partition keeps buffering. This is the headline behavioural change in WS1.
  test("commitFlushableWriters commits only flushable writers; non-flushable siblings stay open") {
    val flushableWriter    = createMockWriter(shouldFlush = true)
    val nonFlushableWriter = createMockWriter(shouldFlush = false)
    val manager = createManager(
      Map(
        MapKey(topicPartition, Map(partitionField -> "v1")) -> flushableWriter,
        MapKey(topicPartition, Map(partitionFieldB -> "v2")) -> nonFlushableWriter,
      ),
    )

    manager.commitFlushableWriters().value shouldBe ()
    verify(flushableWriter).commit
    verify(nonFlushableWriter, never).commit
  }

  // ── commitFlushableWritersForTopicPartition ──────────────────────────────────────────

  test("commitFlushableWritersForTopicPartition should commit flushable writers for the given topic partition") {
    val mockWriter = createMockWriter(shouldFlush = true)
    val manager    = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitFlushableWritersForTopicPartition(topicPartition).value shouldBe ()
    verify(mockWriter).commit
  }

  test("commitFlushableWritersForTopicPartition should continue if no writers require flushing") {
    val mockWriter = createMockWriter()
    val manager    = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitFlushableWritersForTopicPartition(topicPartition).value shouldBe ()
    verify(mockWriter, times(0)).commit
  }

  // Selective replacement for "commitFlushableWritersForTopicPartition should commit all
  // writers for the given topic partition if one is flushable": only the flushable writer
  // commits; the non-flushable sibling stays untouched.
  test("commitFlushableWritersForTopicPartition commits only flushable writers; non-flushable siblings stay open") {
    val mockWriter1 = createMockWriter(shouldFlush = true)
    val mockWriter2 = createMockWriter(shouldFlush = false)
    val manager = createManager(Map(
      MapKey(topicPartition, Map.empty) -> mockWriter1,
      MapKey(topicPartition, Map(partitionField -> "value")) -> mockWriter2,
    ))

    manager.commitFlushableWritersForTopicPartition(topicPartition).value shouldBe ()
    verify(mockWriter1).commit
    verify(mockWriter2, never).commit
  }

  // Rewrite of the previous "should return an error if any writer fails to commit for the
  // given topic partition" test. Both writers are flushable so the multi-failure
  // aggregation contract is exercised under selective commit (the old version had
  // `mockWriter2.shouldFlush = false`, which was only reachable via the legacy fan-out).
  test("commitFlushableWritersForTopicPartition aggregates errors across multiple flushable writers on the TP") {
    val mockWriter1 = createMockWriter(shouldFlush = true)
    val mockWriter2 =
      createMockWriter(shouldFlush = true, commitResult = FatalCloudSinkError("commit failed", topicPartition).asLeft)
    val manager = createManager(Map(
      MapKey(topicPartition, Map.empty) -> mockWriter1,
      MapKey(topicPartition, Map(partitionField -> "value")) -> mockWriter2,
    ))

    manager.commitFlushableWritersForTopicPartition(topicPartition).left.value shouldBe BatchCloudSinkError(
      Set(FatalCloudSinkError("commit failed", topicPartition)),
    )
    verify(mockWriter1).commit
    verify(mockWriter2).commit
  }

  // ── Selective-commit metrics ─────────────────────────────────────────────────────────

  // Headline benefit assertion: with one flushable writer and (N-1) non-flushable siblings
  // on the same topic-partition, `selectiveCommitAvoidedSiblingCommits` records exactly
  // (N-1). `flushFanOutRatio` reflects 1/N — the share of writers actually committed
  // versus the would-have-been-fanned-out set under the old step-2 expansion.
  test("selective-commit metrics: avoidedSiblingCommits and flushFanOutRatio reflect 1-of-N fixture") {
    val metrics         = new CloudSinkMetrics()
    val flushableWriter = createMockWriter(shouldFlush = true)
    val sibling1        = createMockWriter(shouldFlush = false)
    val sibling2        = createMockWriter(shouldFlush = false)
    val sibling3        = createMockWriter(shouldFlush = false)

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

    metrics.getSelectiveCommitInvocations shouldBe 1L
    metrics.getSelectiveCommitWritersCommitted shouldBe 1L
    metrics.getSelectiveCommitAvoidedSiblingCommits shouldBe 3L
    metrics.getSelectedWritersPerFlush shouldBe 1
    metrics.getActiveWritersPerTopicPartition shouldBe 4
    metrics.getFlushFanOutRatio shouldBe (1.0d / 4.0d) +- 1e-9
  }

  // When the trigger matches no writers (e.g. empty-poll with no pending uploads) the
  // metrics still record the invocation but with zeros. `flushFanOutRatio` falls back to
  // 1.0 (degenerate; the matching set is empty so there is no benefit to report).
  test("selective-commit metrics: zero-match cycle records the invocation and leaves fan-out ratio at 1.0") {
    val metrics    = new CloudSinkMetrics()
    val noopWriter = createMockWriter(hasPendingUpload = false)
    val manager    = createManager(Map(MapKey(topicPartition, Map.empty) -> noopWriter), metrics)

    manager.commitPending().value shouldBe ()

    metrics.getSelectiveCommitInvocations shouldBe 1L
    metrics.getSelectiveCommitWritersCommitted shouldBe 0L
    metrics.getSelectiveCommitAvoidedSiblingCommits shouldBe 0L
    metrics.getFlushFanOutRatio shouldBe 1.0d
    verify(noopWriter, never).commit
  }
}
