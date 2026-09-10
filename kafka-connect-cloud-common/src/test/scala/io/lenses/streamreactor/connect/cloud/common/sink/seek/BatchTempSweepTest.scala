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
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfMetadataResponse
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailMoveAt
import org.scalatest.BeforeAndAfterEach
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant

/**
 * The batch-mode `.temp-upload` orphan sweep (§2.7). It runs at the end of batch-mode `open()`,
 * strictly after master resolution, the ownership bump and the legacy snapshot, and reaps aged
 * temp objects under the connector-scoped prefix without ever touching anything else.
 */
class BatchTempSweepTest
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with BeforeAndAfterEach {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("sweep-test", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => Validated.valid(location)
  private implicit val indexFileEncoder: Encoder[IndexFile] = IndexFile.indexFileEncoder

  private val bucket        = "test-bucket"
  private val directoryName = ".indexes"
  private val tp            = Topic("orders").withPartition(0)
  private val ageSeconds    = 3600

  private def scopedPrefix = s".temp-upload/${connectorTaskId.name}/${tp.topic}/${tp.partition}/"
  private def masterPath   = IndexManagerV2.generateLockFilePath(connectorTaskId, tp, directoryName)

  private var storage: InMemoryStorageInterface = _

  override def beforeEach(): Unit = storage = new InMemoryStorageInterface()

  private def bucketAndPrefix(topicPartition: TopicPartition): Either[SinkError, CloudLocation] =
    CloudLocation(bucket, Some(s"data/${topicPartition.topic.value}/${topicPartition.partition}/")).asRight

  private def buildIndexManager(st: InMemoryStorageInterface = storage): IndexManagerV2 = {
    implicit val si: InMemoryStorageInterface = st
    new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = new PendingOperationsProcessors(st),
      directoryFileName           = directoryName,
      gcIntervalSeconds           = Int.MaxValue,
      gcSweepIntervalSeconds      = ageSeconds,
      gcSweepMinAgeSeconds        = ageSeconds,
      gcSweepEnabled              = false,
      commitMode                  = CommitMode.Batch,
    )(si, connectorTaskId)
  }

  private def seedMaster(committed: Long, pending: Option[PendingState] = None): Unit = {
    val _ = storage.writeBlobToFile(
      bucket,
      masterPath,
      NoOverwriteExistingObject(IndexFile("prev-owner", Some(Offset(committed)), pending)),
    ).value
  }

  private def writeTemp(path: String, ageSecondsAgo: Int): Unit = {
    storage.writeStringToFile(bucket, path, UploadableString("payload")).value
    val _ = storage.setLastModified(bucket, path, Instant.now().minusSeconds(ageSecondsAgo.toLong))
  }

  // ── T9.4 ────────────────────────────────────────────────────────────────────────────

  test("[B] T9.4 an aged unreferenced temp under the scoped prefix is swept; a fresh one is kept") {
    seedMaster(99)
    val old   = scopedPrefix + "uuid-old/data/orders/0/old.json"
    val fresh = scopedPrefix + "uuid-fresh/data/orders/0/fresh.json"
    writeTemp(old, ageSecondsAgo = ageSeconds + 60)
    writeTemp(fresh, ageSecondsAgo = 0)

    val im = buildIndexManager()
    try {
      im.open(Set(tp)).value
      storage.snapshot(bucket).keys should not contain old
      storage.snapshot(bucket).keys should contain(fresh)
    } finally im.close()
  }

  // ── T9.5 ────────────────────────────────────────────────────────────────────────────

  test("[B] T9.5 temps under another connector or the granular layout are never touched") {
    seedMaster(99)
    val otherConnector = s".temp-upload/other-connector/${tp.topic}/${tp.partition}/uuid/data/x.json"
    val granularLayout = s".temp-upload/${tp.topic}/${tp.partition}/uuid/data/y.json"
    writeTemp(otherConnector, ageSecondsAgo = ageSeconds + 60)
    writeTemp(granularLayout, ageSecondsAgo = ageSeconds + 60)

    val im = buildIndexManager()
    try {
      im.open(Set(tp)).value
      storage.snapshot(bucket).keys should contain(otherConnector)
      storage.snapshot(bucket).keys should contain(granularLayout)
    } finally im.close()
  }

  // ── T9.2 ────────────────────────────────────────────────────────────────────────────

  test("[NL] T9.2 a temp referenced by a resolvable pending Copy is completed by open, not swept first") {
    val temp  = scopedPrefix + "uuid/data/orders/0/pending.json"
    val final_ = "data/orders/0/pending.json"
    storage.writeStringToFile(bucket, temp, UploadableString("pending payload")).value
    storage.setLastModified(bucket, temp, Instant.now().minusSeconds((ageSeconds + 600).toLong)) shouldBe true
    val tempETag = storage.snapshot(bucket)(temp).eTag
    seedMaster(
      99,
      Some(PendingState(Offset(150), NonEmptyList.of(CopyOperation(bucket, temp, final_, tempETag)))),
    )

    val im = buildIndexManager()
    try {
      im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(150)))
      // The copy completed: the final object exists. The sweep did not delete the temp before
      // resolution (it ran after, and the resolved pending state is None so nothing lingered).
      storage.keysUnder(bucket, "data/orders/0/pending") should have size 1
    } finally im.close()
  }

  // ── T9.3 ────────────────────────────────────────────────────────────────────────────

  test("[NL] T9.3 a temp referenced by a pending state that open could not resolve is not deleted") {
    val temp  = scopedPrefix + "uuid/data/orders/0/stuck.json"
    val final_ = "data/orders/0/stuck.json"
    storage.writeStringToFile(bucket, temp, UploadableString("stuck payload")).value
    storage.setLastModified(bucket, temp, Instant.now().minusSeconds((ageSeconds + 600).toLong)) shouldBe true
    val tempETag = storage.snapshot(bucket)(temp).eTag
    seedMaster(
      99,
      Some(PendingState(Offset(150), NonEmptyList.of(CopyOperation(bucket, temp, final_, tempETag)))),
    )
    // The copy fails, so open() returns Left before the sweep would ever run.
    storage.arm(FailMoveAt(bucket, temp))

    val im = buildIndexManager()
    try {
      // open returns Left (the copy failed); a single-op [Copy] chain is a last-op NonFatal.
      im.open(Set(tp)).isLeft shouldBe true
      // The temp survives: open short-circuited on the failed resolution, so the sweep never ran.
      storage.snapshot(bucket).keys should contain(temp)
    } finally im.close()
  }

  // ── T9.6 ────────────────────────────────────────────────────────────────────────────

  test("[B] T9.6 a sweep LIST failure leaves open() successful") {
    seedMaster(99)
    val listFailing = new InMemoryStorageInterface() {
      override def listFileMetaRecursive(
        b:      String,
        prefix: Option[String],
      ): Either[FileListError, Option[ListOfMetadataResponse[FakeFileMetadata]]] =
        if (prefix.exists(_.startsWith(".temp-upload/")))
          FileListError(new RuntimeException("boom"), b, prefix).asLeft
        else super.listFileMetaRecursive(b, prefix)
    }
    storage = listFailing
    seedMaster(99)

    val im = buildIndexManager(listFailing)
    try {
      im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(99)))
    } finally im.close()
  }
}
