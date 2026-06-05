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
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import org.apache.kafka.connect.data.Schema
import org.mockito.Answers
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.io.IOException
import java.time.Instant

class WriterTest extends AnyFunSuiteLike with Matchers with MockitoSugar with ArgumentMatchersSugar with EitherValues {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("test-connector", 1, 1)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (loc: CloudLocation) => cats.data.Validated.fromEither(Right(loc))

  private val commitPolicy:                CommitPolicy                            = mock[CommitPolicy]
  private val writerIndexer:               IndexManager                            = mock[IndexManager]
  private val objectKeyBuilder:            ObjectKeyBuilder                        = mock[ObjectKeyBuilder]
  private val formatWriter:                FormatWriter                            = mock[FormatWriter]
  private val stagingFilenameFn:           () => Either[SinkError, File]           = () => Right(new File("test-file"))
  private val formatWriterFn:              File => Either[SinkError, FormatWriter] = _ => Right(formatWriter)
  private val topicPartition:              TopicPartition                          = Topic("test-topic").withPartition(0)
  private val schemaChangeDetector:        SchemaChangeDetector                    = mock[SchemaChangeDetector]
  private val pendingOperationsProcessors: PendingOperationsProcessors             = mock[PendingOperationsProcessors]

  test("shouldSkip should return false when indexing is disabled") {
    when(writerIndexer.indexingEnabled).thenReturn(false)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.shouldSkip(Offset(100)) shouldBe false
  }

  test(
    "shouldSkip should return true when current offset is less than or equal to committed offset in NoWriter state",
  ) {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(100)))))
    writer.shouldSkip(Offset(100)) shouldBe true
    writer.shouldSkip(Offset(99)) shouldBe true
  }

  test("shouldSkip should return false when current offset is greater than committed offset in NoWriter state") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(100)))))
    writer.shouldSkip(Offset(101)) shouldBe false
  }

  test("shouldSkip should return true when current offset is less than or equal to largest offset in Uploading state") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(
      Uploading(
        CommitState(topicPartition, Some(Offset(100))),
        new File("test-file"),
        firstBufferedOffset = Offset(100),
        Offset(150),
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 1L,
        recordCount             = 1L,
      ),
    )
    writer.shouldSkip(Offset(150)) shouldBe true
    writer.shouldSkip(Offset(149)) shouldBe true
  }

  test("shouldSkip should return false when current offset is greater than largest offset in Uploading state") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(
      Uploading(CommitState(topicPartition, Some(Offset(100))),
                new File("test-file"),
                Offset(100),
                Offset(150),
                1L,
                1L,
                1L,
      ),
    )
    writer.shouldSkip(Offset(151)) shouldBe false
  }

  test("shouldSkip should return true when current offset is less than or equal to largest offset in Writing state") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(
      Writing(CommitState(topicPartition, Some(Offset(100))),
              formatWriter,
              new File("test-file"),
              Offset(100),
              Offset(150),
              1L,
              1L,
      ),
    )
    writer.shouldSkip(Offset(150)) shouldBe true
    writer.shouldSkip(Offset(149)) shouldBe true
  }

  test("shouldSkip should return false when current offset is greater than largest offset in Writing state") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(
      Writing(CommitState(topicPartition, Some(Offset(100))),
              formatWriter,
              new File("test-file"),
              Offset(100),
              Offset(150),
              1L,
              1L,
      ),
    )
    writer.shouldSkip(Offset(151)) shouldBe false
  }

  test("shouldRollover returns true when rollover is enabled and schema has changed") {
    val schema = mock[Schema]
    val writer = spy(
      new Writer[FileMetadata](topicPartition,
                               commitPolicy,
                               writerIndexer,
                               stagingFilenameFn,
                               objectKeyBuilder,
                               formatWriterFn,
                               schemaChangeDetector,
                               pendingOperationsProcessors,
      ),
    )
    when(writer.schemaHasChanged(schema)).thenReturn(true)
    when(writer.rolloverOnSchemaChange).thenReturn(true)
    writer.shouldRollover(schema) shouldBe true
  }

  test("schemaHasChanged should return true when schema has changed in Writing state") {
    val schema       = mock[Schema]
    val lastSchema   = mock[Schema]
    val formatWriter = mock[FormatWriter](Answers.RETURNS_DEEP_STUBS)
    val commitState = CommitState(topicPartition, Some(Offset(100)))
      .copy(lastKnownSchema = Some(lastSchema))
    val writingState = Writing(commitState, formatWriter, new File("test-file"), Offset(100), Offset(150), 1L, 1L)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(writingState)

    when(schemaChangeDetector.detectSchemaChange(lastSchema, schema)).thenReturn(true)

    writer.schemaHasChanged(schema) shouldBe true
  }

  test("schemaHasChanged should return false when schema has not changed in Writing state") {
    val schema       = mock[Schema]
    val lastSchema   = mock[Schema]
    val formatWriter = mock[FormatWriter](Answers.RETURNS_DEEP_STUBS)
    val commitState = CommitState(topicPartition, Some(Offset(100)))
      .copy(lastKnownSchema = Some(lastSchema))
    val writingState = Writing(commitState, formatWriter, new File("test-file"), Offset(100), Offset(150), 1L, 1L)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(writingState)

    when(schemaChangeDetector.detectSchemaChange(lastSchema, schema)).thenReturn(false)

    writer.schemaHasChanged(schema) shouldBe false
  }

  test("schemaHasChanged should return false when not in Writing state") {
    val schema = mock[Schema]
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(100)))))

    writer.schemaHasChanged(schema) shouldBe false
  }

  test("schemaHasChanged should return false when lastKnownSchema is None in Writing state") {
    val schema       = mock[Schema]
    val formatWriter = mock[FormatWriter]
    val commitState  = CommitState(topicPartition, Some(Offset(100)))
    val writingState = Writing(commitState, formatWriter, new File("test-file"), Offset(100), Offset(150), 1L, 1L)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(writingState)

    writer.schemaHasChanged(schema) shouldBe false
  }
  test("schemaHasChanged should return false when lastKnownSchema is the same as the new schema in Writing state") {
    val schema       = mock[Schema]
    val formatWriter = mock[FormatWriter]
    val commitState = CommitState(topicPartition, Some(Offset(100)))
      .copy(lastKnownSchema = Some(schema))
    val writingState = Writing(commitState, formatWriter, new File("test-file"), Offset(100), Offset(150), 1L, 1L)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(writingState)

    writer.schemaHasChanged(schema) shouldBe false
  }

  test(
    "schemaHasChanged should return false when lastKnownSchema is None and schemaHasChanged returns false in Writing state",
  ) {
    val schema       = mock[Schema]
    val formatWriter = mock[FormatWriter](Answers.RETURNS_DEEP_STUBS)
    val commitState  = CommitState(topicPartition, Some(Offset(100)))
    val writingState = Writing(commitState, formatWriter, new File("test-file"), Offset(100), Offset(150), 1L, 1L)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(writingState)

    when(schemaChangeDetector.detectSchemaChange(null, schema)).thenReturn(false)

    writer.schemaHasChanged(schema) shouldBe false
  }

  test("close should delete staging file when in Writing state") {
    val tmpFile = File.createTempFile("writer-test-writing-", ".tmp")
    tmpFile.exists() shouldBe true

    val formatWriter = mock[FormatWriter]
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(
      Writing(CommitState(topicPartition, Some(Offset(100))), formatWriter, tmpFile, Offset(100), Offset(150), 1L, 1L),
    )

    writer.close()

    tmpFile.exists() shouldBe false
    writer.currentWriteState shouldBe a[NoWriter]
  }

  test("close should delete staging file when in Uploading state") {
    val tmpFile = File.createTempFile("writer-test-uploading-", ".tmp")
    tmpFile.exists() shouldBe true

    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(
      Uploading(CommitState(topicPartition, Some(Offset(100))), tmpFile, Offset(100), Offset(150), 1L, 1L, 1L),
    )

    writer.close()

    tmpFile.exists() shouldBe false
    writer.currentWriteState shouldBe a[NoWriter]
  }

  test("close should be a no-op when in NoWriter state") {
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(100)))))

    writer.close()

    writer.currentWriteState shouldBe a[NoWriter]
  }

  // Phase 1a: WriteState tests - firstBufferedOffset

  test("toWriting should set firstBufferedOffset to the first record's offset") {
    val noWriter = NoWriter(CommitState(topicPartition, None))
    val fw       = mock[FormatWriter]
    val result   = noWriter.toWriting(fw, new File("test"), Offset(42), 1000L)
    result.firstBufferedOffset shouldBe Offset(42)
  }

  test("Writing.update should preserve firstBufferedOffset when uncommittedOffset advances") {
    val fw = mock[FormatWriter]
    when(fw.getPointer).thenReturn(100L)
    val writing = Writing(CommitState(topicPartition, None), fw, new File("test"), Offset(42), Offset(42), 1L, 1L)
    val updated = writing.update(Offset(99), 2L, None).asInstanceOf[Writing]
    updated.firstBufferedOffset shouldBe Offset(42)
    updated.uncommittedOffset shouldBe Offset(99)
  }

  test("toUploading should carry firstBufferedOffset from Writing") {
    val fw        = mock[FormatWriter]
    val writing   = Writing(CommitState(topicPartition, None), fw, new File("test"), Offset(42), Offset(99), 1L, 2L)
    val uploading = writing.toUploading
    uploading.firstBufferedOffset shouldBe Offset(42)
  }

  test("getFirstBufferedOffset returns Some in Writing state") {
    val fw = mock[FormatWriter]
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(Writing(CommitState(topicPartition, None),
                                   fw,
                                   new File("test"),
                                   Offset(42),
                                   Offset(50),
                                   1L,
                                   1L,
    ))
    writer.getFirstBufferedOffset shouldBe Some(Offset(42))
  }

  test("getFirstBufferedOffset returns Some in Uploading state") {
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(Uploading(CommitState(topicPartition, None),
                                     new File("test"),
                                     Offset(42),
                                     Offset(50),
                                     1L,
                                     1L,
                                     1L,
    ))
    writer.getFirstBufferedOffset shouldBe Some(Offset(42))
  }

  test("getFirstBufferedOffset returns None in NoWriter state") {
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.getFirstBufferedOffset shouldBe None
  }

  // Phase 1b: Writer tests - per-writer seeked offset and granular lock routing

  test("Writer with partitionKey should use granular lock seeked offset") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
                                          partitionKey     = Some("date=12_00"),
                                          lastSeekedOffset = Some(Offset(200)),
    )
    writer.shouldSkip(Offset(200)) shouldBe true
  }

  test("Writer with partitionKey=None should fallback to master lock seeked offset") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
                                          partitionKey     = None,
                                          lastSeekedOffset = Some(Offset(100)),
    )
    writer.shouldSkip(Offset(100)) shouldBe true
  }

  test("Writer with partitionKey should not skip offsets above granular lock") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
                                          partitionKey     = Some("date=12_00"),
                                          lastSeekedOffset = Some(Offset(200)),
    )
    writer.shouldSkip(Offset(201)) shouldBe false
  }

  test("close should not evict granular lock even when partitionKey is defined") {
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
                                          partitionKey     = Some("date=12_00"),
                                          lastSeekedOffset = Some(Offset(200)),
    )
    writer.close()
    verify(writerIndexer, never).evictGranularLock(any[TopicPartition], any[String])
  }

  test("close should not evict granular lock when partitionKey is None") {
    val freshIndexer = mock[IndexManager]
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          freshIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
                                          partitionKey = None,
    )
    writer.close()
    verify(freshIndexer, never).evictGranularLock(any[TopicPartition], any[String])
  }

  // --- WriteState safety-net + post-success delete-fail regression guards ---

  /**
   * Uploading.toNoWriter(None) must fall back to the existing committedOffset, not None.
   *
   * This is the `.orElse(commitState.committedOffset)` safety net in WriteState.scala.
   * If `processPendingOperations` returns `Right(None)` (e.g., the index manager decided
   * not to advance the offset), the prior committed offset must be preserved so that
   * preCommit does not regress the globalSafeOffset.
   */
  test("Uploading.toNoWriter(None) preserves prior committedOffset via orElse safety net") {
    val priorOffset = Some(Offset(42))
    val uploadState = Uploading(
      CommitState(topicPartition, priorOffset),
      new File("test"),
      firstBufferedOffset     = Offset(50),
      uncommittedOffset       = Offset(55),
      earliestRecordTimestamp = 1L,
      latestRecordTimestamp   = 2L,
      recordCount             = 1L,
    )

    val result = uploadState.toNoWriter(None)

    result.getCommitState.committedOffset shouldBe priorOffset
  }

  /**
   * Post-success local staging-file delete failure does NOT propagate as fatal.
   *
   * After `processPendingOperations` returns Right(newOffset), the writer calls
   * `Try(file.delete())`. If delete fails (here: the file has already been deleted so
   * delete() returns false = Success(false)), the commit must still return Right(()) and
   * the writer must transition to NoWriter with committedOffset = newOffset.
   *
   * This guards the comment in Writer.scala: "A failure to delete the local temp file is
   * a hygiene issue (disk leak) — not a correctness issue — so we must not propagate it
   * as a FatalCloudSinkError."
   */
  test(
    "post-success local staging-file delete failure does not promote to fatal; writer transitions to NoWriter",
  ) {
    val pendingOps   = mock[PendingOperationsProcessors]
    val localIndexer = mock[IndexManager]
    when(localIndexer.indexingEnabled).thenReturn(false)

    val newOffset = Offset(55)

    when(
      pendingOps.processPendingOperations(
        any[TopicPartition],
        any[Option[Offset]],
        any[PendingState],
        any[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]],
        any[Boolean],
        any[Option[String]],
        any[Option[File]],
      ),
    ).thenReturn(Right(Some(newOffset)))

    when(objectKeyBuilder.build(any[Offset], any[Offset], any[Long], any[Long], any[Long]))
      .thenReturn(Right(CloudLocation("bucket", path = Some("path/to/object"))))

    val tmpFile = File.createTempFile("writer-d19-", ".tmp")
    tmpFile.deleteOnExit()

    val writer = new Writer[FileMetadata](
      topicPartition,
      commitPolicy,
      localIndexer,
      () => Right(tmpFile),
      objectKeyBuilder,
      _ => Right(formatWriter),
      schemaChangeDetector,
      pendingOps,
    )

    writer.forceWriteState(
      Uploading(
        CommitState(topicPartition, Some(Offset(40))),
        tmpFile,
        firstBufferedOffset     = Offset(50),
        uncommittedOffset       = Offset(55),
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 2L,
        recordCount             = 1L,
      ),
    )

    // Delete the file first to simulate a "file already gone" scenario —
    // `Try(file.delete())` will return Success(false) which must NOT become a fatal error.
    tmpFile.delete()

    val result = writer.commit

    result.value shouldBe ()
    writer.isIdle shouldBe true
    writer.getCommittedOffset shouldBe Some(newOffset)
  }

  // ─── OS edge cases — staging file + 0-byte flush ───────────────────────────────────────────

  /**
   * Staging file locked by the OS (or otherwise inaccessible) during the Upload phase.
   *
   * In production, `storageInterface.uploadFile` would throw or return Left(UploadFailedError) when
   * the local staging file is locked. `PendingOperationsProcessors` maps this to a
   * `NonFatalCloudSinkError` (unswallowable) so the Kafka Connect framework retries via
   * `recommitPending` on the next poll — the staging file is preserved on disk for the retry.
   *
   * We simulate this at the `Writer.commit` level by stubbing `processPendingOperations` to return
   * the expected NonFatal error and then verify:
   *  - commit propagates the NonFatal (does not escalate to Fatal)
   *  - writer stays in `Uploading` state (ready for recommitPending)
   *  - writer is not idle (staging file path is preserved for retry)
   *
   * Note: on POSIX/Windows, testing a live OS file-lock requires platform-specific syscalls
   * (java.nio.channels.FileLock). We simulate it here at the POP boundary instead.
   */
  test(
    "Upload transient failure (e.g. OS-locked staging file) returns NonFatalCloudSinkError; writer stays in Uploading state for recommitPending",
  ) {
    val pendingOps   = mock[PendingOperationsProcessors]
    val localIndexer = mock[IndexManager]
    when(localIndexer.indexingEnabled).thenReturn(false)

    val accessDeniedErr = NonFatalCloudSinkError.unswallowable(
      "access denied — staging file locked by OS (POSIX simulation)",
      Some(new java.io.IOException("Permission denied")),
    )
    when(
      pendingOps.processPendingOperations(
        any[TopicPartition],
        any[Option[Offset]],
        any[PendingState],
        any[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]],
        any[Boolean],
        any[Option[String]],
        any[Option[File]],
      ),
    ).thenReturn(Left(accessDeniedErr))

    when(objectKeyBuilder.build(any[Offset], any[Offset], any[Long], any[Long], any[Long]))
      .thenReturn(Right(CloudLocation("bucket", path = Some("path/to/object"))))

    val tmpFile = File.createTempFile("writer-d27a-lock-", ".tmp")
    tmpFile.deleteOnExit()

    val writer = new Writer[FileMetadata](
      topicPartition,
      commitPolicy,
      localIndexer,
      () => Right(tmpFile),
      objectKeyBuilder,
      _ => Right(formatWriter),
      schemaChangeDetector,
      pendingOps,
    )

    writer.forceWriteState(
      Uploading(
        CommitState(topicPartition, Some(Offset(40))),
        tmpFile,
        firstBufferedOffset     = Offset(50),
        uncommittedOffset       = Offset(60),
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 2L,
        recordCount             = 1L,
      ),
    )

    val result = writer.commit

    // NonFatal error propagated — not escalated to Fatal.
    result.isLeft shouldBe true
    result.left.value shouldBe a[NonFatalCloudSinkError]
    result.left.value.asInstanceOf[NonFatalCloudSinkError].swallowable shouldBe false

    // Writer stays in Uploading state — staging file preserved for recommitPending retry.
    writer.isIdle shouldBe false
  }

  private def makeMessageDetail(tp: TopicPartition, offset: Offset): MessageDetail =
    MessageDetail(
      key       = NullSinkData(None),
      value     = NullSinkData(None),
      headers   = Map.empty,
      timestamp = Some(Instant.ofEpochMilli(1000L)),
      topic     = tp.topic,
      partition = tp.partition,
      offset    = offset,
    )

  test(
    "REPRO: Writer.write with IOException from FormatWriter must return FatalCloudSinkError, not NonFatalCloudSinkError",
  ) {
    val ioException    = new IOException("No space left on device")
    val ioFormatWriter = mock[FormatWriter]
    when(ioFormatWriter.write(any[MessageDetail])).thenReturn(Left(ioException))
    when(ioFormatWriter.getPointer).thenReturn(0L)
    when(ioFormatWriter.rolloverFileOnSchemaChange()).thenReturn(false)

    val tmpFile = File.createTempFile("writer-ioexception-repro-", ".tmp")
    tmpFile.deleteOnExit()

    val pendingOps   = mock[PendingOperationsProcessors]
    val localIndexer = mock[IndexManager]
    when(localIndexer.indexingEnabled).thenReturn(false)

    val writer = new Writer[FileMetadata](
      topicPartition,
      commitPolicy,
      localIndexer,
      () => Right(tmpFile),
      objectKeyBuilder,
      _ => Right(ioFormatWriter),
      schemaChangeDetector,
      pendingOps,
    )
    writer.forceWriteState(
      Writing(
        CommitState(topicPartition, Some(Offset(99))),
        ioFormatWriter,
        tmpFile,
        firstBufferedOffset     = Offset(100),
        uncommittedOffset       = Offset(100),
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 1L,
      ),
    )

    val result = writer.write(makeMessageDetail(topicPartition, Offset(101)))

    result.isLeft shouldBe true
    result.left.value shouldBe a[FatalCloudSinkError]
    result.left.value.rollBack() shouldBe true
    result.left.value.topicPartitions() should contain(topicPartition)
  }

  test(
    "REPRO: Writer.write with non-IOException from FormatWriter must remain NonFatalCloudSinkError (no regression for poison records)",
  ) {
    val schemaError = new RuntimeException("incompatible schema: field 'id' missing")
    val badFW       = mock[FormatWriter]
    when(badFW.write(any[MessageDetail])).thenReturn(Left(schemaError))
    when(badFW.getPointer).thenReturn(0L)
    when(badFW.rolloverFileOnSchemaChange()).thenReturn(false)

    val tmpFile = File.createTempFile("writer-nonio-repro-", ".tmp")
    tmpFile.deleteOnExit()

    val pendingOps   = mock[PendingOperationsProcessors]
    val localIndexer = mock[IndexManager]
    when(localIndexer.indexingEnabled).thenReturn(false)

    val writer = new Writer[FileMetadata](
      topicPartition,
      commitPolicy,
      localIndexer,
      () => Right(tmpFile),
      objectKeyBuilder,
      _ => Right(badFW),
      schemaChangeDetector,
      pendingOps,
    )
    writer.forceWriteState(
      Writing(
        CommitState(topicPartition, Some(Offset(99))),
        badFW,
        tmpFile,
        firstBufferedOffset     = Offset(100),
        uncommittedOffset       = Offset(100),
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 1L,
      ),
    )

    val result = writer.write(makeMessageDetail(topicPartition, Offset(101)))

    // Non-IO failure must remain NonFatal so NOOP/RETRY poison-record handling is not regressed.
    result.isLeft shouldBe true
    result.left.value shouldBe a[NonFatalCloudSinkError]
    result.left.value.rollBack() shouldBe false
  }

  test(
    "REPRO: Writer.commit when formatWriter.complete() wraps an IOException must return FatalCloudSinkError",
  ) {
    val ioException   = new IOException("No space left on device")
    val completeErrFW = mock[FormatWriter]
    when(completeErrFW.complete()).thenReturn(
      Left(NonFatalCloudSinkError("disk full on flush", Some(ioException))),
    )
    when(completeErrFW.getPointer).thenReturn(42L)
    when(completeErrFW.rolloverFileOnSchemaChange()).thenReturn(false)

    val tmpFile = File.createTempFile("writer-commit-ioexception-repro-", ".tmp")
    tmpFile.deleteOnExit()

    val pendingOps   = mock[PendingOperationsProcessors]
    val localIndexer = mock[IndexManager]
    when(localIndexer.indexingEnabled).thenReturn(false)

    val writer = new Writer[FileMetadata](
      topicPartition,
      commitPolicy,
      localIndexer,
      () => Right(tmpFile),
      objectKeyBuilder,
      _ => Right(completeErrFW),
      schemaChangeDetector,
      pendingOps,
    )
    writer.forceWriteState(
      Writing(
        CommitState(topicPartition, Some(Offset(99))),
        completeErrFW,
        tmpFile,
        firstBufferedOffset     = Offset(100),
        uncommittedOffset       = Offset(105),
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 2L,
      ),
    )

    val result = writer.commit

    result.isLeft shouldBe true
    result.left.value shouldBe a[FatalCloudSinkError]
    result.left.value.rollBack() shouldBe true
    result.left.value.topicPartitions() should contain(topicPartition)
  }

  /**
   * Time-flush with no records buffered (writer still in NoWriter state).
   *
   * When a commit policy triggers a time-based flush before any records have arrived for
   * this writer, the writer is still in `NoWriter` state (the `write()` call creates the
   * `Writing` state on first use). `Writer.commit` detects `NoWriter` and returns
   * `Right(())` immediately — no upload, no PendingOperationsProcessors call, no error.
   *
   * This documents that Writer.commit is a safe no-op for empty/time-flushed writers.
   */
  test(
    "time-flush with no records (NoWriter state) — commit is a no-op, returns Right(()) without calling processPendingOperations",
  ) {
    val pendingOps   = mock[PendingOperationsProcessors]
    val localIndexer = mock[IndexManager]
    when(localIndexer.indexingEnabled).thenReturn(false)

    val writer = new Writer[FileMetadata](
      topicPartition,
      commitPolicy,
      localIndexer,
      stagingFilenameFn,
      objectKeyBuilder,
      formatWriterFn,
      schemaChangeDetector,
      pendingOps,
    )

    // Writer is in its initial NoWriter state — no records delivered.
    writer.isIdle shouldBe true

    val result = writer.commit

    // No-op: Right(()) returned immediately without calling processPendingOperations.
    result.value shouldBe (())
    writer.isIdle shouldBe true
    verify(pendingOps, never).processPendingOperations(
      any[TopicPartition],
      any[Option[Offset]],
      any[PendingState],
      any[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]],
      any[Boolean],
      any[Option[String]],
      any[Option[File]],
    )
  }
}
