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

import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File

class WriteStateRecordCountTest extends AnyFunSuite with Matchers with MockitoSugar {

  private val topicPartition = Topic("test-topic").withPartition(0)
  private val startOffset    = Offset(10L)
  private val endOffset      = Offset(15L)

  test("Writing.toUploading preserves recordCount from CommitState before reset") {
    val formatWriter = mock[FormatWriter]
    when(formatWriter.getPointer).thenReturn(100L)

    val commitState = CommitState(topicPartition.atOffset(10L).toTopicPartition, None)

    val writing = Writing(
      commitState             = commitState,
      formatWriter            = formatWriter,
      file                    = new File("test"),
      firstBufferedOffset     = startOffset,
      uncommittedOffset       = endOffset,
      earliestRecordTimestamp = 1000L,
      latestRecordTimestamp   = 2000L,
    )

    // Simulate 3 records written
    val writingWith3Records = writing
      .update(Offset(12L), 1100L, None)
      .asInstanceOf[Writing]
      .update(Offset(13L), 1200L, None)
      .asInstanceOf[Writing]
      .update(Offset(14L), 1300L, None)
      .asInstanceOf[Writing]

    writingWith3Records.getCommitState.recordCount shouldBe 3L

    val uploading = writingWith3Records.toUploading

    uploading.recordCount shouldBe 3L
    uploading.getCommitState.recordCount shouldBe 0L // reset happened in commitState
  }

  test("Uploading.toNoWriter does not use recordCount (state transition is independent)") {
    val commitState = CommitState(topicPartition.atOffset(10L).toTopicPartition, None)
    val uploading = Uploading(
      commitState             = commitState,
      file                    = new File("test"),
      firstBufferedOffset     = startOffset,
      uncommittedOffset       = endOffset,
      earliestRecordTimestamp = 1000L,
      latestRecordTimestamp   = 2000L,
      recordCount             = 42L,
    )

    val noWriter = uploading.toNoWriter(Some(endOffset))
    noWriter.getCommitState.committedOffset shouldBe Some(endOffset)
  }

  test("Writing.toUploading with zero records propagates zero count") {
    val formatWriter = mock[FormatWriter]
    when(formatWriter.getPointer).thenReturn(0L)

    val commitState = CommitState(topicPartition.atOffset(10L).toTopicPartition, None)
    val writing = Writing(
      commitState             = commitState,
      formatWriter            = formatWriter,
      file                    = new File("test"),
      firstBufferedOffset     = startOffset,
      uncommittedOffset       = startOffset,
      earliestRecordTimestamp = 0L,
      latestRecordTimestamp   = 0L,
    )

    writing.toUploading.recordCount shouldBe 0L
  }
}
