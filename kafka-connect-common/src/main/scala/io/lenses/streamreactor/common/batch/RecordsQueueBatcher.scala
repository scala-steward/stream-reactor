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
package io.lenses.streamreactor.common.batch

import cats.data.NonEmptySeq
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext
import io.lenses.streamreactor.connect.cloud.common.sink.commit.ConditionCommitResult

import scala.collection.immutable.Queue
import scala.collection.mutable

object RecordsQueueBatcher extends LazyLogging {

  /**
   * Iterates through the queue until the records trigger a commit based on the commit policy.
   *
   * The policy is evaluated against a single reusable [[CommitContext]] whose `count`/`fileSize`
   * are updated in place, so evaluating a batch of N records allocates nothing on the hot path
   * (no per-record commit-context copy, no per-record offset-map update). The conditions read only
   * `count`/`fileSize`/`lastModified` (never `committedOffsets`), so the per-partition max offsets
   * are merged once, at the end, via `OffsetMergeUtils.createCommitContextForEvaluation` over the
   * accepted batch. This is output-identical to threading the merged context through every record,
   * because per-partition max offset merging is associative and commutative.
   *
   * @param batchPolicy The batch policy.
   * @param initialContext The initial commit context.
   * @param records The queue of records to be processed.
   * @return A `NonEmptyBatchInfo` with the batch and updated commit context when the policy
   *         triggers, otherwise an `EmptyBatchInfo`.
   */
  def takeBatch[B <: BatchRecord](
    batchPolicy:    BatchPolicy,
    initialContext: HttpCommitContext,
    records:        Queue[B],
  ): BatchInfo = {

    val batch     = mutable.Buffer[B]()
    val queueSize = records.size

    // Reused across the loop; local to this call, so no synchronisation is required.
    val evalContext = new MutableCommitContext(initialContext.createdTimestamp, initialContext.lastFlushedTimestamp)
    var count       = 0L
    var fileSize    = 0L

    var greedyTriggerReached = false
    var triggerReached       = false

    val iterator = records.iterator
    var continue = true
    while (continue && iterator.hasNext) {
      val record = iterator.next()

      evalContext.count    = count + 1L
      evalContext.fileSize = fileSize + record.length.toLong

      val addToBatch = batchPolicy.shouldBatch(evalContext)
      triggerReached       = addToBatch.triggerReached
      greedyTriggerReached = addToBatch.greedyTriggerReached
      logger.debug(
        s"Trigger Reached: $triggerReached, Greedy trigger Reached: $greedyTriggerReached, Fits in batch: ${addToBatch.fitsInBatch}",
      )

      if (addToBatch.fitsInBatch) {
        batch.addOne(record)
        count += 1L
        fileSize += record.length.toLong
      }

      continue = !triggerReached
    }

    if (triggerReached || greedyTriggerReached) {
      NonEmptySeq.fromSeq(batch.toSeq)
        .map(value =>
          NonEmptyBatchInfo(value,
                            OffsetMergeUtils.createCommitContextForEvaluation(value.toSeq, initialContext),
                            queueSize,
          ),
        )
        .getOrElse(EmptyBatchInfo(queueSize))
    } else {
      EmptyBatchInfo(queueSize)
    }
  }

  /**
   * A minimal mutable [[CommitContext]] used only to feed `count`/`fileSize`/`lastModified` into
   * the batch policy. `committedOffsets` and `generateLogLine` are never consulted by
   * `BatchPolicy.shouldBatch`, so they are intentionally not modelled here.
   */
  private final class MutableCommitContext(
    val createdTimestamp:     Long,
    val lastFlushedTimestamp: Option[Long],
  ) extends CommitContext {
    var count:    Long = 0L
    var fileSize: Long = 0L

    override def generateLogLine(flushing: Boolean, result: Seq[ConditionCommitResult]): String = ""
  }

}
