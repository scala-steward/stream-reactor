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

import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.Logger
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext

import java.time.Clock
import java.time.Duration

/**
 * The [[BatchPolicy]] is responsible for determining when
 * a sink partition (a single file) should be flushed (closed on disk, and moved to be visible).
 *
 * By default, it flushes based on configurable conditions such as number of records, file size,
 * or time since the file was last flushed.
 *
 * @param conditions the conditions to evaluate for flushing the partition
 */
case class BatchPolicy(logger: Logger, conditions: BatchPolicyCondition*) {

  def shouldBatch(context: CommitContext): BatchResult = {
    // Hot path: fold the conditions with three flags, allocating no intermediate collections and no
    // log strings. This is a pure decision function -- the flush explanation is logged by the flush
    // sites (see `logFlush`), which know the batch actually being sent, rather than here where only
    // the candidate batch is visible.
    var triggerReached       = false
    var greedyTriggerReached = false
    var fitsInBatch          = false
    var first                = true
    conditions.foreach { condition =>
      val r = condition.eval(context)
      if (r.triggerReached) triggerReached             = true
      if (r.greedyTriggerReached) greedyTriggerReached = true
      if (first) {
        fitsInBatch = r.fitsInBatch
        first       = false
      }
    }
    BatchResult(fitsInBatch, triggerReached, greedyTriggerReached)
  }

  /**
   * Emits the once-per-flush explanation at INFO, describing the batch actually being sent. Called
   * by the flush sites rather than by [[shouldBatch]], which only ever sees the candidate batch
   * (current batch plus the record under consideration) and so cannot describe what is really
   * flushed. The scala-logging macro gates both the string build and the write on `isInfoEnabled`.
   */
  def logFlush(context: CommitContext): Unit =
    logger.info(generateLogLine(conditions.map(_.explain(context))))

  def generateLogLine(explanations: Seq[String]): String =
    s"Flushing for {${explanations.mkString(", ")}}"

}

object BatchPolicy extends LazyLogging {
  def apply(conditions: BatchPolicyCondition*): BatchPolicy =
    BatchPolicy(logger, conditions: _*)
}

case class BatchResult(
  fitsInBatch:          Boolean,
  triggerReached:       Boolean,
  greedyTriggerReached: Boolean, // room for more
)

object HttpBatchPolicy extends LazyLogging {

  private val defaultFlushSize     = 500_000_000L
  private val defaultFlushInterval = Duration.ofSeconds(3600)
  private val defaultFlushCount    = 50_000L

  val Default: BatchPolicy =
    BatchPolicy(FileSize(defaultFlushSize),
                Interval(defaultFlushInterval, Clock.systemDefaultZone()),
                Count(defaultFlushCount),
    )

}
