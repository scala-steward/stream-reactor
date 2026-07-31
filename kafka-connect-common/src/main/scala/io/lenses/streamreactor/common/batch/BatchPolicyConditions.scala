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
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext

import java.time.Clock
import java.time.Duration
import java.time.Instant

trait BatchPolicyCondition {

  /**
   * Evaluates the condition against the commit context. This is on the per-record hot path, so it
   * must not allocate log strings or `Instant`s: it returns only the decision flags.
   */
  def eval(context: CommitContext): BatchResult

  /**
   * Builds the human-readable log fragment for this condition. Called only when a flush is logged
   * (at most once per flush), so it is free to allocate strings and `Instant`s.
   */
  def explain(context: CommitContext): String
}

case class Count(minCount: Long) extends BatchPolicyCondition {
  override def eval(commitContext: CommitContext): BatchResult = {
    val continueWhen = commitContext.count <= minCount
    val triggerWhen  = commitContext.count >= minCount
    BatchResult(continueWhen, triggerWhen, false)
  }

  override def explain(commitContext: CommitContext): String = {
    val flushing = if (commitContext.count >= minCount) "*" else ""
    s"count$flushing: '${commitContext.count}/$minCount'"
  }
}

case class FileSize(minFileSize: Long) extends BatchPolicyCondition with LazyLogging {
  override def eval(context: CommitContext): BatchResult = {
    val continueWhen = context.fileSize <= minFileSize
    val triggerWhen  = context.fileSize >= minFileSize
    BatchResult(continueWhen, triggerWhen, false)
  }

  override def explain(context: CommitContext): String = {
    val flushing = if (context.fileSize >= minFileSize) "*" else ""
    s"fileSize$flushing: '${context.fileSize}/$minFileSize'"
  }
}

case class Interval(interval: Duration, clock: Clock) extends BatchPolicyCondition with LazyLogging {

  private val intervalMillis: Long = interval.toMillis

  override def eval(context: CommitContext): BatchResult = {
    // An elapsed interval is a flush *trigger*, never a capacity limit: a record's arrival can never
    // be "too late to fit", so `fitsInBatch` is always true and only the greedy trigger reflects the
    // elapsed deadline. Reporting `fitsInBatch = false` here would make an interval-only policy
    // reject every record (see `BatchPolicy.shouldBatch`), collapsing time-based batching into one
    // request per record.
    // Compare epoch millis to avoid allocating three `Instant`s per record on the hot path; the
    // pretty (Instant-based) log string is built only in `explain`, when a flush is logged.
    val nowMillis       = clock.millis()
    val nextFlushMillis = context.lastModified + intervalMillis
    val triggerWhen     = nowMillis >= nextFlushMillis
    BatchResult(fitsInBatch = true, triggerReached = false, greedyTriggerReached = triggerWhen)
  }

  override def explain(context: CommitContext): String = {
    val nowMillis        = clock.millis()
    val nextFlushMillis  = context.lastModified + intervalMillis
    val flushing         = if (nowMillis >= nextFlushMillis) "*" else ""
    val lastWriteInstant = Instant.ofEpochMilli(context.lastModified)
    val nextFlushTime    = Instant.ofEpochMilli(nextFlushMillis)
    val nowInstant       = Instant.ofEpochMilli(nowMillis)
    val timeRemaining    = nextFlushTime.getEpochSecond - nowInstant.getEpochSecond
    s"interval$flushing: {frequency:${interval.toSeconds}s, in:${timeRemaining}s, lastFlush:${lastWriteInstant.toString.substring(0,
                                                                                                                                  19,
    )}, nextFlush:${nextFlushTime.toString.substring(0, 19)}}"
  }
}
