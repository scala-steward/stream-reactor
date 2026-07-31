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

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import scala.collection.immutable.Queue
import scala.jdk.CollectionConverters._

class RecordsQueueBatcherLoggingTest extends AnyFlatSpec with Matchers {

  // `takeBatch` is synchronous, so a fixed clock fully controls the `Interval` condition -- no real
  // wall-clock time is consulted anywhere in these tests.
  private val elapsedClock: Clock = Clock.fixed(Instant.ofEpochMilli(3_600_000L), ZoneOffset.UTC)
  private val tp = Topic("topic").withPartition(0)

  private case class TestRecord(topicPartitionOffset: TopicPartitionOffset, length: Int) extends BatchRecord

  private def testRecord(offset: Long): TestRecord = TestRecord(tp.atOffset(offset), 10)

  // A private, self-managed logback context so tests capture flush log lines without touching the
  // global SLF4J logger factory. Going through `LoggerFactory` and casting to a logback logger is
  // unsafe here: during concurrent SLF4J initialisation (as happens when the whole module's test
  // suite runs) it can hand back a `SubstituteLogger`, which is not castable. Creating the logback
  // logger directly from an isolated `LoggerContext` avoids that race entirely.
  private val loggerContext: LoggerContext = new LoggerContext()

  // A scala-logging Logger backed by a fresh logback logger with an attached ListAppender, so a test
  // can assert on exactly which flush log lines the batch policy emitted. Each call uses a unique
  // logger name so appenders never bleed across tests.
  private def capturingLogger(): (com.typesafe.scalalogging.Logger, ListAppender[ILoggingEvent]) = {
    val logbackLogger = loggerContext.getLogger(s"batch-policy-capture-${java.util.UUID.randomUUID()}")
    logbackLogger.setLevel(Level.INFO)
    val appender = new ListAppender[ILoggingEvent]()
    appender.setContext(loggerContext)
    appender.start()
    logbackLogger.addAppender(appender)
    (com.typesafe.scalalogging.Logger(logbackLogger), appender)
  }

  private def flushLines(appender: ListAppender[ILoggingEvent]): Seq[String] =
    appender.list.asScala.toSeq.map(_.getFormattedMessage).filter(_.startsWith("Flushing for"))

  "takeBatch" should "log exactly one flush line for a greedy-only (interval) batch, with the accepted count" in {
    val (batchLogger, appender) = capturingLogger()
    // Count is far above the record count so it never triggers; only the elapsed interval fires, as a
    // greedy trigger. Before this change such a batch was produced but never logged.
    val policy = BatchPolicy(batchLogger, Count(1000), Interval(Duration.ofSeconds(1), elapsedClock))
    // Anchored at the epoch with the clock an hour ahead, so the 1s interval reads as elapsed.
    val initialContext = HttpCommitContext.default("sink").copy(
      createdTimestamp     = 0L,
      lastFlushedTimestamp = None,
    )
    val records = Queue(testRecord(1), testRecord(2), testRecord(3))

    val result = RecordsQueueBatcher.takeBatch(policy, initialContext, records)

    result shouldBe a[NonEmptyBatchInfo[_]]
    val lines = flushLines(appender)
    lines should have size 1
    // The count reflects the batch actually produced (3), not a candidate; the interval is the
    // condition that triggered (marked with *).
    lines.head should include("count: '3/1000'")
    lines.head should include("interval*")
  }

  it should "log exactly one flush line for a hard count trigger, describing the accepted batch" in {
    val (batchLogger, appender) = capturingLogger()
    val policy                  = BatchPolicy(batchLogger, Count(2))
    val initialContext          = HttpCommitContext.default("sink")
    val records                 = Queue(testRecord(1), testRecord(2), testRecord(3))

    val result = RecordsQueueBatcher.takeBatch(policy, initialContext, records)

    result shouldBe a[NonEmptyBatchInfo[_]]
    val lines = flushLines(appender)
    lines should have size 1
    lines.head should include("count*: '2/2'")
  }
}
