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
package io.lenses.streamreactor.connect.http.sink

import cats.data.NonEmptySeq
import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.testkit.TestControl
import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.batch.BatchPolicy
import io.lenses.streamreactor.common.batch.Count
import io.lenses.streamreactor.common.batch.FileSize
import io.lenses.streamreactor.common.batch.HttpCommitContext
import io.lenses.streamreactor.common.batch.Interval
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.http.sink.client.HttpRequestSender
import io.lenses.streamreactor.connect.http.sink.client.HttpResponseSuccess
import io.lenses.streamreactor.connect.http.sink.config.ErrorNullPayloadHandler
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpFailureConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpSuccessConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.tpl.Headers
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import io.lenses.streamreactor.connect.http.sink.tpl.SimpleTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
import io.lenses.streamreactor.connect.http.sink.tpl.substitutions.SubstitutionError
import io.lenses.streamreactor.connect.reporting.ReportingController
import org.apache.kafka.connect.errors.RetriableException
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AsyncFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

class HttpWriterTest extends AsyncIOSpec with AsyncFunSuiteLike with Matchers with MockitoSugar with LazyLogging {

  private val sinkName  = "MySinkName"
  private val timestamp = 125L

  private val topicPartition: TopicPartition = Topic("myTopic").withPartition(1)

  private val record1 = RenderedRecord(topicPartition.atOffset(100), timestamp, "record1", Seq.empty, "")
  private val record2 = RenderedRecord(topicPartition.atOffset(101), timestamp, "record2", Seq.empty, "")
  private val record3 = RenderedRecord(topicPartition.atOffset(102), timestamp, "record3", Seq.empty, "")

  private def buildWriter(
    batchPolicy:      BatchPolicy,
    maxQueueSize:     Int               = 10000,
    offerTimeout:     FiniteDuration    = 1.minute,
    sender:           HttpRequestSender = successSender(),
    template:         TemplateType      = successTemplate(),
    commitContextRef: Ref[IO, HttpCommitContext],
    offsetMapRef:     Ref[IO, Map[TopicPartition, Offset]],
  ): IO[(HttpWriter, Queue[IO, NonEmptySeq[RenderedRecord]])] =
    for {
      queue   <- Queue.unbounded[IO, NonEmptySeq[RenderedRecord]]
      permits <- Semaphore[IO](maxQueueSize.toLong)
    } yield {
      val writer = new HttpWriter(
        sinkName             = sinkName,
        sender               = sender,
        template             = template,
        batchPolicy          = batchPolicy,
        recordsQueue         = queue,
        permits              = permits,
        maxQueueSize         = maxQueueSize,
        offsetMapRef         = offsetMapRef,
        maxQueueOfferTimeout = offerTimeout,
        errorThreshold       = 5,
        tidyJson             = false,
        errorReporter        = mock[ReportingController[HttpFailureConnectorSpecificRecordData]],
        successReporter      = mock[ReportingController[HttpSuccessConnectorSpecificRecordData]],
        commitContextRef     = commitContextRef,
      )
      (writer, queue)
    }

  private def successSender(): HttpRequestSender = {
    val sender = mock[HttpRequestSender]
    when(sender.sendHttpRequest(any[ProcessedTemplate])).thenReturn(IO(HttpResponseSuccess(200, "OK".some).asRight))
    sender
  }

  // A sender that signals when a request starts and then never completes, so a batch stays in-flight
  // (and its permits stay held) for the duration of the test.
  private def blockingSender(entered: Deferred[IO, Unit]): HttpRequestSender = {
    val sender = mock[HttpRequestSender]
    when(sender.sendHttpRequest(any[ProcessedTemplate])).thenReturn(entered.complete(()) *> IO.never)
    sender
  }

  // A real (pure) template avoids brittle mocking of the abstract process method.
  private def successTemplate(): TemplateType =
    SimpleTemplate("http://bench.invalid",
                   "content",
                   Headers(Seq.empty, copyMessageHeaders = false),
                   ErrorNullPayloadHandler,
    )

  // A template that records the exact records handed to `process` for each flush, so a test can
  // assert on how the chunk was split into batches. `SimpleTemplate.process` only reads `records.head`,
  // which would hide the batch composition, so this captures the whole batch instead.
  private def recordingTemplate(captured: ConcurrentLinkedQueue[Seq[RenderedRecord]]): TemplateType =
    new TemplateType {
      override def endpoint: String  = "http://bench.invalid"
      override def headers:  Headers = Headers(Seq.empty, copyMessageHeaders = false)

      override def renderRecords(
        records: NonEmptySeq[SinkRecord],
      ): Either[SubstitutionError, NonEmptySeq[RenderedRecord]] =
        Left(SubstitutionError("renderRecords is not exercised by this test"))

      override def process(
        records:  NonEmptySeq[RenderedRecord],
        tidyJson: Boolean,
      ): Either[SubstitutionError, ProcessedTemplate] = {
        captured.add(records.toSeq)
        Right(ProcessedTemplate(records.head.endpointRendered,
                                records.head.recordRendered,
                                records.head.headersRendered,
        ))
      }
    }

  private def eventually[A](io: IO[A])(cond: A => Boolean): IO[A] =
    io.flatMap(a => if (cond(a)) IO.pure(a) else IO.sleep(10.millis) *> eventually(io)(cond)).timeout(5.seconds)

  // A private, self-managed logback context so tests capture flush log lines without touching the
  // global SLF4J logger factory. Going through `LoggerFactory` and casting to a logback logger is
  // unsafe here: during concurrent SLF4J initialisation (as happens when the whole module's test
  // suite runs) it can hand back a `SubstituteLogger`, which is not castable. Creating the logback
  // logger directly from an isolated `LoggerContext` avoids that race entirely.
  private val loggerContext: LoggerContext = new LoggerContext()

  // Builds a scala-logging Logger backed by a fresh logback logger with an attached ListAppender, so
  // a test can assert on exactly which flush log lines the batch policy emitted. Each call uses a
  // unique logger name so appenders never bleed across tests.
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

  test("add enqueues records and skips offsets that were already accepted") {
    for {
      commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default(sinkName))
      offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map(topicPartition -> Offset(101)))
      writerAndQueue <-
        buildWriter(BatchPolicy(logger, Count(1000)), commitContextRef = commitContextRef, offsetMapRef = offsetMapRef)
      (writer, queue) = writerAndQueue
      // record1 (100) and record2 (101) are <= the last accepted offset (101) so are discarded;
      // only record3 (102) is enqueued (as a single chunk).
      _         <- writer.add(NonEmptySeq.of(record1, record2, record3))
      queueSize <- queue.size
      chunk     <- queue.take
      offsetMap <- offsetMapRef.get
    } yield {
      queueSize shouldBe 1
      chunk.toSeq shouldBe Seq(record3)
      offsetMap shouldBe Map(topicPartition -> Offset(102))
    }
  }

  test("consumer flushes a batch and advances committed offsets when the count policy triggers") {
    val sender   = successSender()
    val template = successTemplate()
    for {
      commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default(sinkName))
      offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
      writerAndQueue <- buildWriter(
        BatchPolicy(logger, Count(2)),
        sender           = sender,
        template         = template,
        commitContextRef = commitContextRef,
        offsetMapRef     = offsetMapRef,
      )
      (writer, _) = writerAndQueue
      fiber      <- writer.consume().start
      _          <- writer.add(NonEmptySeq.of(record1, record2))
      ctx        <- eventually(commitContextRef.get)(_.committedOffsets.nonEmpty)
      _          <- fiber.cancel
    } yield {
      verify(sender).sendHttpRequest(any[ProcessedTemplate])
      ctx.committedOffsets should contain(topicPartition -> Offset(101))
      ctx.count shouldBe 0L
    }
  }

  // The offer timeout elapses under `TestControl` virtual time, so the test neither sleeps for real
  // nor depends on the wall clock to decide when the timeout fires.
  test("add raises a RetriableException when the queue stays full past the offer timeout") {
    TestControl.executeEmbed(
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default(sinkName))
        offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
        writerAndQueue <- buildWriter(
          BatchPolicy(logger, Count(1000)),
          maxQueueSize     = 1,
          offerTimeout     = 200.millis,
          commitContextRef = commitContextRef,
          offsetMapRef     = offsetMapRef,
        )
        (writer, _) = writerAndQueue
        // no consumer is running, so the second record cannot be enqueued (capacity 1)
        result <- writer.add(NonEmptySeq.of(record1, record2)).attempt
      } yield result match {
        case Left(e)  => e shouldBe a[RetriableException]
        case Right(_) => fail("expected a RetriableException")
      },
    )
  }

  test("backpressure covers in-flight records: add times out while a batch is still being sent") {
    TestControl.executeEmbed(
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default(sinkName))
        offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
        entered          <- Deferred[IO, Unit]
        writerAndQueue <- buildWriter(
          BatchPolicy(logger, Count(2)),
          maxQueueSize     = 2,
          offerTimeout     = 200.millis,
          sender           = blockingSender(entered),
          commitContextRef = commitContextRef,
          offsetMapRef     = offsetMapRef,
        )
        (writer, _) = writerAndQueue
        fiber      <- writer.consume().start
        // record1+record2 fill the Count(2) batch, which is now stuck in the (never-completing) send
        // holding both permits.
        _ <- writer.add(NonEmptySeq.of(record1, record2))
        _ <- entered.get
        // No permits remain (they are released only when the batch leaves the pipeline), so a further
        // record cannot be admitted and the offer times out.
        result <- writer.add(NonEmptySeq.of(record3)).attempt
        _      <- fiber.cancel
      } yield result match {
        case Left(e)  => e shouldBe a[RetriableException]
        case Right(_) => fail("expected a RetriableException")
      },
    )
  }

  test("resetAcceptedOffsets clears the dedup high-water mark so redelivered records are re-enqueued") {
    for {
      commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default(sinkName))
      offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
      writerAndQueue <-
        buildWriter(BatchPolicy(logger, Count(1000)), commitContextRef = commitContextRef, offsetMapRef = offsetMapRef)
      (writer, queue) = writerAndQueue
      _              <- writer.add(NonEmptySeq.of(record1, record2, record3))
      _              <- queue.take
      // Without a reset, re-adding the same records would be discarded as duplicates.
      _              <- writer.resetAcceptedOffsets(Set(topicPartition))
      _              <- writer.add(NonEmptySeq.of(record1, record2, record3))
      queueSizeAfter <- queue.size
      chunk          <- queue.take
    } yield {
      queueSizeAfter shouldBe 1
      chunk.toSeq shouldBe Seq(record1, record2, record3)
    }
  }

  test("preCommit reports the offsets committed by a flush") {
    for {
      commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default(sinkName))
      offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
      writerAndQueue <-
        buildWriter(BatchPolicy(logger, Count(2)), commitContextRef = commitContextRef, offsetMapRef = offsetMapRef)
      (writer, _) = writerAndQueue
      fiber      <- writer.consume().start
      _          <- writer.add(NonEmptySeq.of(record1, record2))
      _          <- eventually(commitContextRef.get)(_.committedOffsets.nonEmpty)
      _          <- fiber.cancel
      initial     = Map(topicPartition -> new org.apache.kafka.clients.consumer.OffsetAndMetadata(0L))
      offsets    <- writer.preCommit(initial)
    } yield offsets.get(topicPartition).map(_.offset()) shouldBe Some(101L)
  }

  // Time is fully controlled in these tests: the `Interval` condition reads a fixed `java.time.Clock`
  // (never the real wall clock), and the consumer loop's `IO.realTime`/`IO.sleep` deadline runs under
  // `TestControl` virtual time. `createdTimestamp` is expressed on the same virtual timeline (epoch
  // millis from 0), so the interval-elapsed decision and the deadline stay consistent and
  // deterministic without any `System.currentTimeMillis` or real sleeping.
  private val utcZone: ZoneId = ZoneOffset.UTC
  private def fixedClockAt(millis: Long): Clock = Clock.fixed(Instant.ofEpochMilli(millis), utcZone)

  // A clock reading far past any interval used below, so `Interval` evaluates as elapsed (greedy).
  private val elapsedClock: Clock = fixedClockAt(3_600_000L)
  // A clock frozen at the epoch, so an interval anchored at `createdTimestamp = 0` is not yet elapsed.
  private val notElapsedClock: Clock = fixedClockAt(0L)

  private def contextAt(createdMillis: Long): HttpCommitContext =
    HttpCommitContext.default(sinkName).copy(createdTimestamp = createdMillis, lastFlushedTimestamp = None)

  // The interval sets only the greedy trigger (never a hard trigger), so an elapsed interval must not
  // cut a chunk short: the whole chunk should be packed into a single batch, not flushed one record at
  // a time.
  test("an elapsed interval packs the whole chunk into a single batch") {
    val captured = new ConcurrentLinkedQueue[Seq[RenderedRecord]]()
    TestControl.executeEmbed(
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](contextAt(0L))
        offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
        writerAndQueue <- buildWriter(
          BatchPolicy(logger, Count(1000), Interval(Duration.ofSeconds(1), elapsedClock)),
          template         = recordingTemplate(captured),
          commitContextRef = commitContextRef,
          offsetMapRef     = offsetMapRef,
        )
        (writer, _) = writerAndQueue
        fiber      <- writer.consume().start
        _          <- writer.add(NonEmptySeq.of(record1, record2, record3))
        _          <- eventually(commitContextRef.get)(_.committedOffsets.nonEmpty)
        _          <- fiber.cancel
      } yield captured.asScala.toList.map(_.toList) shouldBe List(List(record1, record2, record3)),
    )
  }

  // Direct regression guard for the time-only batching bug: with ONLY an (already-elapsed) interval
  // configured -- no Count/FileSize -- the whole chunk must still pack into a single request. Before
  // the fix, an elapsed interval reported `fitsInBatch=false` and, being the sole (hence first)
  // condition, made the policy reject every record, so each record was sent alone via `flushSingle`.
  test("an interval-only elapsed policy packs the whole chunk into a single request") {
    val captured = new ConcurrentLinkedQueue[Seq[RenderedRecord]]()
    TestControl.executeEmbed(
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](contextAt(0L))
        offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
        writerAndQueue <- buildWriter(
          BatchPolicy(logger, Interval(Duration.ofSeconds(1), elapsedClock)),
          template         = recordingTemplate(captured),
          commitContextRef = commitContextRef,
          offsetMapRef     = offsetMapRef,
        )
        (writer, _) = writerAndQueue
        fiber      <- writer.consume().start
        _          <- writer.add(NonEmptySeq.of(record1, record2, record3))
        _          <- eventually(commitContextRef.get)(_.committedOffsets.nonEmpty)
        _          <- fiber.cancel
      } yield captured.asScala.toList.map(_.toList) shouldBe List(List(record1, record2, record3)),
    )
  }

  // A file-size limit must cut the batch before it overshoots. Each record body is 7 bytes, so with
  // `FileSize(17)` the third record's candidate size is 21 > 17: `FileSize` reports
  // `fitsInBatch = false, triggerReached = true`, so the AND fold in `shouldBatch` rejects it, the
  // first two records (14 bytes) flush as one batch and the third starts a fresh one. `Count(1000)`
  // is declared first and reports `fitsInBatch = true`; the old first-condition-wins fold would have
  // taken that and appended the third record, shipping 21 bytes past the 17-byte limit. `FileSize`'s
  // rejection must not be masked, so the byte ceiling is asserted directly below.
  test("a file size limit cuts the batch before it overshoots even when count is declared first") {
    val byteLimit = 17L
    val captured  = new ConcurrentLinkedQueue[Seq[RenderedRecord]]()
    TestControl.executeEmbed(
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](contextAt(0L))
        offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
        writerAndQueue <- buildWriter(
          BatchPolicy(logger, Count(1000), FileSize(byteLimit)),
          template         = recordingTemplate(captured),
          commitContextRef = commitContextRef,
          offsetMapRef     = offsetMapRef,
        )
        (writer, _) = writerAndQueue
        fiber      <- writer.consume().start
        _          <- writer.add(NonEmptySeq.of(record1, record2, record3))
        // The first batch (record1 + record2 = 14 bytes) triggers and commits offset 101; record3
        // (which would take the batch to 21 bytes) starts a fresh batch and stays unflushed.
        _ <- eventually(commitContextRef.get)(_.committedOffsets.get(topicPartition).exists(_.value == 101L))
        _ <- fiber.cancel
      } yield {
        val batches = captured.asScala.toList.map(_.toList)
        batches shouldBe List(List(record1, record2))
        // No batch may exceed the configured byte limit -- states the property directly rather than
        // relying only on the exact composition above.
        batches.foreach(batch => batch.map(_.recordRendered.length.toLong).sum should be <= byteLimit)
      },
    )
  }

  // Coverage for the pure time-based path with no count/size condition: records accumulate while the
  // interval has not yet elapsed and are flushed as a single batch when the deadline fires under
  // virtual time. (This would also pass before the fix -- the discriminating interval case is
  // "an interval-only elapsed policy packs the whole chunk into a single request" above; this one
  // guards the not-yet-elapsed deadline path for an interval-only policy.)
  test("an interval-only not-yet-elapsed policy flushes all accumulated records once the deadline fires") {
    val captured = new ConcurrentLinkedQueue[Seq[RenderedRecord]]()
    TestControl.executeEmbed(
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](contextAt(0L))
        offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
        writerAndQueue <- buildWriter(
          BatchPolicy(logger, Interval(Duration.ofMillis(500), notElapsedClock)),
          template         = recordingTemplate(captured),
          commitContextRef = commitContextRef,
          offsetMapRef     = offsetMapRef,
        )
        (writer, _) = writerAndQueue
        fiber      <- writer.consume().start
        _          <- writer.add(NonEmptySeq.of(record1, record2, record3))
        _          <- eventually(commitContextRef.get)(_.committedOffsets.nonEmpty)
        _          <- fiber.cancel
      } yield captured.asScala.toList.map(_.toList) shouldBe List(List(record1, record2, record3)),
    )
  }

  // When an interval is already elapsed and more chunks are sitting in the queue, they should be
  // drained and packed into the same batch rather than each producing its own flush.
  test("an elapsed interval drains already-queued chunks into a single batch") {
    val captured = new ConcurrentLinkedQueue[Seq[RenderedRecord]]()
    TestControl.executeEmbed(
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](contextAt(0L))
        offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
        writerAndQueue <- buildWriter(
          BatchPolicy(logger, Count(1000), Interval(Duration.ofSeconds(1), elapsedClock)),
          template         = recordingTemplate(captured),
          commitContextRef = commitContextRef,
          offsetMapRef     = offsetMapRef,
        )
        (writer, _) = writerAndQueue
        // Two separate chunks are enqueued before the consumer starts, so the drain has something to pull.
        _     <- writer.add(NonEmptySeq.of(record1, record2))
        _     <- writer.add(NonEmptySeq.of(record3))
        fiber <- writer.consume().start
        _     <- eventually(commitContextRef.get)(_.committedOffsets.nonEmpty)
        _     <- fiber.cancel
      } yield captured.asScala.toList.map(_.toList) shouldBe List(List(record1, record2, record3)),
    )
  }

  // Regression guard: a hard count trigger must still cut the batch at the configured count, even when
  // the interval has elapsed (so both greedy and count would fire). The first two records flush as one
  // batch; the flush re-stamps `lastFlushedTimestamp`, so the interval is no longer elapsed for the
  // third record and it is not stranded into its own greedy flush.
  test("a hard count trigger still cuts the batch at the configured count when the interval has elapsed") {
    val captured = new ConcurrentLinkedQueue[Seq[RenderedRecord]]()
    TestControl.executeEmbed(
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](contextAt(0L))
        offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
        writerAndQueue <- buildWriter(
          BatchPolicy(logger, Count(2), Interval(Duration.ofSeconds(1), elapsedClock)),
          template         = recordingTemplate(captured),
          commitContextRef = commitContextRef,
          offsetMapRef     = offsetMapRef,
        )
        (writer, _) = writerAndQueue
        fiber      <- writer.consume().start
        _          <- writer.add(NonEmptySeq.of(record1, record2, record3))
        _          <- eventually(commitContextRef.get)(_.committedOffsets.nonEmpty)
        _          <- fiber.cancel
      } yield captured.asScala.toList.map(_.toList) shouldBe List(List(record1, record2)),
    )
  }

  // Regression guard: with no elapsed interval and no count/size trigger, records simply accumulate and
  // nothing is flushed. A one-hour interval anchored at the virtual epoch stays comfortably un-elapsed
  // for the (virtual) duration of the test.
  test("records accumulate without flushing while the interval has not elapsed") {
    val captured = new ConcurrentLinkedQueue[Seq[RenderedRecord]]()
    TestControl.executeEmbed(
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](contextAt(0L))
        offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
        writerAndQueue <- buildWriter(
          BatchPolicy(logger, Count(1000), Interval(Duration.ofHours(1), notElapsedClock)),
          template         = recordingTemplate(captured),
          commitContextRef = commitContextRef,
          offsetMapRef     = offsetMapRef,
        )
        (writer, _) = writerAndQueue
        fiber      <- writer.consume().start
        _          <- writer.add(NonEmptySeq.of(record1, record2, record3))
        _          <- IO.sleep(300.millis)
        ctx        <- commitContextRef.get
        _          <- fiber.cancel
      } yield {
        captured.asScala.toList shouldBe empty
        ctx.committedOffsets shouldBe empty
      },
    )
  }

  // A time-based flush driven by the interval deadline (records accumulate while the interval is not
  // yet elapsed, then the deadline fires with no further records) must still emit the once-per-flush
  // explanation -- exactly once. The deadline (`createdTimestamp = 0` + 500ms) fires under virtual
  // time; the wall clock is frozen before that instant, so the interval condition is present in the
  // line but not marked `*` (that marker would require the wall clock to advance to the deadline,
  // which it does in production where both clocks are the real wall clock).
  test("a time-based deadline flush emits exactly one flush log line") {
    val (batchLogger, appender) = capturingLogger()
    TestControl.executeEmbed(
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](contextAt(0L))
        offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
        writerAndQueue <- buildWriter(
          BatchPolicy(batchLogger, Count(1000), Interval(Duration.ofMillis(500), notElapsedClock)),
          commitContextRef = commitContextRef,
          offsetMapRef     = offsetMapRef,
        )
        (writer, _) = writerAndQueue
        fiber      <- writer.consume().start
        // Below the count trigger, so nothing flushes until the 500ms interval deadline fires.
        _ <- writer.add(NonEmptySeq.of(record1, record2))
        _ <- eventually(commitContextRef.get)(_.committedOffsets.nonEmpty)
        _ <- fiber.cancel
      } yield {
        val lines = flushLines(appender)
        lines should have size 1
        lines.head should include("interval")
        lines.head should include("count: '2/1000'")
      },
    )
  }

  // A greedy (already-elapsed interval) flush is decided outside `shouldBatch`, but must still emit
  // the once-per-flush explanation -- exactly once for the drained batch.
  test("a greedy interval flush emits exactly one flush log line") {
    val (batchLogger, appender) = capturingLogger()
    TestControl.executeEmbed(
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](contextAt(0L))
        offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
        writerAndQueue <- buildWriter(
          BatchPolicy(batchLogger, Count(1000), Interval(Duration.ofSeconds(1), elapsedClock)),
          commitContextRef = commitContextRef,
          offsetMapRef     = offsetMapRef,
        )
        (writer, _) = writerAndQueue
        fiber      <- writer.consume().start
        _          <- writer.add(NonEmptySeq.of(record1, record2, record3))
        _          <- eventually(commitContextRef.get)(_.committedOffsets.nonEmpty)
        _          <- fiber.cancel
      } yield {
        val lines = flushLines(appender)
        lines should have size 1
        lines.head should include("interval*")
      },
    )
  }

  // A hard count trigger still logs exactly one line, describing the batch actually sent.
  test("a hard count trigger emits exactly one flush log line") {
    val (batchLogger, appender) = capturingLogger()
    TestControl.executeEmbed(
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](contextAt(0L))
        offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
        writerAndQueue <- buildWriter(
          BatchPolicy(batchLogger, Count(2)),
          commitContextRef = commitContextRef,
          offsetMapRef     = offsetMapRef,
        )
        (writer, _) = writerAndQueue
        fiber      <- writer.consume().start
        _          <- writer.add(NonEmptySeq.of(record1, record2))
        _          <- eventually(commitContextRef.get)(_.committedOffsets.nonEmpty)
        _          <- fiber.cancel
      } yield {
        val lines = flushLines(appender)
        lines should have size 1
        lines.head should include("count*: '2/2'")
      },
    )
  }

  // Off-by-one guard: with Count(2) and three records, the first flush sends two records, so the log
  // line must report the batch actually sent ('2/2'), not the rejected candidate that triggered it.
  test("the flush log line counts the batch actually sent, not the candidate") {
    val (batchLogger, appender) = capturingLogger()
    TestControl.executeEmbed(
      for {
        commitContextRef <- Ref.of[IO, HttpCommitContext](contextAt(0L))
        offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
        writerAndQueue <- buildWriter(
          BatchPolicy(batchLogger, Count(2)),
          commitContextRef = commitContextRef,
          offsetMapRef     = offsetMapRef,
        )
        (writer, _) = writerAndQueue
        fiber      <- writer.consume().start
        _          <- writer.add(NonEmptySeq.of(record1, record2, record3))
        _          <- eventually(commitContextRef.get)(_.committedOffsets.nonEmpty)
        _          <- fiber.cancel
      } yield {
        val lines = flushLines(appender)
        lines.head should include("count*: '2/2'")
        lines.head should not include "'3/2'"
      },
    )
  }

}
