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
import cats.effect.IO
import cats.effect.Ref
import cats.effect.std.Queue
import cats.effect.std.Semaphore
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import cats.implicits.toFoldableOps
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.batch.BatchPolicy
import io.lenses.streamreactor.common.batch.HttpCommitContext
import io.lenses.streamreactor.common.batch.OffsetMergeUtils
import io.lenses.streamreactor.common.utils.CyclopsToScalaOption.convertToCyclopsOption
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.http.sink.client.HttpRequestSender
import io.lenses.streamreactor.connect.http.sink.client.HttpResponseFailure
import io.lenses.streamreactor.connect.http.sink.client.HttpResponseSuccess
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpFailureConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpSuccessConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
import io.lenses.streamreactor.connect.reporting.ReportingController
import io.lenses.streamreactor.connect.reporting.model.ConnectorSpecificRecordData
import io.lenses.streamreactor.connect.reporting.model.ReportingRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.errors.RetriableException

import scala.concurrent.duration.DurationLong
import scala.concurrent.duration.FiniteDuration

object HttpWriter {

  /**
   * Allocates the writer's internal state and constructs a production writer.
   *
   * The semaphore has exactly `maxQueueSize` permits, matching the chunk cap in `enqueueGroups`.
   * Keeping those values paired guarantees that a chunk never requests more permits than can
   * exist. Direct construction remains available for tests and benchmarks that inject or observe
   * the internal queue and references.
   */
  def create(
    sinkName:             String,
    sender:               HttpRequestSender,
    template:             TemplateType,
    batchPolicy:          BatchPolicy,
    maxQueueSize:         Int,
    maxQueueOfferTimeout: FiniteDuration,
    errorThreshold:       Int,
    tidyJson:             Boolean,
    errorReporter:        ReportingController[HttpFailureConnectorSpecificRecordData],
    successReporter:      ReportingController[HttpSuccessConnectorSpecificRecordData],
  ): IO[HttpWriter] =
    for {
      recordsQueue     <- Queue.unbounded[IO, NonEmptySeq[RenderedRecord]]
      permits          <- Semaphore[IO](maxQueueSize.toLong)
      commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default(sinkName))
      offsetMapRef     <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
    } yield new HttpWriter(
      sinkName             = sinkName,
      sender               = sender,
      template             = template,
      batchPolicy          = batchPolicy,
      recordsQueue         = recordsQueue,
      permits              = permits,
      maxQueueSize         = maxQueueSize,
      offsetMapRef         = offsetMapRef,
      maxQueueOfferTimeout = maxQueueOfferTimeout,
      errorThreshold       = errorThreshold,
      tidyJson             = tidyJson,
      errorReporter        = errorReporter,
      successReporter      = successReporter,
      commitContextRef     = commitContextRef,
    )
}

/**
 * The `HttpWriter` owns a single topic's ingest-to-send pipeline.
 *
 * Records enter via [[add]] (called on the Kafka Connect `put` thread), which hands whole chunks of
 * records to a concurrent [[cats.effect.std.Queue]]. Backpressure is enforced by a
 * [[cats.effect.std.Semaphore]] whose permits count individual records (capped at `maxQueueSize`):
 * a producer acquires one permit per record before enqueuing, blocking until capacity frees up or
 * `maxQueueOfferTimeout` elapses, at which point a `RetriableException` is raised (so Kafka
 * Connect's retry configuration takes over). Passing records as chunks keeps the number of queue
 * operations and fiber hand-offs proportional to the number of `put` calls, not the number of
 * records.
 *
 * A single long-lived consumer fiber (started by `HttpWriterManager`) drives [[consume]], which
 * takes a chunk, releases its permits, feeds the records into a mutable [[BatchAccumulator]] and
 * flushes a batch whenever the configured [[BatchPolicy]] triggers (by count, size, or a
 * time-based interval deadline).
 *
 * Production code should prefer [[HttpWriter.create]]. Direct construction is retained for tests
 * and benchmarks that need to inject or observe the writer's internal state.
 */
class HttpWriter(
  sinkName:             String,
  sender:               HttpRequestSender,
  template:             TemplateType,
  batchPolicy:          BatchPolicy,
  recordsQueue:         Queue[IO, NonEmptySeq[RenderedRecord]],
  permits:              Semaphore[IO],
  maxQueueSize:         Int,
  offsetMapRef:         Ref[IO, Map[TopicPartition, Offset]],
  maxQueueOfferTimeout: FiniteDuration,
  errorThreshold:       Int,
  tidyJson:             Boolean,
  errorReporter:        ReportingController[HttpFailureConnectorSpecificRecordData],
  successReporter:      ReportingController[HttpSuccessConnectorSpecificRecordData],
  commitContextRef:     Ref[IO, HttpCommitContext],
) extends LazyLogging {

  // TODO: feedback to kafka a warning if the queue gets too large

  /**
   * Enqueues records for asynchronous processing. Records whose offset was already accepted are
   * discarded (deduplication). The remainder are enqueued as chunks under the record-counting
   * backpressure semaphore; if capacity does not free up within `maxQueueOfferTimeout` a
   * `RetriableException` is raised.
   */
  def add(newRecords: NonEmptySeq[RenderedRecord]): IO[Unit] =
    offsetMapRef.get.flatMap { offsetMap =>
      enqueueGroups(filterDuplicates(newRecords.toSeq.toList, offsetMap))
    }

  private def filterDuplicates(
    records:   List[RenderedRecord],
    offsetMap: Map[TopicPartition, Offset],
  ): List[RenderedRecord] =
    records.filter { record =>
      val tp = record.topicPartitionOffset.toTopicPartition
      offsetMap.get(tp) match {
        case Some(lastOffset) if record.topicPartitionOffset.offset.value <= lastOffset.value => false
        case _                                                                                => true
      }
    }

  // Enqueue the records as chunks. Each chunk is capped at `maxQueueSize` records so a single
  // oversized `put` cannot request more permits than exist (which would deadlock).
  private def enqueueGroups(records: List[RenderedRecord]): IO[Unit] =
    if (records.isEmpty) IO.unit
    else
      records.grouped(maxQueueSize).toList.traverse_ { group =>
        val chunk = NonEmptySeq.fromSeqUnsafe(group)
        // Keep the acquired permits and the enqueued chunk in lockstep: once permits are acquired,
        // the offer (and its offset bookkeeping) must not be cancelled, otherwise those permits
        // would be lost with no chunk for the consumer to release them against.
        acquirePermits(group.size) *>
          IO.uncancelable(_ => recordsQueue.offer(chunk) *> recordThePartitionOffsets(group))
      }

  // Fast path: try to reserve all permits without blocking. Only when capacity is exhausted do we
  // block on `acquireN` under a single shared timeout budget (one timer per put call, not per
  // record). `acquireN` releases any partially held permits if cancelled by the timeout.
  private def acquirePermits(n: Int): IO[Unit] =
    permits.tryAcquireN(n.toLong).flatMap {
      case true => IO.unit
      case false =>
        permits.acquireN(n.toLong).timeoutTo(
          maxQueueOfferTimeout,
          IO.raiseError(new RetriableException("Enqueue timed out and records remain")),
        )
    }

  // Advances the per-partition max offset for a whole set of accepted records in a single atomic
  // update, rather than one `Ref` update per record.
  private def recordThePartitionOffsets(records: List[RenderedRecord]): IO[Unit] =
    if (records.isEmpty) IO.unit
    else
      offsetMapRef.update { offsetMap =>
        records.foldLeft(offsetMap) { (acc, record) =>
          val tp     = record.topicPartitionOffset.toTopicPartition
          val offset = record.topicPartitionOffset.offset
          val updatedOffset: Offset = acc.get(tp) match {
            case Some(existingOffset) if existingOffset.value >= offset.value => existingOffset
            case _                                                            => offset
          }
          acc.updated(tp, updatedOffset)
        }
      }

  /**
   * The consumer loop. Runs until cancelled. Takes chunks off the queue, folds their records into a
   * mutable [[BatchAccumulator]] and flushes according to the batch policy.
   */
  def consume(): IO[Unit] =
    commitContextRef.get.flatMap(ctx => loop(new BatchAccumulator(batchPolicy, ctx)))

  private def loop(acc: BatchAccumulator): IO[Unit] =
    nextChunk(acc).flatMap {
      case Some(chunk) =>
        permits.releaseN(chunk.length.toLong) *> processChunk(acc, chunk) *> loop(acc)
      case None =>
        flushIfNonEmpty(acc) *> loop(acc)
    }

  /**
   * Waits for the next chunk. When the batch is empty (or the policy has no interval condition) it
   * blocks indefinitely on the queue. Otherwise it races the next chunk against the interval-based
   * flush deadline, returning `None` when the deadline fires first.
   */
  private def nextChunk(acc: BatchAccumulator): IO[Option[NonEmptySeq[RenderedRecord]]] =
    if (acc.isEmpty) recordsQueue.take.map(_.some)
    else acc.nextFlushDeadlineMillis match {
      case None => recordsQueue.take.map(_.some)
      case Some(deadline) =>
        IO.realTime.flatMap { now =>
          val waitMillis = math.max(0L, deadline - now.toMillis)
          IO.race(recordsQueue.take, IO.sleep(waitMillis.millis)).map {
            case Left(chunk) => chunk.some
            case Right(_)    => none
          }
        }
    }

  /**
   * Folds a chunk into the accumulator. Records are offered synchronously inside `IO.defer` and an
   * `IO` is materialised only when a flush is actually required, so the common "fits, no flush"
   * record incurs no per-record `IO` allocation. The iterator is stateful and shared across
   * `step()` recursions, so each recursion resumes where the previous flush segment stopped.
   */
  private def processChunk(acc: BatchAccumulator, chunk: NonEmptySeq[RenderedRecord]): IO[Unit] = {
    val iterator = chunk.toSeq.iterator

    def step(): IO[Unit] = IO.defer {
      var flushAction: IO[Unit] = null
      while (iterator.hasNext && (flushAction eq null)) {
        val record = iterator.next()
        val result = acc.offer(record)
        if (result.fitsInBatch) {
          if (result.triggerReached || result.greedyTriggerReached) flushAction = flush(acc)
        } else {
          // record does not fit the current batch: flush what we have, then place it in a fresh batch
          flushAction = flushIfNonEmpty(acc) *> reofferAfterFlush(acc, record)
        }
      }
      if (flushAction eq null) IO.unit else flushAction *> step()
    }

    step()
  }

  private def reofferAfterFlush(acc: BatchAccumulator, record: RenderedRecord): IO[Unit] =
    IO(acc.offer(record)).flatMap { result =>
      if (result.fitsInBatch) {
        if (result.triggerReached || result.greedyTriggerReached) flush(acc) else IO.unit
      } else {
        // even an empty batch rejects it (e.g. a single record over the size limit): send it alone
        flushSingle(acc, record)
      }
    }

  private def flushIfNonEmpty(acc: BatchAccumulator): IO[Unit] =
    if (acc.isEmpty) IO.unit else flush(acc)

  private def flush(acc: BatchAccumulator): IO[Unit] =
    acc.currentBatch match {
      case None => IO.unit
      case Some(batch) =>
        sendBatch(batch).attempt.flatMap {
          case Right(_) =>
            val flushedCtx = acc.flushedContext().resetErrors
            commitContextRef.set(flushedCtx) *> IO(acc.resetTo(flushedCtx))
          case Left(error) => handleFlushError(acc, error)
        }
    }

  private def flushSingle(acc: BatchAccumulator, record: RenderedRecord): IO[Unit] =
    sendBatch(NonEmptySeq.of(record)).attempt.flatMap {
      case Right(_) =>
        val flushedCtx = acc.candidateContextFor(record).resetErrors
        commitContextRef.set(flushedCtx) *> IO(acc.resetTo(flushedCtx))
      case Left(error) => handleFlushError(acc, error)
    }

  /**
   * On a flush failure the error is recorded against the commit context. If the accumulated error
   * count has crossed `errorThreshold` the error is re-raised (propagating out of the consumer
   * fiber to the task's error callback); otherwise it is logged and the failed batch is dropped
   * without advancing committed offsets, so Kafka will redeliver those records.
   */
  private def handleFlushError(acc: BatchAccumulator, error: Throwable): IO[Unit] =
    addErrorToCommitContext(error).flatMap {
      case Some(_) =>
        IO(logger.error(s"[$sinkName] Error in HttpWriter", error)) *> IO.raiseError(error)
      case None =>
        IO(logger.error(s"[$sinkName] Error in HttpWriter but not reached threshold so ignoring", error)) *>
          commitContextRef.get.flatMap(ctx => IO(acc.resetTo(ctx)))
    }

  private def sendBatch(records: NonEmptySeq[RenderedRecord]): IO[ProcessedTemplate] =
    for {
      _          <- IO.delay(logger.debug(s"[$sinkName] flushing batch of ${records.length}"))
      processed  <- IO.fromEither(template.process(records, tidyJson))
      httpResult <- sender.sendHttpRequest(processed)
      _          <- reportResult(records, processed, httpResult)
    } yield processed

  def preCommit(
    initialOffsetAndMetaMap: Map[TopicPartition, OffsetAndMetadata],
  ): IO[Map[TopicPartition, OffsetAndMetadata]] =
    commitContextRef.get.map {
      case HttpCommitContext(_, committedOffsets, _, _, _, _, _) =>
        committedOffsets.flatMap {
          case (tp, offset) =>
            for {
              initialOffsetAndMeta <- initialOffsetAndMetaMap.get(tp)

            } yield tp -> new OffsetAndMetadata(offset.value,
                                                initialOffsetAndMeta.leaderEpoch(),
                                                initialOffsetAndMeta.metadata(),
            )
        }
      case _ => initialOffsetAndMetaMap
    }.orElse(IO(Map.empty[TopicPartition, OffsetAndMetadata]))

  private def addErrorToCommitContext(e: Throwable): IO[Option[Throwable]] =
    commitContextRef.getAndUpdate {
      commitContext => commitContext.addError(e)
    }.map(cc =>
      cc
        .errors
        .maxByOption { case (_, errSeq) => errSeq.size }
        .filter { case (_, errSeq) => errSeq.size > errorThreshold }
        .flatMap {
          case (_, errSeq) => errSeq.headOption
        },
    )

  private def reportResult(
    renderedRecords:   NonEmptySeq[RenderedRecord],
    processedTemplate: ProcessedTemplate,
    responseIo:        Either[HttpResponseFailure, HttpResponseSuccess],
  ): IO[Unit] = {
    val maxRecord = OffsetMergeUtils.maxRecord(renderedRecords.toSeq)

    def reportRecord[C <: ConnectorSpecificRecordData]: C => ReportingRecord[C] = (connectorSpecific: C) =>
      new ReportingRecord[C](
        maxRecord.topicPartitionOffset.toTopicPartition.toKafka,
        maxRecord.topicPartitionOffset.offset.value,
        maxRecord.timestamp,
        processedTemplate.endpoint,
        processedTemplate.content,
        connectorSpecific,
      )

    responseIo match {
      case Left(error) => IO(
          errorReporter.enqueue(
            reportRecord[HttpFailureConnectorSpecificRecordData](
              HttpFailureConnectorSpecificRecordData(
                convertToCyclopsOption(error.statusCode).map(_.toInt),
                convertToCyclopsOption(error.responseContent),
                error.getMessage,
              ),
            ),
          ),
        ) *> IO.raiseError(error)
      case Right(success) => IO(
          successReporter.enqueue(
            reportRecord[HttpSuccessConnectorSpecificRecordData](
              HttpSuccessConnectorSpecificRecordData(
                success.statusCode,
                convertToCyclopsOption(success.responseContent),
              ),
            ),
          ),
        )
    }
  }

}
