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
package io.lenses.streamreactor.connect.benchmarks.http

import cats.data.NonEmptySeq
import cats.effect.IO
import cats.effect.Ref
import cats.effect.Resource
import cats.effect.unsafe.IORuntime
import io.lenses.streamreactor.common.batch.HttpBatchPolicy
import io.lenses.streamreactor.common.batch.HttpCommitContext
import io.lenses.streamreactor.connect.benchmarks.BenchResult
import io.lenses.streamreactor.connect.benchmarks.HeapSampler
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.http.sink.HttpWriter
import io.lenses.streamreactor.connect.http.sink.RecordsQueue
import io.lenses.streamreactor.connect.http.sink.client.NoAuthenticationHttpRequestSender
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig
import io.lenses.streamreactor.connect.http.sink.metrics.HttpSinkMetrics
import io.lenses.streamreactor.connect.http.sink.tpl.RawTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
import org.apache.kafka.connect.sink.SinkRecord
import org.http4s.Response
import org.http4s.Status
import org.http4s.client.Client

import scala.collection.immutable.Queue
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration

/**
 * Drives the HTTP sink's production render -> queue -> batch -> send pipeline for a single
 * (single-partition) topic, with Kafka and the network removed:
 *
 *  - "Kafka removed": records are handed directly to the pipeline as `SinkRecord`s, exactly as
 *    `HttpSinkTask.put()` would receive them from Kafka Connect; no consumer/broker is involved.
 *  - "Network removed": the `org.http4s.client.Client[IO]` at the bottom of the stack is a stub
 *    that returns `200 OK` (optionally after a configurable delay), so no bytes ever leave the
 *    process. Everything above that seam -- template rendering, the per-topic `RecordsQueue`,
 *    batching policy evaluation, request/body assembly, and the real `HttpSinkMetrics` -- is
 *    unmodified production code.
 *
 * Design note on the drain loop: production drives `HttpWriter.process()` from a fixed-rate
 * scheduler (`connect.http.upload.sync.period`, default 100ms) inside `HttpWriterManager`. That
 * scheduler is a polling artifact, not part of the pipeline's real cost, so it is not reused here.
 * This harness instead calls `HttpWriter.process()` back-to-back until the queue is drained,
 * which measures the pipeline's unconstrained throughput ceiling rather than a scheduler-limited
 * approximation of it (see the plan's "pure CPU ceiling" sweep).
 */
class HttpSinkThroughputHarness(implicit runtime: IORuntime) {

  private val sinkName = "bench-http"

  /**
   * @param props           HTTP sink connector properties (same keys/semantics as production,
   *                         e.g. `connect.http.batch.count`), minus `connect.http.endpoint`/
   *                         `connect.http.request.content`/`connect.http.method` which are
   *                         fixed by this harness.
   * @param records         input records; `records.length` MUST be an exact multiple of the
   *                         effective batch count so the final partial batch is not left
   *                         stranded in the queue (see [[HttpSinkThroughputHarness]] scaladoc).
   * @param egressLatency   simulated network/server latency added to every stubbed HTTP response.
   * @param requestContent  the `connect.http.request.content` template; defaults to the
   *                         customer's own template.
   */
  def run(
    scenario:       String,
    props:          Map[String, String],
    records:        IndexedSeq[SinkRecord],
    egressLatency:  FiniteDuration = Duration.Zero,
    requestContent: String         = "{{#message}}{{value}}\n{{/message}}",
    pollChunkSize:  Int            = 500,
  ): BenchResult = {
    val fullProps = props ++ Map(
      "connect.http.endpoint"         -> "http://bench.invalid/mocked",
      "connect.http.request.content"  -> requestContent,
      "connect.http.method"           -> "POST",
    )

    val config = HttpSinkConfig.from(fullProps).fold(e => throw e, identity)
    val template: TemplateType = RawTemplate(config.endpoint, config.content, config.headers, config.nullPayloadHandler)
    val metrics = new HttpSinkMetrics()
    val sender  = new NoAuthenticationHttpRequestSender(sinkName, config.method.toHttp4sMethod, stubClient(egressLatency), metrics)

    val batchPolicy = {
      val configured = config.batch.toBatchPolicy
      if (configured.conditions.nonEmpty) configured else HttpBatchPolicy.Default
    }

    val (writer, queueRef) = buildWriter(sender, template, batchPolicy, config.maxQueueSize, config.maxQueueOfferTimeout).unsafeRunSync()

    val (_, heapBefore, heapAfter, elapsedNanos) = {
      val heapBefore = HeapSampler.usedHeapBytes()
      val start      = System.nanoTime()

      records.grouped(math.max(1, pollChunkSize)).foreach { chunk =>
        putChunk(writer, template, chunk).unsafeRunSync()
      }
      drainLoop(writer, queueRef).unsafeRunSync()

      val elapsed    = System.nanoTime() - start
      val heapAfter  = HeapSampler.usedHeapBytes()
      ((), heapBefore, heapAfter, elapsed)
    }

    BenchResult(
      sink               = "HTTP",
      scenario           = scenario,
      records            = records.length.toLong,
      elapsedNanos       = elapsedNanos,
      networkOps         = metrics.get2xxCount,
      flushes            = metrics.get2xxCount, // one HTTP request per batch: request == flush
      heapUsedDeltaBytes = math.max(0L, heapAfter - heapBefore),
    )
  }

  private def stubClient(latency: FiniteDuration): Client[IO] =
    Client[IO] { _ =>
      val ok = IO.pure(Response[IO](Status.Ok).withEntity("OK"))
      Resource.eval(if (latency <= Duration.Zero) ok else IO.sleep(latency) *> ok)
    }

  private def buildWriter(
    sender:               NoAuthenticationHttpRequestSender,
    template:             TemplateType,
    batchPolicy:          io.lenses.streamreactor.common.batch.BatchPolicy,
    maxQueueSize:         Int,
    maxQueueOfferTimeout: FiniteDuration,
  ): IO[(HttpWriter, Ref[IO, Queue[RenderedRecord]])] =
    for {
      recordsQueueRef  <- Ref.of[IO, Queue[RenderedRecord]](Queue.empty)
      commitContextRef <- Ref.of[IO, HttpCommitContext](HttpCommitContext.default(sinkName))
      offsetsRef       <- Ref.of[IO, Map[TopicPartition, Offset]](Map.empty)
      queue = new RecordsQueue(recordsQueueRef, commitContextRef, batchPolicy, maxQueueSize, maxQueueOfferTimeout, offsetsRef)
      writer = new HttpWriter(
        sinkName        = sinkName,
        sender          = sender,
        template        = template,
        recordsQueue    = queue,
        errorThreshold  = Int.MaxValue,
        tidyJson        = false,
        errorReporter   = NoopReporters.error,
        successReporter = NoopReporters.success,
        commitContextRef = commitContextRef,
      )
    } yield (writer, recordsQueueRef)

  private def putChunk(writer: HttpWriter, template: TemplateType, chunk: IndexedSeq[SinkRecord]): IO[Unit] =
    NonEmptySeq.fromSeq(chunk.toList) match {
      case None => IO.unit
      case Some(nes) =>
        template.renderRecords(nes) match {
          case Left(err)        => IO.raiseError(err)
          case Right(rendered)  => writer.add(rendered)
        }
    }

  private def drainLoop(writer: HttpWriter, queueRef: Ref[IO, Queue[RenderedRecord]]): IO[Unit] =
    queueRef.get.flatMap { q =>
      if (q.isEmpty) IO.unit
      else writer.process() *> drainLoop(writer, queueRef)
    }
}
