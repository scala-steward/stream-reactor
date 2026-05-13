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
package io.lenses.streamreactor.connect.cloud.common.perf

import io.lenses.streamreactor.connect.cloud.common.formats.writer.JsonFormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.GZIP
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.location.FileUtils.toBufferedOutputStream
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.StringSinkData
import io.lenses.streamreactor.connect.cloud.common.stream.BuildLocalOutputStream
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.time.Instant

/**
 * Lightweight regression guardrail for the staging-write hot path.
 *
 * This is NOT a JMH benchmark — it is a wall-time sanity check that runs in CI
 * alongside unit tests. Thresholds are set conservatively so that the test
 * virtually never produces a false positive on healthy hardware, while still
 * catching catastrophic regressions (e.g. accidentally re-introducing the
 * per-write bytes.slice allocation or per-record flush).
 *
 * The suite is intentionally light (≤ a few seconds total) and does not need
 * to be excluded from the default CI run.
 */
class StagingWritePerfTest extends AnyFlatSpec with Matchers {

  private val tp      = Topic("perf-topic").withPartition(0)
  private val payload = """{"name":"alice","title":"eng","salary":123.45}""".getBytes("UTF-8")

  // -----------------------------------------------------------------------
  // BuildLocalOutputStream — write(bytes, off, len) with non-zero offset
  // -----------------------------------------------------------------------

  "BuildLocalOutputStream.write(bytes, off, len)" should
    "process 100k writes with non-zero offset within 2 seconds" in {
      val N    = 100_000
      val file = Files.createTempFile("perf-blos-", ".bin").toFile
      try {
        val blos = new BuildLocalOutputStream(toBufferedOutputStream(file), tp)

        val padded = Array.fill[Byte](4)(0) ++ payload // 4-byte prefix → non-zero start offset
        val start  = System.nanoTime()

        (0 until N).foreach(_ => blos.write(padded, 4, payload.length))
        blos.complete()

        val elapsedMs = (System.nanoTime() - start) / 1_000_000L
        info(s"BuildLocalOutputStream: $N writes in ${elapsedMs}ms (${elapsedMs * 1000 / N} µs/write)")

        blos.getPointer should be(N.toLong * payload.length)
        elapsedMs should be < 2000L
      } finally {
        file.delete()
        ()
      }
    }

  // -----------------------------------------------------------------------
  // JsonFormatWriter — uncompressed
  // -----------------------------------------------------------------------

  "JsonFormatWriter (uncompressed)" should "process 50k records within 5 seconds" in {
    val N    = 50_000
    val file = Files.createTempFile("perf-json-", ".json").toFile
    try {
      implicit val codec: CompressionCodec = UNCOMPRESSED.toCodec()
      val blos   = new BuildLocalOutputStream(toBufferedOutputStream(file), tp)
      val writer = new JsonFormatWriter(blos)

      val start = System.nanoTime()
      (0 until N).foreach { i =>
        writer.write(jsonMessage(i)).isRight should be(true)
      }
      writer.complete()

      val elapsedMs = (System.nanoTime() - start) / 1_000_000L
      info(s"JsonFormatWriter (uncompressed): $N records in ${elapsedMs}ms (${elapsedMs * 1000 / N} µs/rec)")

      elapsedMs should be < 5000L
    } finally {
      file.delete()
      ()
    }
  }

  // -----------------------------------------------------------------------
  // JsonFormatWriter — GZIP: verify file is smaller than uncompressed and
  // that the gzip path completes in a reasonable time.
  // -----------------------------------------------------------------------

  "JsonFormatWriter (gzip)" should
    "produce a smaller output file than uncompressed and complete 10k records within 5 seconds" in {
      val N                = 10_000
      val fileGzip         = Files.createTempFile("perf-json-gzip-", ".json.gz").toFile
      val fileUncompressed = Files.createTempFile("perf-json-plain-", ".json").toFile
      try {
        val gzipSize: Long = {
          implicit val codec: CompressionCodec = GZIP.toCodec()
          val blos   = new BuildLocalOutputStream(toBufferedOutputStream(fileGzip), tp)
          val writer = new JsonFormatWriter(blos)
          val start  = System.nanoTime()
          (0 until N).foreach(i => writer.write(jsonMessage(i)))
          writer.complete()
          val elapsedMs = (System.nanoTime() - start) / 1_000_000L
          info(s"JsonFormatWriter (gzip): $N records in ${elapsedMs}ms (${elapsedMs * 1000 / N} µs/rec)")
          elapsedMs should be < 5000L
          fileGzip.length()
        }

        val plainSize: Long = {
          implicit val codec: CompressionCodec = UNCOMPRESSED.toCodec()
          val blos   = new BuildLocalOutputStream(toBufferedOutputStream(fileUncompressed), tp)
          val writer = new JsonFormatWriter(blos)
          (0 until N).foreach(i => writer.write(jsonMessage(i)))
          writer.complete()
          fileUncompressed.length()
        }

        val ratio    = if (gzipSize > 0) plainSize.toDouble / gzipSize.toDouble else 0.0
        val ratioStr = "%.2f".format(ratio)
        info(s"File sizes — gzip: ${gzipSize}B, plain: ${plainSize}B (compression ratio: ${ratioStr}x)")

        gzipSize should be < plainSize
      } finally {
        fileGzip.delete()
        fileUncompressed.delete()
        ()
      }
    }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private def jsonMessage(i: Int): MessageDetail =
    MessageDetail(
      key       = NullSinkData(None),
      value     = StringSinkData(s"""{"id":$i,"name":"user-$i","value":${i * 1.5}}"""),
      headers   = Map.empty,
      timestamp = Some(Instant.now()),
      topic     = Topic("perf-topic"),
      partition = 0,
      offset    = Offset(i.toLong),
    )
}
