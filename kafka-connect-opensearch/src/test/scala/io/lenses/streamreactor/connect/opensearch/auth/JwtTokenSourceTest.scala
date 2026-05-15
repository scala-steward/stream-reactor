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
package io.lenses.streamreactor.connect.opensearch.auth

import org.apache.kafka.connect.errors.ConnectException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

class JwtTokenSourceTest extends AnyFunSuite with Matchers {

  // ---- StaticJwtTokenSource ----

  test("static token returns the configured token") {
    val source = new StaticJwtTokenSource("my-token")
    source.getToken shouldBe "my-token"
  }

  test("fromStaticToken emits a startup warning and returns a StaticJwtTokenSource") {
    val source = JwtTokenSource.fromStaticToken("warn-token")
    source.getToken shouldBe "warn-token"
  }

  // ---- FileJwtTokenSource ----

  test("file-backed token reads from file on first call") {
    val f = Files.createTempFile("jwt-token", ".txt")
    Files.write(f, "file-token".getBytes(StandardCharsets.UTF_8))
    val source = new FileJwtTokenSource(f.toString, 60000L)
    source.getToken shouldBe "file-token"
  }

  test("file-backed token trims whitespace") {
    val f = Files.createTempFile("jwt-token", ".txt")
    Files.write(f, "  trimmed-token  \n".getBytes(StandardCharsets.UTF_8))
    val source = new FileJwtTokenSource(f.toString, 60000L)
    source.getToken shouldBe "trimmed-token"
  }

  test("file-backed token throws ConnectException when file is missing") {
    intercept[ConnectException](new FileJwtTokenSource("/no/such/file.jwt", 60000L))
  }

  test("file-backed token throws ConnectException when file is empty") {
    val f = Files.createTempFile("jwt-token-empty", ".txt")
    Files.write(f, "".getBytes(StandardCharsets.UTF_8))
    val ex = intercept[ConnectException](new FileJwtTokenSource(f.toString, 60000L))
    ex.getMessage should include("empty")
  }

  test("file-backed token is refreshed after the interval elapses") {
    val f = Files.createTempFile("jwt-token-rotate", ".txt")
    Files.write(f, "token-v1".getBytes(StandardCharsets.UTF_8))

    var now: Instant = Instant.ofEpochMilli(1000L)
    val testClock = new Clock {
      override def getZone: ZoneOffset = ZoneOffset.UTC
      override def withZone(z: java.time.ZoneId): Clock = this
      override def instant(): Instant = now
    }

    val source = new FileJwtTokenSource(f.toString, 5000L, clock = testClock)
    source.getToken shouldBe "token-v1"

    // Update the file
    Files.write(f, "token-v2".getBytes(StandardCharsets.UTF_8))

    // Before the interval expires, still see v1
    now = Instant.ofEpochMilli(5000L)
    source.getToken shouldBe "token-v1"

    // After the interval expires, see v2
    now = Instant.ofEpochMilli(6001L)
    source.getToken shouldBe "token-v2"
  }

  test("file-backed token throws if file deleted after initial read") {
    val f = Files.createTempFile("jwt-token-delete", ".txt")
    Files.write(f, "initial-token".getBytes(StandardCharsets.UTF_8))

    var now: Instant = Instant.ofEpochMilli(1000L)
    val testClock = new Clock {
      override def getZone: ZoneOffset = ZoneOffset.UTC
      override def withZone(z: java.time.ZoneId): Clock = this
      override def instant(): Instant = now
    }

    val source = new FileJwtTokenSource(f.toString, 100L, clock = testClock)
    source.getToken shouldBe "initial-token"

    Files.delete(f)

    now = Instant.ofEpochMilli(5000L)
    val ex = intercept[ConnectException](source.getToken)
    ex.getMessage should include(f.toString)
  }

  // C6: Deterministic rotation test with distinct token values (simulates distinct JWT subjects)
  test("C6: file-backed rotation returns distinct token values on each rotation — clock-controlled, no Thread.sleep") {
    val f = Files.createTempFile("jwt-token-c6-", ".txt")

    val tokenSubject1 = "subject=kafka-connect-v1"
    val tokenSubject2 = "subject=kafka-connect-v2"

    Files.write(f, tokenSubject1.getBytes(StandardCharsets.UTF_8))

    var now: Instant = Instant.ofEpochMilli(0L)
    val testClock = new Clock {
      override def getZone: ZoneOffset = ZoneOffset.UTC
      override def withZone(z: java.time.ZoneId): Clock = this
      override def instant(): Instant = now
    }

    val refreshInterval = 1000L
    val source          = new FileJwtTokenSource(f.toString, refreshInterval, clock = testClock)

    // First read returns subject 1
    source.getToken shouldBe tokenSubject1

    // Write subject 2 to the file — but do NOT advance the clock yet
    Files.write(f, tokenSubject2.getBytes(StandardCharsets.UTF_8))

    // Still before interval — cache returns subject 1
    now = Instant.ofEpochMilli(refreshInterval - 1)
    source.getToken shouldBe tokenSubject1

    // Advance clock past the interval — next call re-reads and returns subject 2
    now = Instant.ofEpochMilli(refreshInterval + 1)
    source.getToken shouldBe tokenSubject2
  }

  // ---- Path traversal and base-directory guard ----

  test("raw '..' component in path is rejected before normalization") {
    val ex = intercept[ConnectException](new FileJwtTokenSource("../../etc/passwd", 60000L))
    ex.getMessage should include("path traversal detected")
  }

  test("raw '..' on Windows-style separator is also rejected") {
    val ex = intercept[ConnectException](new FileJwtTokenSource("..\\..\\etc\\passwd", 60000L))
    ex.getMessage should include("path traversal detected")
  }

  test("absolute path inside base directory is accepted") {
    val dir = Files.createTempDirectory("jwt-basedir-ok")
    val f   = Files.createTempFile(dir, "token", ".jwt")
    Files.write(f, "safe-token".getBytes(StandardCharsets.UTF_8))
    val source = new FileJwtTokenSource(f.toString, 60000L, baseDir = Some(dir.toString))
    source.getToken shouldBe "safe-token"
  }

  test("absolute path outside base directory is rejected") {
    val allowedDir = Files.createTempDirectory("jwt-basedir-allowed")
    val otherDir   = Files.createTempDirectory("jwt-basedir-other")
    val f          = Files.createTempFile(otherDir, "secret", ".txt")
    Files.write(f, "secret-contents".getBytes(StandardCharsets.UTF_8))
    val ex = intercept[ConnectException](
      new FileJwtTokenSource(f.toString, 60000L, baseDir = Some(allowedDir.toString)),
    )
    ex.getMessage should include("outside allowed base directory")
  }

  test("absolute path to sensitive file is accepted when no base directory is configured (backward-compat)") {
    // Without baseDir the connector imposes no directory restriction —
    // this is the existing admin-only trust model. The test documents it explicitly.
    val f = Files.createTempFile("jwt-compat-", ".txt")
    Files.write(f, "compat-token".getBytes(StandardCharsets.UTF_8))
    val source = new FileJwtTokenSource(f.toString, 60000L)
    source.getToken shouldBe "compat-token"
  }

  test("path with '..' component is rejected even when base directory is not set") {
    val ex = intercept[ConnectException](new FileJwtTokenSource("subdir/../../../etc/passwd", 60000L))
    ex.getMessage should include("path traversal detected")
  }

  test("second call within interval after failed rotation also raises (cache stays empty)") {
    val f = Files.createTempFile("jwt-token-second-fail", ".txt")
    Files.write(f, "good-token".getBytes(StandardCharsets.UTF_8))

    var now: Instant = Instant.ofEpochMilli(0L)
    val testClock = new Clock {
      override def getZone: ZoneOffset = ZoneOffset.UTC
      override def withZone(z: java.time.ZoneId): Clock = this
      override def instant(): Instant = now
    }

    val source = new FileJwtTokenSource(f.toString, 100L, clock = testClock)
    source.getToken shouldBe "good-token"

    Files.delete(f)

    // Advance past the refresh interval — first call should fail and invalidate the cache
    now = Instant.ofEpochMilli(5000L)
    intercept[ConnectException](source.getToken)

    // Second call within the same interval tick (lastReadAt == now so no re-read attempted),
    // but the cache was invalidated — must raise ConnectException, NOT silently return a stale token.
    intercept[ConnectException](source.getToken)
  }
}
