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
package io.lenses.streamreactor.connect.cloud.common.sink.metrics

import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors

class OpTimerTest extends AnyFunSuiteLike with Matchers {

  test("defaults to zero before any observation") {
    val t = new OpTimer()
    t.count      shouldBe 0L
    t.sumMillis  shouldBe 0L
    t.maxMillis  shouldBe 0L
    t.minMillis  shouldBe 0L
    t.lastMillis shouldBe 0L
  }

  test("single observation is reflected in all fields") {
    val t = new OpTimer()
    t.record(42L)
    t.count      shouldBe 1L
    t.sumMillis  shouldBe 42L
    t.maxMillis  shouldBe 42L
    t.minMillis  shouldBe 42L
    t.lastMillis shouldBe 42L
  }

  test("count accumulates across multiple records") {
    val t = new OpTimer()
    t.record(10L)
    t.record(20L)
    t.record(30L)
    t.count     shouldBe 3L
    t.sumMillis shouldBe 60L
  }

  test("max tracks the largest value seen") {
    val t = new OpTimer()
    t.record(5L)
    t.record(100L)
    t.record(3L)
    t.maxMillis shouldBe 100L
  }

  test("min tracks the smallest value seen") {
    val t = new OpTimer()
    t.record(50L)
    t.record(1L)
    t.record(200L)
    t.minMillis shouldBe 1L
  }

  test("last reflects the most recent observation") {
    val t = new OpTimer()
    t.record(10L)
    t.record(20L)
    t.record(5L)
    t.lastMillis shouldBe 5L
  }

  test("zero observation is valid and does not corrupt accumulators") {
    val t = new OpTimer()
    t.record(0L)
    t.count      shouldBe 1L
    t.sumMillis  shouldBe 0L
    t.maxMillis  shouldBe 0L
    t.minMillis  shouldBe 0L
    t.lastMillis shouldBe 0L
  }

  test("concurrent records are all counted (thread-safety)") {
    val numThreads = 8
    val recsPerThread = 1000
    val t = new OpTimer()
    val executor = Executors.newFixedThreadPool(numThreads)
    val latch = new CountDownLatch(numThreads)

    (1 to numThreads).foreach { _ =>
      executor.submit(new Runnable {
        def run(): Unit = {
          (1 to recsPerThread).foreach(_ => t.record(1L))
          latch.countDown()
        }
      })
    }
    latch.await()
    executor.shutdown()

    t.count     shouldBe (numThreads * recsPerThread).toLong
    t.sumMillis shouldBe (numThreads * recsPerThread).toLong
    t.minMillis shouldBe 1L
    t.maxMillis shouldBe 1L
  }
}
