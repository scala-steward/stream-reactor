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

import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Clock
import java.time.Duration

class BatchPolicyConditionsTest extends AnyFlatSpec with Matchers with MockitoSugar {

  private def countContext(currentCount: Long): CommitContext = {
    val cc = mock[CommitContext]
    when(cc.count).thenReturn(currentCount)
    cc
  }

  private def fileSizeContext(currentSize: Long): CommitContext = {
    val cc = mock[CommitContext]
    when(cc.fileSize).thenReturn(currentSize)
    cc
  }

  private def intervalContext(lastModified: Long): CommitContext = {
    val cc = mock[CommitContext]
    when(cc.lastModified).thenReturn(lastModified)
    cc
  }

  private def clockAt(nowMillis: Long): Clock = {
    val clock = mock[Clock]
    when(clock.millis()).thenReturn(nowMillis)
    clock
  }

  "Count.eval" should "not trigger below the minimum and still fit in the batch" in {
    Count(100).eval(countContext(99)) should be(BatchResult(fitsInBatch = true, triggerReached = false, false))
  }

  it should "trigger and still fit at the boundary" in {
    Count(100).eval(countContext(100)) should be(BatchResult(fitsInBatch = true, triggerReached = true, false))
  }

  it should "trigger and no longer fit above the minimum" in {
    Count(100).eval(countContext(101)) should be(BatchResult(fitsInBatch = false, triggerReached = true, false))
  }

  "Count.explain" should "render count without a flush marker below the minimum" in {
    Count(100).explain(countContext(99)) should be("count: '99/100'")
  }

  it should "render count with a flush marker at or above the minimum" in {
    Count(100).explain(countContext(100)) should be("count*: '100/100'")
    Count(100).explain(countContext(101)) should be("count*: '101/100'")
  }

  "FileSize.eval" should "not trigger below the minimum and still fit in the batch" in {
    FileSize(100).eval(fileSizeContext(99)) should be(BatchResult(fitsInBatch = true, triggerReached = false, false))
  }

  it should "trigger and still fit at the boundary" in {
    FileSize(100).eval(fileSizeContext(100)) should be(BatchResult(fitsInBatch = true, triggerReached = true, false))
  }

  it should "trigger and no longer fit above the minimum" in {
    FileSize(100).eval(fileSizeContext(101)) should be(BatchResult(fitsInBatch = false, triggerReached = true, false))
  }

  "FileSize.explain" should "render file size without a flush marker below the minimum" in {
    FileSize(100).explain(fileSizeContext(99)) should be("fileSize: '99/100'")
  }

  it should "render file size with a flush marker at or above the minimum" in {
    FileSize(100).explain(fileSizeContext(100)) should be("fileSize*: '100/100'")
  }

  "Interval.eval" should "not trigger before the interval elapses" in {
    val interval = Interval(Duration.ofMinutes(10), clockAt(0L))
    interval.eval(intervalContext(0L)) should be(
      BatchResult(fitsInBatch = true, triggerReached = false, greedyTriggerReached = false),
    )
  }

  it should "greedily trigger once the interval has elapsed" in {
    val interval = Interval(Duration.ofMinutes(10), clockAt(600000L))
    interval.eval(intervalContext(0L)) should be(
      BatchResult(fitsInBatch = true, triggerReached = false, greedyTriggerReached = true),
    )
  }

  "Interval.explain" should "render the interval without a flush marker before it elapses" in {
    val interval = Interval(Duration.ofMinutes(10), clockAt(0L))
    interval.explain(intervalContext(0L)) should be(
      "interval: {frequency:600s, in:600s, lastFlush:1970-01-01T00:00:00, nextFlush:1970-01-01T00:10:00}",
    )
  }

  it should "render the interval with a flush marker once it has elapsed" in {
    val interval = Interval(Duration.ofMinutes(10), clockAt(600000L))
    interval.explain(intervalContext(0L)) should be(
      "interval*: {frequency:600s, in:0s, lastFlush:1970-01-01T00:00:00, nextFlush:1970-01-01T00:10:00}",
    )
  }

}
