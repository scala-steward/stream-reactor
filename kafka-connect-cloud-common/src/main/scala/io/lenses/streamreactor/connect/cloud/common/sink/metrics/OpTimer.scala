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

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAccumulator
import java.util.concurrent.atomic.LongAdder

/**
 * Lock-free timer accumulator that tracks count, sum, max, min and last
 * elapsed-millisecond observations.  All [[record]] calls are thread-safe.
 *
 * Max and min return 0 when no observations have been made, making them safe
 * to expose as JMX attributes without special-casing on the consumer side.
 */
class OpTimer {

  private val _count = new LongAdder()
  private val _sum   = new LongAdder()
  private val _max   = new LongAccumulator(Math.max, Long.MinValue)
  private val _min   = new LongAccumulator(Math.min, Long.MaxValue)
  private val _last  = new AtomicLong(0L)

  /** Records one elapsed-millisecond observation. */
  def record(elapsedMillis: Long): Unit = {
    _count.increment()
    _sum.add(elapsedMillis)
    _max.accumulate(elapsedMillis)
    _min.accumulate(elapsedMillis)
    _last.set(elapsedMillis)
  }

  def count:      Long = _count.sum()
  def sumMillis:  Long = _sum.sum()
  def maxMillis:  Long = if (_count.sum() == 0L) 0L else _max.get()
  def minMillis:  Long = if (_count.sum() == 0L) 0L else _min.get()
  def lastMillis: Long = _last.get()
}
