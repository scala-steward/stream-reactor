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
package io.lenses.streamreactor.connect.benchmarks

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord

/**
 * Produces identical synthetic `SinkRecord`s for both the HTTP sink and the GCS sink harnesses,
 * so that the two throughput measurements are driven from exactly the same input.
 *
 * Records carry a plain JSON string value (`Schema.STRING_SCHEMA`), which both sinks accept:
 *  - The HTTP sink's `{{value}}` template substitution passes the string straight through.
 *  - The GCS sink's `ValueToSinkDataConverter` turns a `String` into `StringSinkData`, which the
 *    JSON format writer serialises via the standard Kafka `JsonConverter` — the same per-record
 *    JSON-encoding cost a real deployment pays, even though the resulting bytes are a quoted/escaped
 *    JSON string rather than a bare JSON object. This is irrelevant for a throughput comparison
 *    (we are not asserting on output content), but is called out here and in the README.
 */
object RecordGenerator {

  /**
   * @param topic        Kafka topic name to stamp on every record.
   * @param count        number of records to generate.
   * @param startOffset  offset of the first record; subsequent records increment by 1.
   * @param partition    Kafka partition to stamp on every record.
   * @param payloadBytes approximate size in bytes of the generated JSON value (padded with an
   *                     opaque filler field so the record body reaches roughly this size).
   */
  def sinkRecords(
    topic:        String,
    count:        Int,
    startOffset:  Long = 0L,
    partition:    Int  = 0,
    payloadBytes: Int  = 128,
  ): IndexedSeq[SinkRecord] = {
    val basePadding = math.max(0, payloadBytes - baseRecordOverheadBytes)
    val padding     = "x" * basePadding
    (0 until count).map { i =>
      val offset = startOffset + i
      val json   = jsonValue(offset, padding)
      new SinkRecord(topic, partition, null, null, Schema.STRING_SCHEMA, json, offset)
    }
  }

  // Rough fixed overhead of the JSON envelope below (field names/quotes/braces), used so
  // `payloadBytes` approximates the total record size rather than the padding size alone.
  private val baseRecordOverheadBytes = 40

  private def jsonValue(offset: Long, padding: String): String =
    s"""{"id":$offset,"name":"user-$offset","payload":"$padding"}"""
}
