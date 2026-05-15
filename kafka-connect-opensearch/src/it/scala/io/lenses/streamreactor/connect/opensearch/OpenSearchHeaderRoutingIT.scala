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
package io.lenses.streamreactor.connect.opensearch

import com.fasterxml.jackson.databind.JsonNode
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.sink.SinkRecord
import org.opensearch.client.opensearch.core.GetRequest

import scala.util.Try

/**
 * Integration tests for `_header.index_name` dynamic index routing combined with `PK _key`.
 *
 * These tests mirror the core behaviour of the following production configuration:
 *
 * {{{
 *   connect.elastic.kcql: "UPSERT INTO _header.index_name SELECT * FROM elastic-dr-topic PK _key;
 *                           UPSERT INTO _header.index_name SELECT * FROM elastic-dr-topic2 PK _key"
 * }}}
 *
 * SSL, basic-auth, and secret-provider concerns (`secret.value://`, `secret.ref://`) are out of
 * scope here — they are covered by [[OpenSearchBasicAuthIT]] / [[OpenSearchPkiAuthIT]] and are
 * resolved by the Kafka Connect runtime before config reaches the connector.
 */
class OpenSearchHeaderRoutingIT extends ITBase {

  private val host      = "localhost"
  private lazy val port = container.hostNetwork.httpHostAddress.split(":").last.toInt

  private val msgSchema: Schema = SchemaBuilder.struct()
    .field("msg", Schema.STRING_SCHEMA)
    .build()

  private def makeWriter(props: Map[String, String]): JsonBulkWriter =
    OpenSearchWriter(OpenSearchConfig(props))

  /**
   * Build a [[SinkRecord]] whose target index is supplied via the `index_name` Connect header.
   * The Kafka message key is a plain string and will be used as the document `_id` when `PK _key`
   * is specified in the KCQL statement.
   */
  private def recordWithIndexHeader(
    topic:       String,
    offset:      Int,
    key:         String,
    targetIndex: String,
    msgValue:    String,
  ): SinkRecord = {
    val headers = new ConnectHeaders()
      .add("index_name", new SchemaAndValue(Schema.STRING_SCHEMA, targetIndex))
    val struct = new Struct(msgSchema).put("msg", msgValue)
    new SinkRecord(topic, 0, Schema.STRING_SCHEMA, key, msgSchema, struct, offset.toLong, null, null, headers)
  }

  // ---- Two topics → two indices ----

  test(
    "header routing: records from two topics are upserted into header-specified indices with record key as _id",
  ) {
    val indexA = "dr-header-index-a"
    val indexB = "dr-header-index-b"
    val topic1 = "elastic-dr-topic"
    val topic2 = "elastic-dr-topic2"

    val kcql =
      s"UPSERT INTO _header.index_name SELECT * FROM $topic1 PK _key; " +
        s"UPSERT INTO _header.index_name SELECT * FROM $topic2 PK _key"

    val writer = makeWriter(baseProps(host, port, kcql))
    try {
      val r1 = recordWithIndexHeader(topic1, 0, "key-t1-1", indexA, "from-topic1")
      val r2 = recordWithIndexHeader(topic2, 0, "key-t2-1", indexB, "from-topic2")

      writer.write(Vector(r1, r2))

      awaitDocumentCount(indexA, 1L)
      awaitDocumentCount(indexB, 1L)

      // Verify document _id matches the Kafka record key (PK _key)
      val docA = client.get(
        (g: GetRequest.Builder) => g.index(indexA).id("key-t1-1"),
        classOf[JsonNode],
      )
      docA.found() shouldBe true
      docA.source().get("msg").asText() shouldBe "from-topic1"

      val docB = client.get(
        (g: GetRequest.Builder) => g.index(indexB).id("key-t2-1"),
        classOf[JsonNode],
      )
      docB.found() shouldBe true
      docB.source().get("msg").asText() shouldBe "from-topic2"
    } finally {
      Try(writer.close())
    }
  }

  // ---- Same topic, per-record header picks different indices ----

  test(
    "header routing: two records from the same topic fan out to separate indices determined per-record by index_name header",
  ) {
    val idx1  = "dr-fan-index-1"
    val idx2  = "dr-fan-index-2"
    val topic = "elastic-dr-topic"

    val kcql = s"UPSERT INTO _header.index_name SELECT * FROM $topic PK _key"

    val writer = makeWriter(baseProps(host, port, kcql))
    try {
      val r1 = recordWithIndexHeader(topic, 0, "fan-key-1", idx1, "record-one")
      val r2 = recordWithIndexHeader(topic, 1, "fan-key-2", idx2, "record-two")

      writer.write(Vector(r1, r2))

      awaitDocumentCount(idx1, 1L)
      awaitDocumentCount(idx2, 1L)

      val doc1 = client.get(
        (g: GetRequest.Builder) => g.index(idx1).id("fan-key-1"),
        classOf[JsonNode],
      )
      doc1.found() shouldBe true
      doc1.source().get("msg").asText() shouldBe "record-one"

      val doc2 = client.get(
        (g: GetRequest.Builder) => g.index(idx2).id("fan-key-2"),
        classOf[JsonNode],
      )
      doc2.found() shouldBe true
      doc2.source().get("msg").asText() shouldBe "record-two"
    } finally {
      Try(writer.close())
    }
  }

  // ---- UPSERT idempotency via header-routed index ----

  test(
    "header routing: upserting the same key twice into a header-specified index produces exactly one document with the latest value",
  ) {
    val index = "dr-header-upsert-idem"
    val topic = "elastic-dr-topic"
    val kcql  = s"UPSERT INTO _header.index_name SELECT * FROM $topic PK _key"

    val writer = makeWriter(baseProps(host, port, kcql))
    try {
      val v1 = recordWithIndexHeader(topic, 0, "idem-key", index, "version-one")
      val v2 = recordWithIndexHeader(topic, 1, "idem-key", index, "version-two")

      writer.write(Vector(v1))
      writer.write(Vector(v2))

      awaitDocumentCount(index, 1L)

      val doc = client.get(
        (g: GetRequest.Builder) => g.index(index).id("idem-key"),
        classOf[JsonNode],
      )
      doc.found() shouldBe true
      doc.source().get("msg").asText() shouldBe "version-two"
    } finally {
      Try(writer.close())
    }
  }
}
