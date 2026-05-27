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

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.lenses.streamreactor.connect.elastic.common.bulk.DeleteOp
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.elastic.common.bulk.UpsertOp

import java.time.LocalDate

/**
 * Integration tests for writer feature parity with Elastic 7:
 *
 *  - UPSERT idempotency
 *  - Tombstone DELETE readback
 *  - Multi-KCQL fan-out (requires fix #2 — semicolon splitter)
 *  - Nested PK extraction
 *  - Date-suffix index naming (WITHINDEXSUFFIX)
 */
class OpenSearchWriterFeatureIT extends ITBase {

  private val host      = "localhost"
  private lazy val port = container.hostNetwork.httpHostAddress.split(":").last.toInt

  // ---- UPSERT idempotency ----

  test("UPSERT: same id written twice produces exactly one document with the latest body") {
    val index   = "upsert-idempotency"
    val kClient = makeKClient(baseProps(host, port, s"UPSERT INTO $index SELECT * FROM topic PK id"))

    val docV1 = JsonNodeFactory.instance.objectNode().put("id", "u1").put("val", "first")
    val docV2 = JsonNodeFactory.instance.objectNode().put("id", "u1").put("val", "second")

    val r1 = kClient.bulk(Seq(UpsertOp(index, "u1", docV1, None)))
    r1.isSuccess shouldBe true
    r1.get.errors shouldBe false

    val r2 = kClient.bulk(Seq(UpsertOp(index, "u1", docV2, None)))
    r2.isSuccess shouldBe true
    r2.get.errors shouldBe false

    // Exactly one document; it must have the second version's field value.
    // awaitDocumentCount asserts the count via search (NRT); the body assertion uses a
    // realtime GET by id, which bypasses the search index and is not subject to refresh timing.
    awaitDocumentCount(index, 1L)

    val getResp = client.get(
      (g: org.opensearch.client.opensearch.core.GetRequest.Builder) => g.index(index).id("u1"),
      classOf[com.fasterxml.jackson.databind.JsonNode],
    )
    getResp.found() shouldBe true
    getResp.source().get("val").asText() shouldBe "second"
  }

  // ---- Tombstone DELETE readback ----

  test("Tombstone: DeleteOp removes the document; awaitDocumentCount confirms 0") {
    val index   = "tombstone-delete"
    val kClient = makeKClient(baseProps(host, port, s"INSERT INTO $index SELECT * FROM topic PK id"))

    val doc = JsonNodeFactory.instance.objectNode().put("id", "d1").put("msg", "to-be-deleted")
    val ins = kClient.bulk(Seq(InsertOp(index, "d1", doc, None, None)))
    ins.isSuccess shouldBe true

    awaitDocumentCount(index, 1L)

    val del = kClient.bulk(Seq(DeleteOp(index, "d1", None)))
    del.isSuccess shouldBe true
    del.get.errors shouldBe false

    awaitDocumentCount(index, 0L)
  }

  // ---- Multi-KCQL fan-out (fix #2 dependency) ----

  test("Multi-KCQL: two semicolon-separated statements fan out to two separate indices") {
    val idx1 = "multi-kcql-a"
    val idx2 = "multi-kcql-b"
    val kcql = s"INSERT INTO $idx1 SELECT * FROM topicA;INSERT INTO $idx2 SELECT * FROM topicB"

    val kClient = makeKClient(baseProps(host, port, kcql))

    val docA = JsonNodeFactory.instance.objectNode().put("src", "topicA")
    val docB = JsonNodeFactory.instance.objectNode().put("src", "topicB")

    val rA = kClient.bulk(Seq(InsertOp(idx1, "a1", docA, None, None)))
    rA.isSuccess shouldBe true

    val rB = kClient.bulk(Seq(InsertOp(idx2, "b1", docB, None, None)))
    rB.isSuccess shouldBe true

    awaitDocumentCount(idx1, 1L)
    awaitDocumentCount(idx2, 1L)
  }

  // ---- Nested PK ----

  test("Nested PK: document _id resolves to nested field value") {
    val index   = "nested-pk"
    val kClient = makeKClient(baseProps(host, port, s"INSERT INTO $index SELECT * FROM topic PK meta.id"))

    // Build a nested JSON doc: {"meta": {"id": "nested-val"}, "payload": "hello"}
    val doc = JsonNodeFactory.instance.objectNode()
    doc.putObject("meta").put("id", "nested-val")
    doc.put("payload", "hello")

    // The PK path "meta.id" produces id = "nested-val"
    val result = kClient.bulk(Seq(InsertOp(index, "nested-val", doc, None, None)))
    result.isSuccess shouldBe true
    result.get.errors shouldBe false

    awaitDocumentCount(index, 1L)

    val getResp = client.get(
      (g: org.opensearch.client.opensearch.core.GetRequest.Builder) => g.index(index).id("nested-val"),
      classOf[com.fasterxml.jackson.databind.JsonNode],
    )
    getResp.found() shouldBe true
    getResp.source().get("payload").asText() shouldBe "hello"
  }

  // ---- Date-suffix index naming ----

  test("Date-suffix: WITHINDEXSUFFIX appends today's date to the index name") {
    val baseIndex = "suffix-base"
    val today     = LocalDate.now().toString // "YYYY-MM-dd"
    val expected  = s"${baseIndex}_$today"

    val kClient = makeKClient(baseProps(
      host,
      port,
      s"INSERT INTO $baseIndex SELECT * FROM topic WITHINDEXSUFFIX=_{YYYY-MM-dd}",
    ))

    val doc    = JsonNodeFactory.instance.objectNode().put("msg", "dated")
    val result = kClient.bulk(Seq(InsertOp(expected, "s1", doc, None, None)))
    result.isSuccess shouldBe true
    result.get.errors shouldBe false

    awaitDocumentCount(expected, 1L)
  }
}
