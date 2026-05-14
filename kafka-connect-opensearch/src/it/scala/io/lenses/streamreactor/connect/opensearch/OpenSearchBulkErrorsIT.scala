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
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

/**
 * Integration tests for bulk error handling:
 *  - Strict mode: a real `mapper_parsing_exception` surfaces as `BulkResult.errors=true`
 *  - Tolerant mode: same item error is swallowed, `BulkResult.errors=false`
 *  - AUTOCREATE path: writing to a new index when the KCQL carries AUTOCREATE succeeds
 *
 * The mapping conflict is induced by:
 *  1. Explicitly mapping `field` as `integer` on the index.
 *  2. Inserting a document whose `field` value is a String — OpenSearch returns
 *     `mapper_parsing_exception` per item, while the HTTP response is still 200.
 */
class OpenSearchBulkErrorsIT extends ITBase {

  private val host = "localhost"
  private lazy val port = container.hostNetwork.httpHostAddress.split(":").last.toInt

  private def numericDoc(value: Int) =
    JsonNodeFactory.instance.objectNode().put("field", value)

  private def stringDoc(value: String) =
    JsonNodeFactory.instance.objectNode().put("field", value)

  /** Create an index with `field` explicitly mapped as `integer`. */
  private def createIndexWithIntegerMapping(index: String, kClient: KOpenSearchClient): Unit = {
    val _ = kClient.createIndex(index)
    // Force the mapping via the OpenSearch indices API through our raw client
    val rawClient = {
      import org.opensearch.client.opensearch.OpenSearchClient
      import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder
      import org.apache.hc.core5.http.HttpHost
      val httpHost = new HttpHost("http", host, port)
      val transport = ApacheHttpClient5TransportBuilder.builder(httpHost).build()
      new OpenSearchClient(transport)
    }
    val mappingBody =
      """{"properties":{"field":{"type":"integer"}}}"""
    val jsonParser = rawClient._transport().jsonpMapper().jsonProvider().createParser(
      new java.io.StringReader(mappingBody),
    )
    val putReq = org.opensearch.client.opensearch.indices.PutMappingRequest.of { b =>
      b.index(index)
        .withJson(jsonParser)
    }
    rawClient.indices().putMapping(putReq)
    rawClient._transport().close()
  }

  test("strict mode: item-level mapper_parsing_exception returns BulkResult with errors=true") {
    val index   = "strict-test-mapping"
    val kClient = makeKClient(baseProps(host, port, s"INSERT INTO $index SELECT * FROM topic"))
    createIndexWithIntegerMapping(index, kClient)

    // Good numeric document — establishes the mapping and should succeed
    val good = kClient.bulk(Seq(InsertOp(index, "1", numericDoc(42), None, None)))
    good.isSuccess shouldBe true
    good.get.errors shouldBe false

    // String document — conflicts with the integer mapping → mapper_parsing_exception per item
    val bad = kClient.bulk(Seq(InsertOp(index, "2", stringDoc("not-a-number"), None, None)))
    bad.isSuccess shouldBe true
    bad.get.errors shouldBe true
    bad.get.itemErrors should not be empty
    bad.get.itemErrors.head.reason.toLowerCase should (
      include("mapper_parsing_exception") or include("mapping") or include("parse")
    )
  }

  test("tolerant mode: item-level mapper_parsing_exception is swallowed, BulkResult.errors=false") {
    val index = "tolerant-test-mapping"
    val kClient = makeKClient(baseProps(
      host,
      port,
      s"INSERT INTO $index SELECT * FROM topic",
      Map(BULK_STRICT_ITEM_ERRORS_KEY -> "false"),
    ))
    createIndexWithIntegerMapping(index, kClient)

    val _ = kClient.bulk(Seq(InsertOp(index, "1", numericDoc(99), None, None)))

    // Same conflicting doc — tolerant mode must swallow the item error
    val result = kClient.bulk(Seq(InsertOp(index, "2", stringDoc("still-wrong"), None, None)))
    result.isSuccess shouldBe true
    result.get.errors shouldBe false
    result.get.itemErrors shouldBe empty
  }

  test("write to a non-existent index succeeds when AUTOCREATE is used in KCQL") {
    val index   = "autocreate-test"
    val kClient = makeKClient(baseProps(
      host,
      port,
      s"INSERT INTO $index SELECT * FROM topic AUTOCREATE",
    ))
    val result = bulkWrite(kClient, Seq(InsertOp(index, "1", numericDoc(1), None, None)))
    result.errors shouldBe false
  }

  test("C9: strict-mode + error.policy=THROW throws ConnectException on item-level bulk failure via JsonBulkWriter") {
    val index = "c9-throw-test"

    // Step 1: create the index and fix the mapping via a NOOP client first
    val setupClient = makeKClient(baseProps(host, port, s"INSERT INTO $index SELECT * FROM topic"))
    createIndexWithIntegerMapping(index, setupClient)
    // Seed one good record so the mapping is established
    val _ = setupClient.bulk(Seq(InsertOp(index, "seed", numericDoc(1), None, None)))

    // Step 2: Create a JsonBulkWriter backed by a real KOpenSearchClient with THROW + strict mode
    import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
    import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
    import org.opensearch.client.opensearch.OpenSearchClient

    val props = baseProps(
      host, port,
      s"INSERT INTO $index SELECT * FROM topic PK id",
      Map(
        BULK_STRICT_ITEM_ERRORS_KEY -> "true",
        "connect.opensearch.error.policy" -> "THROW",
      ),
    )
    val config    = OpenSearchConfig(props)
    val settings  = OpenSearchSettings(config)
    val transport = OpenSearchTransportFactory.create(settings)
    val osClient  = new OpenSearchClient(transport)
    val kClient   = new KOpenSearchClient(osClient, settings)
    val writer    = new JsonBulkWriter(kClient, settings.common)

    // Step 3: Send a string value — mapping conflict with integer mapping
    val schema: Schema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build()
    val struct:  Struct = new Struct(schema).put("id", "not-a-number")
    val record: SinkRecord = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", schema, struct, 1L)

    // C9 assertion: the writer must throw ConnectException (THROW policy) on item-level errors
    val ex = intercept[ConnectException](writer.write(Vector(record)))
    ex.getMessage should include("item-level error")
  }
}
