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
package io.lenses.streamreactor.connect

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.fasterxml.jackson.databind.JsonNode
import _root_.io.confluent.kafka.serializers.KafkaJsonSerializer
import _root_.io.lenses.streamreactor.connect.model.Order
import _root_.io.lenses.streamreactor.connect.testcontainers.OpensearchContainer
import _root_.io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.ConfigValue
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.ConnectorConfiguration
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient.createConnector
import _root_.io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import org.apache.hc.core5.http.HttpHost
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.opensearch.core.SearchRequest
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder
import org.scalatest.concurrent.Eventually
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Seconds
import org.scalatest.time.Span

class OpenSearchTest
    extends AsyncFlatSpec
    with AsyncIOSpec
    with StreamReactorContainerPerSuite
    with Matchers
    with Eventually {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(60, Seconds), interval = Span(2, Seconds))

  lazy val container: OpensearchContainer =
    OpensearchContainer(dockerTag = OpensearchContainer.defaultTag).withNetwork(network)

  override val schemaRegistryContainer: Option[SchemaRegistryContainer] = None

  override val connectorModule: String = "opensearch"

  private lazy val opensearchClient: OpenSearchClient = {
    val parts    = container.hostNetwork.httpHostAddress.split(":")
    val httpHost = new HttpHost("http", parts(0), parts(1).toInt)
    val transport = ApacheHttpClient5TransportBuilder.builder(httpHost).build()
    new OpenSearchClient(transport)
  }

  override def beforeAll(): Unit = {
    container.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    scala.util.Try(opensearchClient._transport().close())
    container.stop()
  }

  behavior of "OpenSearch connector"

  it should "sink records" in {
    val resources = for {
      producer  <- createProducer[String, Order](classOf[StringSerializer], classOf[KafkaJsonSerializer[Order]])
      connector <- createConnector(sinkConfig(), 30L)
    } yield (producer, connector)

    resources.use {
      case (producer, _) =>
        IO {
          val order = Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100)
          producer.send(new ProducerRecord[String, Order]("orders", order)).get()
          producer.flush()

          val _ = eventually {
            val resp = opensearchClient.search(
              (s: SearchRequest.Builder) => s.index("orders"),
              classOf[JsonNode],
            )
            assert(resp.hits().total().value() == 1L)
          }

          opensearchClient.search(
            (s: SearchRequest.Builder) => s.index("orders"),
            classOf[JsonNode],
          )
        }.asserting { resp =>
          resp.hits().total().value() shouldBe 1L
          val hit = resp.hits().hits().get(0).source()
          hit.get("id").asInt() shouldBe 1
          hit.get("product").asText() shouldBe "OP-DAX-P-20150201-95.7"
          hit.get("price").asDouble() shouldBe 94.2
          hit.get("qty").asInt() shouldBe 100
        }
    }
  }

  private def sinkConfig(): ConnectorConfiguration =
    ConnectorConfiguration(
      "opensearch-sink",
      Map(
        "connector.class"             -> ConfigValue("io.lenses.streamreactor.connect.opensearch.OpenSearchSinkConnector"),
        "tasks.max"                   -> ConfigValue(1),
        "topics"                      -> ConfigValue("orders"),
        "connect.opensearch.protocol" -> ConfigValue("http"),
        "connect.opensearch.hosts"    -> ConfigValue(container.networkAlias),
        "connect.opensearch.port"     -> ConfigValue(Integer.valueOf(container.port)),
        "connect.opensearch.kcql"     -> ConfigValue("INSERT INTO orders SELECT * FROM orders"),
        "connect.progress.enabled"    -> ConfigValue(true),
      ),
    )
}
