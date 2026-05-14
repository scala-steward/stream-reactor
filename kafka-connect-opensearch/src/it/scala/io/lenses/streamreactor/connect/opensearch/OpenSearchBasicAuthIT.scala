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
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import io.lenses.streamreactor.connect.testcontainers.OpensearchContainer
import org.apache.hc.client5.http.auth.AuthScope
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy
import org.apache.hc.core5.http.HttpHost
import org.opensearch.client.json.jackson.JacksonJsonpMapper
import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.opensearch.core.SearchRequest
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder

import java.io.FileInputStream
import java.security.KeyStore
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory

import scala.util.Try

/**
 * Integration tests for HTTP basic auth against OpenSearch with the Security plugin enabled.
 *
 * - Happy path: correct credentials authenticate and documents land.
 * - Negative path: wrong password fails with an exception.
 */
class OpenSearchBasicAuthIT extends ITBase {

  override val container: OpensearchContainer = OpensearchContainer()
    .withSecurityEnabled(true)

  private def host = container.hostNetwork.httpHostAddress.split(":").head
  private def port = container.hostNetwork.httpHostAddress.split(":").last.toInt

  /** Build an authenticated [[OpenSearchClient]] for read-back search under basic auth. */
  private def authedSearchClient(username: String, password: String): OpenSearchClient = {
    val httpHost = new HttpHost("https", host, port)

    val credProvider = new BasicCredentialsProvider()
    credProvider.setCredentials(
      new AuthScope(null, null, -1, null, null),
      new UsernamePasswordCredentials(username, password.toCharArray),
    )

    val sslCtx = buildTrustStoreSslContext(container.clientTruststorePath, "changeit")
    val tlsStrategy = new DefaultClientTlsStrategy(sslCtx)
    val cm = PoolingAsyncClientConnectionManagerBuilder.create()
      .setTlsStrategy(tlsStrategy)
      .build()

    val transport = ApacheHttpClient5TransportBuilder.builder(httpHost)
      .setMapper(new JacksonJsonpMapper())
      .setHttpClientConfigCallback { b =>
        b.setDefaultCredentialsProvider(credProvider)
        b.setConnectionManager(cm)
      }
      .build()
    new OpenSearchClient(transport)
  }

  private def buildTrustStoreSslContext(truststorePath: String, password: String): SSLContext = {
    val ts  = KeyStore.getInstance("JKS")
    val fis = new FileInputStream(truststorePath)
    try ts.load(fis, password.toCharArray)
    finally fis.close()
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(ts)
    val ctx = SSLContext.getInstance("TLS")
    ctx.init(null, tmf.getTrustManagers, null)
    ctx
  }

  test("happy path: valid basic credentials write documents successfully and read-back confirms landing") {
    val index   = "test-basic-happy"
    val kClient = makeKClient(baseProps(
      host, port,
      s"INSERT INTO $index SELECT * FROM topic",
      Map(
        "connect.opensearch.protocol"   -> "https",
        CLIENT_HTTP_BASIC_AUTH_USERNAME -> "kafka-connect",
        CLIENT_HTTP_BASIC_AUTH_PASSWORD -> "connect-password",
        "ssl.truststore.location"       -> container.clientTruststorePath,
        "ssl.truststore.password"       -> "changeit",
      ),
    ))
    val doc    = JsonNodeFactory.instance.objectNode().put("msg", "hello")
    val result = kClient.bulk(Seq(InsertOp(index, "1", doc, None, None)))
    result.isSuccess shouldBe true
    result.get.errors shouldBe false

    // Read-back: the document must be searchable under the same credentials
    val searchClient = authedSearchClient("kafka-connect", "connect-password")
    try {
      eventually {
        val resp = searchClient.search(
          (s: SearchRequest.Builder) => s.index(index),
          classOf[JsonNode],
        )
        resp.hits().total().value() shouldBe 1L
      }
    } finally Try(searchClient._transport().close())
  }

  test("negative path: wrong password fails with an exception") {
    val kClient = makeKClient(baseProps(
      host, port,
      "INSERT INTO test-basic-bad SELECT * FROM topic",
      Map(
        "connect.opensearch.protocol"   -> "https",
        CLIENT_HTTP_BASIC_AUTH_USERNAME -> "kafka-connect",
        CLIENT_HTTP_BASIC_AUTH_PASSWORD -> "wrong-password",
        "ssl.truststore.location"       -> container.clientTruststorePath,
        "ssl.truststore.password"       -> "changeit",
      ),
    ))
    val doc    = JsonNodeFactory.instance.objectNode().put("msg", "hello")
    val result = kClient.bulk(Seq(InsertOp("test-basic-bad", "1", doc, None, None)))
    result.isFailure shouldBe true
  }
}
