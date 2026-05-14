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
import io.lenses.streamreactor.connect.testcontainers.OpensearchContainer
import io.lenses.streamreactor.connect.testcontainers.SecurityPkiFixture

import java.nio.file.Files

/**
 * Integration tests for mTLS / PKI client certificate authentication.
 *
 * - Happy path: connect-client cert authenticates and documents land.
 * - Negative path: wrong truststore causes SSLHandshakeException.
 */
class OpenSearchPkiAuthIT extends ITBase {

  override val container: OpensearchContainer = OpensearchContainer()
    .withSecurityEnabled(true)

  private def host = "localhost"
  private def port = container.hostNetwork.httpHostAddress.split(":").last.toInt

  test("C4: happy path — mTLS-alone client cert authenticates; whoami returns client CN as principal") {
    val pki = SecurityPkiFixture.shared
    val kClient = makeKClient(
      Map(
        "connect.opensearch.hosts"    -> host,
        "connect.opensearch.port"     -> port.toString,
        "connect.opensearch.kcql"     -> "INSERT INTO test-pki-happy SELECT * FROM topic",
        "connect.opensearch.protocol" -> "https",
        "ssl.keystore.location"       -> pki.keystorePath.toString,
        "ssl.keystore.password"       -> "changeit",
        "ssl.truststore.location"     -> pki.truststorePath.toString,
        "ssl.truststore.password"     -> "changeit",
      ),
    )

    val doc    = JsonNodeFactory.instance.objectNode().put("msg", "pki-happy")
    val result = kClient.bulk(Seq(InsertOp("test-pki-happy", "1", doc, None, None)))
    result.isSuccess shouldBe true
    result.get.errors shouldBe false

    // C4: Assert that the client was recognised by the server as the mTLS principal.
    // Call /_plugins/_security/whoami to verify the certificate CN is the authenticated DN.
    val rawClient = openSearchClient(host, port, Some(pki))
    try {
      import org.opensearch.client.opensearch.generic.Requests
      val resp = rawClient.generic().execute(
        Requests.builder().endpoint("/_plugins/_security/whoami").method("GET").build(),
      )
      val bodyStream = resp.getBody.get().body()
      val json       = new com.fasterxml.jackson.databind.ObjectMapper().readTree(bodyStream)
      val dn         = Option(json.get("dn")).map(_.asText()).getOrElse("")
      // The client cert subject is "CN=connect-client,O=lenses,L=test,C=GB".
      // Assert the expected CN is present in the DN returned by the server.
      dn should include("CN=connect-client")
    } finally {
      rawClient._transport().close()
    }
  }

  test("C7: negative path — wrong truststore causes SSLHandshakeException in exception cause chain") {
    val wrongTruststoreFile = Files.createTempFile("wrong-truststore", ".jks")

    val kClient = makeKClient(
      Map(
        "connect.opensearch.hosts"    -> host,
        "connect.opensearch.port"     -> port.toString,
        "connect.opensearch.kcql"     -> "INSERT INTO test-pki-bad SELECT * FROM topic",
        "connect.opensearch.protocol" -> "https",
        "ssl.truststore.location"     -> wrongTruststoreFile.toString,
        "ssl.truststore.password"     -> "changeit",
      ),
    )

    val doc    = JsonNodeFactory.instance.objectNode().put("msg", "pki-bad")
    val result = kClient.bulk(Seq(InsertOp("test-pki-bad", "1", doc, None, None)))
    result.isFailure shouldBe true

    // C7: Tightened assertion — SSLHandshakeException MUST be in the cause chain,
    // not just any message containing "ssl" (which could be an unrelated log string).
    def findInCauseChain(t: Throwable)(p: Throwable => Boolean): Boolean =
      p(t) || Option(t.getCause).exists(findInCauseChain(_)(p))

    val thrown = result.failed.get
    findInCauseChain(thrown)(_.isInstanceOf[javax.net.ssl.SSLHandshakeException]) shouldBe true
  }
}
