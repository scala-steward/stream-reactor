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
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Integration tests for AWS SigV4 signing using WireMock as the target.
 *
 * WireMock stubs verify that:
 * (a) The signed POST /_bulk request reaches WireMock with an Authorization header (B2).
 * (b) The signed GET / info probe also carries Authorization (B2).
 * (c) 302 redirect from WireMock is NOT followed (credential replay prevention) (C2).
 *
 * C5 - LocalStack compromise note:
 * A full LocalStack-backed `OpenSearchSigV4LocalStackIT` would provide stronger guarantees
 * (real SigV4 endpoint verification, actual credential chain resolution), but it requires:
 *  - A LocalStack Docker image in testcontainers (>2 GB)
 *  - OpenSearch Serverless endpoint configuration on LocalStack
 *  - Additional `software.amazon.awssdk:opensearchserverless` dependencies
 *
 * The WireMock-only IT is the accepted compromise for this project: it verifies that the
 * `Authorization: AWS4-HMAC-SHA256 ...` header is present and well-formed on BOTH the info probe
 * and the bulk request, which covers the connector's signing behaviour without requiring a real
 * AWS endpoint. The risk accepted by this compromise is that the SigV4 signature ALGORITHM is
 * not verified end-to-end against a real AWS endpoint; the AWS SDK's own unit tests cover that.
 */
class OpenSearchSigV4WireMockIT extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var wireMock: WireMockServer = _

  override def beforeAll(): Unit = {
    wireMock = new WireMockServer(wireMockConfig().dynamicPort())
    wireMock.start()
  }

  override def afterAll(): Unit = {
    wireMock.stop()
  }

  private def doc = JsonNodeFactory.instance.objectNode().put("field", "value")

  private def makeSigV4Client(service: String): KOpenSearchClient = {
    val host = "localhost"
    val port = wireMock.port()
    val props = Map(
      HOSTS                         -> host,
      ES_PORT                       -> port.toString,
      KCQL                          -> "INSERT INTO idx SELECT * FROM topic",
      AWS_SIGNING_ENABLED_KEY       -> "true",
      AWS_REGION_KEY                 -> "us-east-1",
      AWS_SIGNING_SERVICE_KEY        -> service,
      AWS_CREDENTIALS_PROVIDER_KEY   -> "STATIC",
      AWS_ACCESS_KEY_ID_KEY          -> "AKIAIOSFODNN7EXAMPLE",
      AWS_SECRET_ACCESS_KEY_KEY      -> "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )
    import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
    import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
    import org.opensearch.client.opensearch.OpenSearchClient
    val config   = OpenSearchConfig(props)
    val settings = OpenSearchSettings(config)
    val transport = OpenSearchTransportFactory.create(settings)
    val osClient  = new OpenSearchClient(transport)
    new KOpenSearchClient(osClient, settings)
  }

  private val infoBody =
    """{"name":"test","cluster_name":"test","version":{"number":"2.13.0","distribution":"opensearch"},"tagline":"The OpenSearch Project"}"""

  test("B2: signed bulk POST reaches WireMock with Authorization header on both info probe and bulk") {
    wireMock.resetAll()
    wireMock.stubFor(
      get(urlEqualTo("/")).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(infoBody),
      ),
    )
    wireMock.stubFor(
      post(urlEqualTo("/_bulk")).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json")
          .withBody("""{"took":1,"errors":false,"items":[]}"""),
      ),
    )

    val kClient = makeSigV4Client("es")
    val result  = kClient.bulk(Seq(InsertOp("idx", "1", doc, None, None)))

    result.isSuccess shouldBe true
    result.get.errors shouldBe false

    // B2: The info() probe (GET /) MUST carry a SigV4 Authorization header.
    wireMock.verify(
      getRequestedFor(urlEqualTo("/"))
        .withHeader("Authorization", matching("AWS4-HMAC-SHA256.*")),
    )
    // Bulk POST must also be signed.
    wireMock.verify(
      postRequestedFor(urlEqualTo("/_bulk"))
        .withHeader("Authorization", matching("AWS4-HMAC-SHA256.*")),
    )
  }

  test("B3: aoss signing service — Authorization header contains service=aoss in credential scope") {
    wireMock.resetAll()
    wireMock.stubFor(
      get(urlEqualTo("/")).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(infoBody),
      ),
    )
    wireMock.stubFor(
      post(urlEqualTo("/_bulk")).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json")
          .withBody("""{"took":1,"errors":false,"items":[]}"""),
      ),
    )

    val kClient = makeSigV4Client("aoss")
    val result  = kClient.bulk(Seq(InsertOp("idx", "3", doc, None, None)))

    result.isSuccess shouldBe true

    // B3: Authorization header credential scope must name "aoss" as the signing service.
    // Format: AWS4-HMAC-SHA256 Credential=<key>/<date>/us-east-1/aoss/aws4_request, ...
    wireMock.verify(
      postRequestedFor(urlEqualTo("/_bulk"))
        .withHeader("Authorization", matching(".*us-east-1/aoss/aws4_request.*")),
    )
    wireMock.verify(
      getRequestedFor(urlEqualTo("/"))
        .withHeader("Authorization", matching(".*us-east-1/aoss/aws4_request.*")),
    )
  }

  test("C2: 302 to same-host redirect path is not followed — zero requests land on redirect target") {
    wireMock.resetAll()
    // The redirect target is a different path on the SAME WireMock host.
    // We stub it to return a valid response — if redirect-following were enabled,
    // WireMock would record a request there. We assert zero such requests.
    val redirectTarget = "/_bulk_redirected"
    wireMock.stubFor(
      get(urlEqualTo("/")).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(infoBody),
      ),
    )
    wireMock.stubFor(
      post(urlEqualTo("/_bulk")).willReturn(
        aResponse().withStatus(302)
          .withHeader("Location", s"http://localhost:${wireMock.port()}$redirectTarget"),
      ),
    )
    wireMock.stubFor(
      post(urlEqualTo(redirectTarget)).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json")
          .withBody("""{"took":1,"errors":false,"items":[]}"""),
      ),
    )

    val kClient = makeSigV4Client("es")
    val result  = kClient.bulk(Seq(InsertOp("idx", "2", doc, None, None)))

    // The no-redirect behaviour means the bulk must fail (302 is not a valid bulk response).
    result.isFailure shouldBe true

    // Crucially: the redirect target must have received ZERO requests.
    wireMock.verify(0, postRequestedFor(urlEqualTo(redirectTarget)))
  }
}
