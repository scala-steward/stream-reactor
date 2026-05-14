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
package io.lenses.streamreactor.connect.opensearch.auth

import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider

class AwsCredentialsProviderFactoryTest extends AnyFunSuite with Matchers {

  private val baseProps = Map(
    HOSTS                          -> "my-cluster.us-east-1.es.amazonaws.com",
    ES_PORT                        -> "9200",
    KCQL                           -> "INSERT INTO idx SELECT * FROM topic",
    AWS_SIGNING_ENABLED_KEY        -> "true",
    AWS_REGION_KEY                  -> "us-east-1",
    "connect.opensearch.protocol"  -> "https",
  )

  private def settings(extra: (String, String)*): OpenSearchSettings =
    OpenSearchSettings(OpenSearchConfig(baseProps ++ extra.toMap))

  test("DEFAULT provider returns a DefaultCredentialsProvider") {
    val s        = settings(AWS_CREDENTIALS_PROVIDER_KEY -> "DEFAULT")
    val provider = AwsCredentialsProviderFactory.create(s)
    provider shouldBe a[DefaultCredentialsProvider]
  }

  test("STATIC provider with access key + secret returns StaticCredentialsProvider") {
    val s = settings(
      AWS_CREDENTIALS_PROVIDER_KEY -> "STATIC",
      AWS_ACCESS_KEY_ID_KEY         -> "AKIAIOSFODNN7EXAMPLE",
      AWS_SECRET_ACCESS_KEY_KEY     -> "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )
    val provider = AwsCredentialsProviderFactory.create(s)
    provider shouldBe a[StaticCredentialsProvider]
    val creds = provider.resolveCredentials()
    creds.accessKeyId() shouldBe "AKIAIOSFODNN7EXAMPLE"
    creds.secretAccessKey() shouldBe "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  }

  test("STATIC provider with session token returns AwsSessionCredentials") {
    val s = settings(
      AWS_CREDENTIALS_PROVIDER_KEY -> "STATIC",
      AWS_ACCESS_KEY_ID_KEY         -> "AKIAIOSFODNN7EXAMPLE",
      AWS_SECRET_ACCESS_KEY_KEY     -> "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      AWS_SESSION_TOKEN_KEY         -> "SESSION-TOKEN-VALUE",
    )
    val provider = AwsCredentialsProviderFactory.create(s)
    val creds    = provider.resolveCredentials()
    creds shouldBe a[AwsSessionCredentials]
    creds.asInstanceOf[AwsSessionCredentials].sessionToken() shouldBe "SESSION-TOKEN-VALUE"
  }
}
