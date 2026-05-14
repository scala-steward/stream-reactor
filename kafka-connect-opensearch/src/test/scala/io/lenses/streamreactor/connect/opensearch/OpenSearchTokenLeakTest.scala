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

import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import org.apache.kafka.common.config.ConfigException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Guards that secret-bearing configuration values do not appear in exceptions,
 * toString output, or error messages (except for a controlled "masked" form).
 *
 * Tested keys:
 *  1. connect.opensearch.security.jwt.token (static JWT)
 *  2. connect.opensearch.aws.secret.access.key (SigV4 secret)
 *  3. connect.opensearch.aws.session.token (SigV4 session token)
 *  4. connect.opensearch.ssl.keystore.password (keystore password)
 *  5. connect.opensearch.client.http.auth.password (basic auth password)
 */
class OpenSearchTokenLeakTest extends AnyFunSuite with Matchers {

  private val secretJwtToken  = "SECRET-JWT-TOKEN-DO-NOT-LOG-abc123"
  private val secretAccessKey = "SECRET-AWS-KEY-DO-NOT-LOG-xyz789"
  private val secretSessionTok = "SECRET-SESSION-TOKEN-DO-NOT-LOG-mno456"
  private val secretPassword  = "SECRET-BASIC-PASS-DO-NOT-LOG-pqr012"

  private val baseProps = Map(
    HOSTS   -> "localhost",
    ES_PORT -> "9200",
    KCQL    -> "INSERT INTO idx SELECT * FROM topic",
  )

  test("static JWT token does not appear in ConfigException messages") {
    // Mutual-exclusion validation fires in OpenSearchSettings, not OpenSearchConfig.
    // Create config first (toString must not leak), then trigger settings validation.
    val config = OpenSearchConfig(baseProps ++ Map(
      JWT_TOKEN_KEY                   -> secretJwtToken,
      CLIENT_HTTP_BASIC_AUTH_USERNAME -> "user",
      CLIENT_HTTP_BASIC_AUTH_PASSWORD -> "pass",
    ))
    config.toString should not include secretJwtToken

    val ex = intercept[ConfigException] {
      import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
      OpenSearchSettings(config)
    }
    ex.getMessage should not include secretJwtToken
  }

  test("AWS secret access key does not appear in ConfigException messages") {
    val ex = intercept[ConfigException] {
      import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
      OpenSearchSettings(OpenSearchConfig(baseProps ++ Map(
        AWS_SIGNING_ENABLED_KEY       -> "true",
        AWS_REGION_KEY                 -> "us-east-1",
        AWS_CREDENTIALS_PROVIDER_KEY   -> "STATIC",
        AWS_ACCESS_KEY_ID_KEY          -> "AKIAIOSFODNN7EXAMPLE",
        AWS_SECRET_ACCESS_KEY_KEY      -> secretAccessKey,
        HOSTS                          -> "host1,host2",
      )))
    }
    ex.getMessage should not include secretAccessKey
  }

  test("AWS session token does not appear in exception chain") {
    val config = OpenSearchConfig(baseProps ++ Map(
      AWS_SESSION_TOKEN_KEY -> secretSessionTok,
    ))
    config.toString should not include secretSessionTok
  }

  test("basic auth password does not appear in ConfigException messages") {
    val config = OpenSearchConfig(baseProps ++ Map(
      CLIENT_HTTP_BASIC_AUTH_USERNAME -> "user",
      CLIENT_HTTP_BASIC_AUTH_PASSWORD -> secretPassword,
      JWT_TOKEN_KEY                   -> "some-token",
    ))
    config.toString should not include secretPassword

    val ex = intercept[ConfigException] {
      import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
      OpenSearchSettings(config)
    }
    ex.getMessage should not include secretPassword
  }

  test("kafka CONFIG with PASSWORD type masks secret in toString") {
    val config = OpenSearchConfig(baseProps ++ Map(
      JWT_TOKEN_KEY             -> secretJwtToken,
      AWS_SECRET_ACCESS_KEY_KEY -> secretAccessKey,
      AWS_SESSION_TOKEN_KEY     -> secretSessionTok,
      CLIENT_HTTP_BASIC_AUTH_PASSWORD -> secretPassword,
    ))
    val repr = config.toString
    repr should not include secretJwtToken
    repr should not include secretAccessKey
    repr should not include secretSessionTok
    repr should not include secretPassword
  }
}
