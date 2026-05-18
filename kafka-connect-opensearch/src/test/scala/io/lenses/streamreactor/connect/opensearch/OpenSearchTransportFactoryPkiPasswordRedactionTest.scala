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
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

/**
 * Guards that keystore/truststore passwords are not exposed in exception messages
 * when PKI loading fails (e.g., corrupt keystore file).
 *
 * The guard tests that the known password "redact-me-12345" does NOT appear in
 * the exception chain even though it was provided to StoresInfo.
 */
class OpenSearchTransportFactoryPkiPasswordRedactionTest extends AnyFunSuite with Matchers {

  private val knownPassword = "redact-me-12345"

  test("keystore password is not exposed in exception chain when keystore is corrupt") {
    // Write a corrupt keystore file
    val corruptKeystore = Files.createTempFile("corrupt-keystore", ".jks")
    Files.write(corruptKeystore, "this-is-not-a-valid-jks-file".getBytes())

    val props = Map(
      HOSTS                   -> "localhost",
      ES_PORT                 -> "9200",
      KCQL                    -> "INSERT INTO idx SELECT * FROM topic",
      "ssl.keystore.location" -> corruptKeystore.toString,
      "ssl.keystore.password" -> knownPassword,
      "ssl.keystore.type"     -> "JKS",
    )

    // Config toString must not expose the password regardless of whether loading succeeds.
    val config = OpenSearchConfig(props)
    config.toString should not include knownPassword

    // A corrupt keystore must cause an exception when the transport is created.
    // Walk the full exception chain to ensure the password is not leaked in any message.
    def collectMessages(t: Throwable): Seq[String] = {
      val msg = Option(t.getMessage).getOrElse("")
      val sup = Option(t.getSuppressed).toSeq.flatMap(_.flatMap(s => collectMessages(s)))
      val cau = Option(t.getCause).toSeq.flatMap(collectMessages)
      msg +: (cau ++ sup)
    }

    val ex = intercept[Exception] {
      val sSettings = OpenSearchSettings(config)
      val transport = OpenSearchTransportFactory.create(sSettings)
      transport.close()
    }
    collectMessages(ex).mkString("\n") should not include knownPassword
  }
}
