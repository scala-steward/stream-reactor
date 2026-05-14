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

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.ConnectException

import java.nio.file.Files
import java.nio.file.Paths
import java.time.Clock
import java.time.Instant
import scala.jdk.CollectionConverters._

/**
 * Source of a JWT bearer token.
 *
 * Two concrete implementations:
 *  - Static token: the token is fixed at construction time.
 *  - File-backed token: the token is read from a file and refreshed at a configurable interval.
 */
sealed trait JwtTokenSource {
  def getToken: String
}

/**
 * A static, non-rotating JWT bearer token.
 *
 * @param token The bearer token.
 */
class StaticJwtTokenSource(token: String) extends JwtTokenSource {
  override def getToken: String = token
}

/**
 * A file-backed JWT bearer token that is re-read at a configurable interval.
 *
 * @param path           Path to the file containing the bearer token.
 * @param refreshInterval How often (in ms) to re-read the file.
 * @param clock          Clock for time-based refresh decisions.
 */
class FileJwtTokenSource(path: String, refreshInterval: Long, clock: Clock = Clock.systemUTC())
    extends JwtTokenSource
    with StrictLogging {

  // None means "failed / not yet loaded"; Some(token) means a good cached value.
  @volatile private var current:    Option[String] = Some(readFile())
  @volatile private var lastReadAt: Instant        = clock.instant()

  override def getToken: String = {
    val now = clock.instant()
    if (now.toEpochMilli - lastReadAt.toEpochMilli >= refreshInterval) {
      // Invalidate BEFORE the read attempt so that any failure leaves the cache
      // empty. Subsequent calls within the same interval slot therefore also fail
      // rather than silently returning the stale (possibly revoked) token.
      current    = None
      lastReadAt = now
      current    = Some(readFile())
    }
    current.getOrElse(
      throw new ConnectException(s"JWT token cache is empty — last read from $path failed"),
    )
  }

  private def readFile(): String = {
    // Normalize and make absolute so that relative segments are collapsed before use.
    // Connector configuration is an admin-only trust boundary (same as keystore/truststore
    // paths elsewhere in this codebase), so a full allowed-directory whitelist is not
    // enforced, but we reject any path that still contains ".." after normalization as a
    // basic defence against accidental or malicious path traversal.
    val p = Paths.get(path).normalize().toAbsolutePath
    if (p.iterator().asScala.exists(_.toString == "..")) {
      throw new ConnectException(s"JWT token file path rejected (path traversal detected): $path")
    }
    val bytes: Array[Byte] =
      try Files.readAllBytes(p)
      catch {
        case e: Exception =>
          throw new ConnectException(s"Failed to read JWT token from $path", e)
      }
    val value = new String(bytes).trim
    if (value.isEmpty) {
      throw new ConnectException(s"JWT token file $path is empty")
    }
    value
  }
}

object JwtTokenSource extends StrictLogging {

  def fromStaticToken(token: String): JwtTokenSource = {
    logger.warn(
      "connect.opensearch.security.jwt.token configured (static); " +
        "this token does NOT auto-refresh and the connector will fail when it expires. " +
        "Use connect.opensearch.security.jwt.token.file with your IdP rotation tooling for production deployments.",
    )
    new StaticJwtTokenSource(token)
  }

  def fromFile(path: String, refreshIntervalMs: Long): JwtTokenSource =
    new FileJwtTokenSource(path, refreshIntervalMs)
}
