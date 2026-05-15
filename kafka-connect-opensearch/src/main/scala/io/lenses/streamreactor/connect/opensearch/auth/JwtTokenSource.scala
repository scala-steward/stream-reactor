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
 * @param path            Path to the file containing the bearer token.
 * @param refreshInterval How often (in ms) to re-read the file.
 * @param baseDir         When [[Some]], every read validates that the resolved absolute path
 *                        starts with this directory. Prevents a connector configuration from
 *                        being used to read arbitrary worker-local files.
 * @param clock           Clock for time-based refresh decisions.
 */
class FileJwtTokenSource(
  path:            String,
  refreshInterval: Long,
  baseDir:         Option[String] = None,
  clock:           Clock          = Clock.systemUTC(),
) extends JwtTokenSource
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
    // Reject any path whose raw form contains ".." components before normalization.
    // This catches relative traversal attempts (e.g. "../../etc/passwd") early and
    // is independent of the base-directory check below.
    if (path.split("[/\\\\]").contains("..")) {
      throw new ConnectException(s"JWT token file path rejected (path traversal detected): $path")
    }

    val p = Paths.get(path).normalize().toAbsolutePath

    // When a base directory is configured, verify that the resolved path is
    // contained within it. This is the primary guard against absolute paths that
    // point at arbitrary worker-local files (e.g. /etc/passwd,
    // /var/run/secrets/kubernetes.io/serviceaccount/token). Without this check,
    // any absolute path accepted by the OS is reachable.
    baseDir.foreach { bd =>
      val base = Paths.get(bd).normalize().toAbsolutePath
      if (!p.startsWith(base)) {
        throw new ConnectException(
          s"JWT token file path rejected (outside allowed base directory '$bd'): $path",
        )
      }
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

  def fromFile(path: String, refreshIntervalMs: Long, baseDir: Option[String] = None): JwtTokenSource =
    new FileJwtTokenSource(path, refreshIntervalMs, baseDir)
}
